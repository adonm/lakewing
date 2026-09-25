//! PostgreSQL connections: tokio-postgres over rustls, pooled most recently
//! used first.
//!
//! LIFO, because a TCP sender restarts slow start on a connection idle longer
//! than its RTO (Linux: >= 200 ms), so a FIFO pool (sqlx 0.8's) hands every
//! request its coldest connection: on the rig, a 64 KiB fetch from Aurora took
//! 1.18 ms on a hot connection and 2.17 ms after 300 ms idle. Reusing the most
//! recent connection keeps a hot working set the size of the real concurrency.
//! No ping on checkout: a broken connection fails its query and is discarded.

use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Socket, Statement};

/// A checkout waits at most this long for a free connection.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle connections beyond `Options::min` close after this long unused.
const IDLE_MAX: Duration = Duration::from_secs(600);

pub struct Options {
    /// Connections opened at start and kept however idle.
    pub min: usize,
    /// Connections open at once, idle or checked out. This is the client's
    /// share of a shared database, not the server's ceiling.
    pub max: usize,
    /// Run on every new connection (one simple-query batch).
    pub session: String,
    /// The hot statement, prepared once per connection (`Pooled::range`).
    pub range_sql: &'static str,
    pub range_types: &'static [Type],
}

/// Cloneable handle to one pool.
#[derive(Clone)]
pub struct Pool(Arc<Inner>);

struct Inner {
    config: tokio_postgres::Config,
    tls: MakeRustls,
    opts: Options,
    /// Idle connections, most recently used last.
    idle: Mutex<Vec<Conn>>,
    slots: Arc<Semaphore>,
}

struct Conn {
    client: Client,
    /// Prepared on first use, not at connect: the pool's warm-up connections
    /// come up before `db::init` has created the schema the statement names.
    range: OnceCell<Statement>,
    idle_since: Instant,
}

impl Pool {
    pub async fn connect(url: &str, opts: Options) -> Result<Pool> {
        let mut config: tokio_postgres::Config = url.parse()?;
        if config.get_application_name().is_none() {
            config.application_name("pgvs3");
        }
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(Duration::from_secs(10));
        }
        let pool = Pool(Arc::new(Inner {
            config,
            tls: MakeRustls::new()?,
            slots: Arc::new(Semaphore::new(opts.max)),
            opts,
            idle: Mutex::new(Vec::new()),
        }));
        let warm =
            futures::future::try_join_all((0..pool.0.opts.min).map(|_| pool.0.open())).await?;
        pool.0.idle.lock().unwrap().extend(warm);
        Ok(pool)
    }

    /// The most recently used idle connection, else a new one.
    pub async fn get(&self) -> Result<Pooled> {
        let slot = tokio::time::timeout(ACQUIRE_TIMEOUT, self.0.slots.clone().acquire_owned())
            .await
            .map_err(|_| anyhow!("no PostgreSQL connection free within {ACQUIRE_TIMEOUT:?}"))??;
        let conn = loop {
            let top = self.0.idle.lock().unwrap().pop();
            match top {
                Some(c) if c.client.is_closed() => continue,
                Some(c) => break c,
                None => break self.0.open().await?,
            }
        };
        Ok(Pooled {
            conn: Some(conn),
            pool: self.clone(),
            _slot: slot,
        })
    }
}

impl Inner {
    async fn open(&self) -> Result<Conn> {
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pgvs3: postgres connection closed: {e}");
            }
        });
        client.batch_execute(&self.opts.session).await?;
        Ok(Conn {
            client,
            range: OnceCell::new(),
            idle_since: Instant::now(),
        })
    }
}

/// A checked-out connection. Dropping it puts the connection back on top of
/// the idle stack: tokio-postgres keeps a client usable after a cancelled
/// query, a dropped transaction has already queued its ROLLBACK, and a
/// dropped COPY sink sends CopyFail, all ahead of the next request.
pub struct Pooled {
    conn: Option<Conn>,
    pool: Pool,
    _slot: OwnedSemaphorePermit,
}

impl Pooled {
    /// This connection's prepared `Options::range_sql`, prepared on first use.
    pub async fn range(&mut self) -> Result<&Statement> {
        let Pooled {
            conn,
            pool,
            _slot: _,
        } = self;
        let conn = conn.as_mut().expect("live until drop");
        let opts = &pool.0.opts;
        Ok(conn
            .range
            .get_or_try_init(|| conn.client.prepare_typed(opts.range_sql, opts.range_types))
            .await?)
    }

    fn live(&self) -> &Conn {
        self.conn.as_ref().expect("live until drop")
    }
}

impl Deref for Pooled {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.live().client
    }
}

impl DerefMut for Pooled {
    fn deref_mut(&mut self) -> &mut Client {
        &mut self.conn.as_mut().expect("live until drop").client
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        if conn.client.is_closed() {
            return;
        }
        conn.idle_since = Instant::now();
        let stale = {
            let mut idle = self.pool.0.idle.lock().unwrap();
            idle.push(conn);
            // The bottom of the stack is the least recently used.
            (idle.len() > self.pool.0.opts.min && idle[0].idle_since.elapsed() > IDLE_MAX)
                .then(|| idle.remove(0))
        };
        drop(stale); // closes it outside the lock
    }
}

// ---------------------------------------------------------------------------
// TLS: rustls on ring. As with libpq's sslmode=require without a root
// certificate (and sqlx before), the channel is encrypted but the server
// certificate is not verified; handshake signatures still are.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MakeRustls(Arc<rustls::ClientConfig>);

impl MakeRustls {
    fn new() -> Result<Self> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnly(provider)))
            .with_no_client_auth();
        Ok(Self(Arc::new(config)))
    }
}

impl MakeTlsConnect<Socket> for MakeRustls {
    type Stream = RustlsStream;
    type TlsConnect = RustlsConnect;
    type Error = rustls::pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<RustlsConnect, Self::Error> {
        Ok(RustlsConnect {
            name: ServerName::try_from(domain)?.to_owned(),
            config: self.0.clone(),
        })
    }
}

struct RustlsConnect {
    name: ServerName<'static>,
    config: Arc<rustls::ClientConfig>,
}

impl TlsConnect<Socket> for RustlsConnect {
    type Stream = RustlsStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<RustlsStream>> + Send>>;

    fn connect(self, stream: Socket) -> Self::Future {
        let connector = tokio_rustls::TlsConnector::from(self.config);
        Box::pin(async move { connector.connect(self.name, stream).await.map(RustlsStream) })
    }
}

struct RustlsStream(tokio_rustls::client::TlsStream<Socket>);

impl TlsStream for RustlsStream {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

impl AsyncRead for RustlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for RustlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// Accepts any server certificate (sslmode=require semantics) but still
/// checks the handshake signatures against it.
#[derive(Debug)]
struct EncryptOnly(Arc<CryptoProvider>);

impl ServerCertVerifier for EncryptOnly {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
