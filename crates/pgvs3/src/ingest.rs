//! Binary COPY framing for the ingest path: fixed-size rows, one header per
//! stream, then row tuples. Pure byte transform (easy to get subtly wrong and
//! worth isolating) — the transaction and multipart bookkeeping stay in `db`.

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};

use crate::db::ROW_BYTES;

pub const COPY_SQL: &str = "COPY s3p.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)";
pub const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";
pub const COPY_TRAILER: &[u8] = &[0xFF, 0xFF];
/// Flush to the COPY stream at about this granularity.
pub const SEND_BATCH: usize = 4 << 20;

pub enum IngestMsg {
    Data(Bytes),
    Abort,
}

/// `(size, sha256)` of the bytes an ingest streamed.
pub type IngestResult = Result<(i64, Vec<u8>), anyhow::Error>;

/// Accumulates request bytes into COPY rows: hashes them as they pass through
/// and frames each whole row. Bytes not yet framed are always less than one
/// row between pushes.
pub struct RowFramer {
    file_id: i64,
    hasher: Sha256,
    pending: Vec<u8>,
    frame: BytesMut,
    next_no: i32,
    total: i64,
}

impl RowFramer {
    pub fn new(file_id: i64) -> Self {
        Self {
            file_id,
            hasher: Sha256::new(),
            pending: Vec::new(),
            frame: BytesMut::with_capacity(SEND_BATCH),
            next_no: 0,
            total: 0,
        }
    }

    /// Frame one incoming chunk; returns a buffer to flush to the COPY stream
    /// once it is worth a round trip.
    pub fn push(&mut self, chunk: &[u8]) -> Option<Bytes> {
        self.hasher.update(chunk);
        self.total += chunk.len() as i64;
        self.pending.extend_from_slice(chunk);
        let whole = self.pending.len() - self.pending.len() % ROW_BYTES as usize;
        if whole == 0 {
            return None;
        }
        frame_rows(
            self.file_id,
            self.next_no,
            &self.pending[..whole],
            &mut self.frame,
        );
        self.next_no += (whole / ROW_BYTES as usize) as i32;
        self.pending.drain(..whole);
        (self.frame.len() >= SEND_BATCH).then(|| self.frame.split().freeze())
    }

    /// Flush any partial row and the COPY trailer; returns the final send
    /// buffer and `(size, sha256)` of everything streamed.
    pub fn finish(mut self) -> (Bytes, (i64, Vec<u8>)) {
        if !self.pending.is_empty() {
            frame_rows(self.file_id, self.next_no, &self.pending, &mut self.frame);
        }
        self.frame.extend_from_slice(COPY_TRAILER);
        let out = self.frame.split().freeze();
        let sum = self.hasher.finalize().to_vec();
        (out, (self.total, sum))
    }
}

/// Binary COPY framing: field count, then the (file_id, no, data) tuple.
fn frame_rows(file_id: i64, first_no: i32, data: &[u8], out: &mut BytesMut) {
    let nrows = data.len().div_ceil(ROW_BYTES as usize);
    for i in 0..nrows {
        let start = i * ROW_BYTES as usize;
        let end = ((i + 1) * ROW_BYTES as usize).min(data.len());
        out.extend_from_slice(&3i16.to_be_bytes()); // field count
        out.extend_from_slice(&8i32.to_be_bytes());
        out.extend_from_slice(&file_id.to_be_bytes());
        out.extend_from_slice(&4i32.to_be_bytes());
        out.extend_from_slice(&(first_no + i as i32).to_be_bytes());
        out.extend_from_slice(&((end - start) as i32).to_be_bytes());
        out.extend_from_slice(&data[start..end]);
    }
}
