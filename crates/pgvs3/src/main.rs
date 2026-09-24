use anyhow::Result;
use clap::{Parser, Subcommand};
use pgvs3::{bench, db, seed, server};

#[derive(Parser)]
#[command(name = "pgvs3", about = "Lowest-overhead S3-compatible service over PostgreSQL byte rows")]
struct Cli {
    #[arg(long, global = true, default_value = "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench")]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the S3 gateway (SigV4 auth).
    Serve {
        #[arg(long, default_value = "127.0.0.1:8014")]
        addr: String,
        #[arg(long, default_value = "cachebench")]
        access_key: String,
        #[arg(long, default_value = "cachebench-local-only")]
        secret_key: String,
    },
    /// Seed deterministic incompressible objects.
    Seed {
        #[arg(long, default_value = "lake")]
        bucket: String,
        #[arg(long, default_value_t = 8.0)]
        gigabytes: f64,
        #[arg(long, default_value_t = 64)]
        object_mib: usize,
        #[arg(long, default_value_t = 16)]
        tasks: usize,
    },
    /// Latency/throughput bench against a running gateway.
    Bench {
        #[arg(long, default_value = "http://127.0.0.1:8014")]
        endpoint: String,
        #[arg(long, default_value = "lake")]
        bucket: String,
        #[arg(long, default_value = "cachebench")]
        access_key: String,
        #[arg(long, default_value = "cachebench-local-only")]
        secret_key: String,
        #[arg(long, value_delimiter = ',', default_value = "4096,65536,262144,1048576,8388608")]
        sizes: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "1,16")]
        concurrency: Vec<usize>,
        #[arg(long, default_value_t = 2000)]
        requests: usize,
        #[arg(long, default_value_t = 64)]
        sample: usize,
    },
    /// Storage overhead summary (logical vs physical).
    Stat,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve { addr, access_key, secret_key } => {
            let pool = db::connect(&cli.url).await?;
            db::init(&pool).await?;
            server::serve(pool, server::ServeConfig { addr, access_key, secret_key }).await
        }
        Cmd::Seed { bucket, gigabytes, object_mib, tasks } => {
            let pool = db::connect(&cli.url).await?;
            seed::run(&pool, seed::SeedConfig { bucket, gigabytes, object_mib, tasks }).await
        }
        Cmd::Bench { endpoint, bucket, access_key, secret_key, sizes, concurrency, requests, sample } => {
            bench::run(bench::BenchConfig {
                endpoint,
                bucket,
                access_key,
                secret_key,
                sizes,
                concurrency,
                requests,
                pg_url: cli.url,
                sample,
            })
            .await
        }
        Cmd::Stat => {
            let pool = db::connect(&cli.url).await?;
            db::sizes(&pool).await?;
            Ok(())
        }
    }
}
