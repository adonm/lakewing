//! Deterministic incompressible filler (splitmix64) + the `seed` command.

use anyhow::Result;
use sha2::{Digest, Sha256};

use crate::db;

/// splitmix64 stream: cheap, deterministic, incompressible by pglz and BtrBlocks.
pub struct Filler {
    state: u64,
}

impl Filler {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    #[allow(clippy::chunks_exact_to_as_chunks)] // the tail is filled, not iterated
    pub fn fill(&mut self, out: &mut [u8]) {
        let mut chunks = out.chunks_exact_mut(8);
        for c in &mut chunks {
            c.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let tail = self.next_u64();
        for (i, b) in chunks.into_remainder().iter_mut().enumerate() {
            *b = (tail >> (8 * i)) as u8;
        }
    }
}

pub struct SeedConfig {
    pub bucket: String,
    pub gigabytes: f64,
    pub object_mib: usize,
    pub tasks: usize,
}

pub async fn run(pool: &db::Pool, cfg: SeedConfig) -> Result<()> {
    db::init(pool).await?;
    let object_bytes = cfg.object_mib * 1024 * 1024;
    let n_objects = ((cfg.gigabytes * 1024.0 * 1024.0 * 1024.0) as usize).div_ceil(object_bytes);
    println!(
        "seeding {} objects x {} MiB ({} MiB total, {}B rows)",
        n_objects,
        cfg.object_mib,
        n_objects * cfg.object_mib,
        db::ROW_BYTES
    );

    let mut handles = Vec::new();
    let per_task = n_objects.div_ceil(cfg.tasks.max(1));
    for task in 0..cfg.tasks.max(1) {
        let pool = pool.clone();
        let bucket = cfg.bucket.clone();
        let (lo, hi) = (task * per_task, ((task + 1) * per_task).min(n_objects));
        handles.push(tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            for i in lo..hi {
                let key = format!("obj-{i:05}.bin");
                let mut filler = Filler::new(0x5EED_0000 + i as u64);
                let mut data = vec![0u8; object_bytes];
                filler.fill(&mut data);
                let etag = Sha256::digest(&data).to_vec();
                let t_put = std::time::Instant::now();
                db::put(&pool, &bucket, &key, &data, &etag).await?;
                let mb = object_bytes as f64 / 1024.0 / 1024.0;
                println!(
                    "  {key}: {} MiB in {:.2}s ({:.0} MiB/s)",
                    mb,
                    t_put.elapsed().as_secs_f64(),
                    mb / t_put.elapsed().as_secs_f64()
                );
            }
            anyhow::Ok(t0.elapsed())
        }));
    }
    let mut total = std::time::Duration::ZERO;
    for h in handles {
        total = total.max(h.await??);
    }
    let total_bytes = n_objects as f64 * object_bytes as f64;
    println!(
        "seeded {:.1} GiB in {:.1}s ({:.0} MiB/s wall)",
        total_bytes / 1024.0 / 1024.0 / 1024.0,
        total.as_secs_f64(),
        total_bytes / 1024.0 / 1024.0 / total.as_secs_f64()
    );
    db::sizes(pool).await?;
    Ok(())
}
