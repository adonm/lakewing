//! Startup resource detection for auto-derived cache budgets. Explicit
//! flags always win; when absent the budgets below are derived from the
//! cgroup (or system) memory limit, the effective CPU quota, and the free
//! space of the cache directory's filesystem — and every derivation is
//! logged so a pod's effective configuration is never a mystery.
//!
//! The formulas are chosen so an 8 GiB / 4-CPU pod reproduces the previous
//! static defaults exactly (64 MiB foyer RAM, 8 MiB HEAD RAM, 256 MiB
//! decoded-index RAM, 64 MiB file-metadata RAM, 16 concurrent fills).
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct Resources {
    pub memory_bytes: u64,
    pub cpus: usize,
    pub disk_free_bytes: Option<u64>,
}

impl Resources {
    /// Detect from this process's cgroup/system, plus the cache directory's
    /// filesystem when one is configured.
    pub fn detect(cache_dir: Option<&str>) -> Self {
        Self {
            memory_bytes: memory_limit_bytes().unwrap_or(2 * 1024 * 1024 * 1024),
            cpus: effective_cpus(),
            disk_free_bytes: cache_dir.and_then(|dir| disk_free_bytes(Path::new(dir)).ok()),
        }
    }
}

/// Effective CPU count: the smallest of schedulable CPUs and any cgroup
/// quota (v2 `cpu.max`, v1 `cpu.cfs_quota_us`/`cpu.cfs_period_us`).
pub fn effective_cpus() -> usize {
    let affinity = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let quotas = [
        read_cpu_max_v2().map(|(quota, period)| quota as f64 / period as f64),
        read_cpu_max_v1().map(|(quota, period)| quota as f64 / period as f64),
    ]
    .into_iter()
    .flatten()
    .filter(|cpus| *cpus >= 1.0);
    quotas.fold(affinity, |acc, cpus| acc.min(cpus.floor() as usize).max(1))
}

fn cgroup_file_v2(name: &str) -> Option<String> {
    // In a container the pod cgroup is usually the root of the mounted v2
    // hierarchy; /proc/self/cgroup resolves the relative path otherwise.
    let direct = Path::new("/sys/fs/cgroup").join(name);
    std::fs::read_to_string(&direct).ok().or_else(|| {
        let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = line
            .lines()
            .find(|l| l.starts_with("0::"))?
            .trim_start_matches("0::");
        std::fs::read_to_string(Path::new("/sys/fs/cgroup").join(path).join(name)).ok()
    })
}

fn read_cpu_max_v2() -> Option<(u64, u64)> {
    let raw = cgroup_file_v2("cpu.max")?;
    parse_cpu_max(&raw)
}

pub(crate) fn parse_cpu_max(raw: &str) -> Option<(u64, u64)> {
    let mut parts = raw.split_whitespace();
    match (parts.next()?, parts.next()) {
        ("max", _) => None,
        (quota, Some(period)) => Some((quota.parse().ok()?, period.parse().ok()?)),
        _ => None,
    }
}

fn read_cpu_max_v1() -> Option<(u64, u64)> {
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    if quota == 0 || period == 0 {
        return None;
    }
    Some((quota, period))
}

/// Memory limit: cgroup v2 `memory.max`, v1 `memory.limit_in_bytes`, else
/// `/proc/meminfo` MemTotal. "max"/absurd values fall back to the system.
pub fn memory_limit_bytes() -> Option<u64> {
    if let Some(raw) = cgroup_file_v2("memory.max") {
        if let Some(bytes) = parse_memory_max(&raw) {
            return Some(bytes);
        }
    }
    let v1 = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .ok()
        .and_then(|raw| parse_memory_max(&raw));
    if v1.is_some() {
        return v1;
    }
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|raw| {
            raw.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kib| kib.parse::<u64>().ok())
                .map(|kib| kib * 1024)
        })
}

pub(crate) fn parse_memory_max(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    let bytes: u64 = raw.parse().ok()?;
    // The v1 fallback reports HUGE_MAX on unlimited hosts.
    (bytes > 0 && bytes < u64::MAX / 2).then_some(bytes)
}

pub fn disk_free_bytes(path: &Path) -> std::io::Result<u64> {
    let stat = rustix::fs::statvfs(path)?;
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

// ---- derivations (pure; unit-tested) ----

pub fn derive_memory_bytes(memory_bytes: u64) -> usize {
    clamp_bytes(memory_bytes / 128, 16 * 1024 * 1024, 1024 * 1024 * 1024) as usize
}

pub fn derive_metadata_bytes(memory_bytes: u64) -> usize {
    clamp_bytes(memory_bytes / 4096, 1024 * 1024, 16 * 1024 * 1024) as usize
}

pub fn derive_lance_index_bytes(memory_bytes: u64) -> usize {
    clamp_bytes(memory_bytes / 32, 64 * 1024 * 1024, 2 * 1024 * 1024 * 1024) as usize
}

pub fn derive_lance_metadata_bytes(memory_bytes: u64) -> usize {
    clamp_bytes(memory_bytes / 128, 8 * 1024 * 1024, 512 * 1024 * 1024) as usize
}

pub fn derive_fetch_concurrency(cpus: usize) -> usize {
    (cpus.saturating_mul(4)).clamp(8, 64)
}

/// A quarter of the free space on the cache filesystem, capped: the auto
/// budget assumes the directory is (mostly) dedicated to the cache tier,
/// and the cap keeps a mis-shared volume from claiming terabytes.
pub fn derive_disk_bytes(free: u64) -> usize {
    clamp_bytes(free / 4, 64 * 1024 * 1024, 256 * 1024 * 1024 * 1024) as usize
}

fn clamp_bytes(value: u64, min: u64, max: u64) -> u64 {
    value.clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_quota_and_memory() {
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max("200000 100000\n"), Some((200_000, 100_000)));
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("8589934592\n"), Some(8_589_934_592));
        assert_eq!(parse_memory_max(&u64::MAX.to_string()), None);
    }

    #[test]
    fn derivations_reproduce_the_8gib_pod_defaults() {
        let eight_gib = 8 * 1024 * 1024 * 1024;
        assert_eq!(derive_memory_bytes(eight_gib), 64 * 1024 * 1024);
        assert_eq!(derive_metadata_bytes(eight_gib), 2 * 1024 * 1024);
        assert_eq!(derive_lance_index_bytes(eight_gib), 256 * 1024 * 1024);
        assert_eq!(derive_lance_metadata_bytes(eight_gib), 64 * 1024 * 1024);
        assert_eq!(derive_fetch_concurrency(4), 16);
        // Tiny pods clamp to safe floors, huge hosts to caps.
        assert_eq!(derive_memory_bytes(0), 16 * 1024 * 1024);
        assert_eq!(derive_lance_index_bytes(u64::MAX), 2 * 1024 * 1024 * 1024);
        assert_eq!(derive_disk_bytes(u64::MAX), 256 * 1024 * 1024 * 1024);
        assert_eq!(derive_disk_bytes(0), 64 * 1024 * 1024);
        // A dedicated 1 TiB NVMe volume yields a 256 GiB tier (cap), and a
        // 100 GiB volume its quarter.
        assert_eq!(
            derive_disk_bytes(100 * 1024 * 1024 * 1024),
            25 * 1024 * 1024 * 1024
        );
    }
}
