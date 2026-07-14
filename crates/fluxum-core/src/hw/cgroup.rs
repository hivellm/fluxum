//! cgroup v1/v2 limit detection (SPEC-016 HWA-001…HWA-004).
//!
//! Pure string parsers are platform-independent (unit-tested everywhere
//! against fixture strings); the filesystem readers are Linux-only and treat
//! every absence or read failure as "no limit" — absent limits are normal,
//! never an error (HWA-003).

/// cgroup v1 "unlimited" sentinel: `i64::MAX` rounded down to the page size.
const V1_UNLIMITED_SENTINEL: u64 = 0x7FFF_FFFF_FFFF_F000;

/// Container limits discovered from the cgroup filesystem.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CgroupLimits {
    /// CPU quota in cores (e.g. `1.5`); `None` when unlimited or undetected.
    pub cpu_quota: Option<f64>,
    /// Memory limit in bytes; `None` when unlimited or undetected.
    pub memory_limit_bytes: Option<u64>,
}

/// Parse cgroup v2 `cpu.max` (`"<quota> <period>"` or `"max <period>"`)
/// into a quota in cores. Sentinel `max` means no limit (HWA-002).
pub fn parse_cpu_max_v2(contents: &str) -> Option<f64> {
    let mut parts = contents.split_whitespace();
    let quota_raw = parts.next()?;
    if quota_raw == "max" {
        return None;
    }
    let quota: u64 = quota_raw.parse().ok()?;
    let period: u64 = match parts.next() {
        Some(p) => p.parse().ok()?,
        None => 100_000, // kernel default period
    };
    if quota == 0 || period == 0 {
        return None;
    }
    Some(quota as f64 / period as f64)
}

/// Parse cgroup v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us` into a quota in
/// cores. A quota of `-1` (or any non-positive value) means no limit.
pub fn parse_cpu_quota_v1(quota: &str, period: &str) -> Option<f64> {
    let quota: i64 = quota.trim().parse().ok()?;
    if quota <= 0 {
        return None;
    }
    let period: u64 = period.trim().parse().ok()?;
    if period == 0 {
        return None;
    }
    Some(quota as f64 / period as f64)
}

/// Parse a cgroup memory limit (v2 `memory.max`, v1 `memory.limit_in_bytes`).
/// The sentinels `max`, `-1`, and the v1 page-aligned `i64::MAX` all mean
/// "no limit" (HWA-002).
pub fn parse_memory_limit(contents: &str) -> Option<u64> {
    let trimmed = contents.trim();
    if trimmed == "max" || trimmed.starts_with('-') {
        return None;
    }
    let value: u64 = trimmed.parse().ok()?;
    if value == 0 || value >= V1_UNLIMITED_SENTINEL {
        return None;
    }
    Some(value)
}

/// Read the limits that apply to this process. Non-Linux hosts have no
/// cgroups; every failure mode degrades to "no limit" (HWA-003).
#[cfg(not(target_os = "linux"))]
pub fn read_limits() -> CgroupLimits {
    CgroupLimits::default()
}

/// Read the limits that apply to this process from `/sys/fs/cgroup`,
/// resolving this process's own cgroup via `/proc/self/cgroup` and falling
/// back to the hierarchy root. Every failure mode degrades to "no limit"
/// (HWA-003); this function never fails.
#[cfg(target_os = "linux")]
pub fn read_limits() -> CgroupLimits {
    linux::read_limits_from("/sys/fs/cgroup", "/proc/self/cgroup")
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{CgroupLimits, parse_cpu_max_v2, parse_cpu_quota_v1, parse_memory_limit};
    use std::path::{Path, PathBuf};

    pub(super) fn read_limits_from(cgroup_root: &str, proc_self: &str) -> CgroupLimits {
        let root = Path::new(cgroup_root);
        let proc_contents = std::fs::read_to_string(proc_self).unwrap_or_default();
        if root.join("cgroup.controllers").is_file() {
            read_v2(root, &proc_contents)
        } else {
            read_v1(root, &proc_contents)
        }
    }

    /// The nearest readable value of `file`, walking from this process's own
    /// cgroup directory up to the hierarchy root.
    fn find_up(mut dir: PathBuf, root: &Path, file: &str) -> Option<String> {
        loop {
            if let Ok(contents) = std::fs::read_to_string(dir.join(file)) {
                return Some(contents);
            }
            if dir == root || !dir.pop() {
                return None;
            }
        }
    }

    fn read_v2(root: &Path, proc_contents: &str) -> CgroupLimits {
        // /proc/self/cgroup (v2): a single line "0::/path".
        let rel = proc_contents
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .map(|p| p.trim().trim_start_matches('/'))
            .unwrap_or("");
        let own = root.join(rel);
        CgroupLimits {
            cpu_quota: find_up(own.clone(), root, "cpu.max")
                .as_deref()
                .and_then(parse_cpu_max_v2),
            memory_limit_bytes: find_up(own, root, "memory.max")
                .as_deref()
                .and_then(parse_memory_limit),
        }
    }

    fn read_v1(root: &Path, proc_contents: &str) -> CgroupLimits {
        // /proc/self/cgroup (v1): lines "N:controller[,controller]:/path".
        let rel_for = |controller: &str| -> &str {
            proc_contents
                .lines()
                .find_map(|line| {
                    let mut parts = line.splitn(3, ':');
                    let _ = parts.next()?;
                    let controllers = parts.next()?;
                    let path = parts.next()?;
                    controllers
                        .split(',')
                        .any(|c| c == controller)
                        .then(|| path.trim().trim_start_matches('/'))
                })
                .unwrap_or("")
        };

        let cpu_root = root.join("cpu");
        let cpu_dir = cpu_root.join(rel_for("cpu"));
        let cpu_quota = match (
            find_up(cpu_dir.clone(), &cpu_root, "cpu.cfs_quota_us"),
            find_up(cpu_dir, &cpu_root, "cpu.cfs_period_us"),
        ) {
            (Some(quota), Some(period)) => parse_cpu_quota_v1(&quota, &period),
            _ => None,
        };

        let mem_root = root.join("memory");
        let mem_dir = mem_root.join(rel_for("memory"));
        let memory_limit_bytes = find_up(mem_dir, &mem_root, "memory.limit_in_bytes")
            .as_deref()
            .and_then(parse_memory_limit);

        CgroupLimits {
            cpu_quota,
            memory_limit_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_cpu_max_fixtures() {
        // SPEC-016 §2 example: cpu.max = "150000 100000" → 1.5 cores.
        assert_eq!(parse_cpu_max_v2("150000 100000"), Some(1.5));
        assert_eq!(parse_cpu_max_v2("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max_v2("50000 100000"), Some(0.5));
        // Sentinel and malformed inputs → no limit.
        assert_eq!(parse_cpu_max_v2("max 100000"), None);
        assert_eq!(parse_cpu_max_v2("max"), None);
        assert_eq!(parse_cpu_max_v2(""), None);
        assert_eq!(parse_cpu_max_v2("garbage 100000"), None);
        assert_eq!(parse_cpu_max_v2("100000 0"), None);
        // Missing period defaults to the kernel's 100ms.
        assert_eq!(parse_cpu_max_v2("150000"), Some(1.5));
    }

    #[test]
    fn v1_cpu_quota_fixtures() {
        assert_eq!(parse_cpu_quota_v1("150000", "100000"), Some(1.5));
        assert_eq!(parse_cpu_quota_v1("-1", "100000"), None, "-1 = unlimited");
        assert_eq!(parse_cpu_quota_v1("0", "100000"), None);
        assert_eq!(parse_cpu_quota_v1("100000", "0"), None);
        assert_eq!(parse_cpu_quota_v1("junk", "100000"), None);
    }

    #[test]
    fn memory_limit_fixtures() {
        // SPEC-016 §2 example: memory.max = 536870912 → 512 MiB.
        assert_eq!(parse_memory_limit("536870912"), Some(512 << 20));
        assert_eq!(parse_memory_limit("536870912\n"), Some(512 << 20));
        assert_eq!(parse_memory_limit("max"), None, "v2 sentinel");
        assert_eq!(parse_memory_limit("-1"), None, "v1 sentinel");
        assert_eq!(
            parse_memory_limit("9223372036854771712"),
            None,
            "v1 page-aligned i64::MAX sentinel"
        );
        assert_eq!(parse_memory_limit("0"), None);
        assert_eq!(parse_memory_limit("bogus"), None);
    }

    #[test]
    fn read_limits_never_panics_anywhere() {
        // On non-Linux this is a constant; on Linux it reads the live cgroup
        // fs. Either way it must return without panicking (HWA-003/004).
        let _ = read_limits();
    }
}
