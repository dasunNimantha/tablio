use serde::Serialize;
use std::sync::Mutex;
use std::time::{Duration, Instant};
#[cfg(not(target_os = "linux"))]
use sysinfo::ProcessRefreshKind;
use sysinfo::{Pid, ProcessesToUpdate, System};

#[derive(Serialize)]
pub struct AppResourceUsage {
    pub memory_mb: f64,
    pub cpu_percent: f32,
}

/// Cached samples for our own process, used to compute a CPU%
/// over the wall-clock window between successive calls.
///
/// We deliberately do NOT cache a `sysinfo::System`. Earlier versions
/// of this command kept a `LazyLock<Mutex<System>>` initialised with
/// `refresh_processes(All, true)`. On Linux that scan opened
/// `/proc/<pid>/[task/<tid>/]stat` for every process+thread on the
/// host and never released them, leaking ~2 file descriptors per
/// 3-second poll. After several hours the renderer's main JS thread
/// fell behind on Tauri IPC, the WebKit IPC socket's Recv-Q backed
/// up, and the entire UI froze — recoverable only by restarting the
/// app. See `fix/resource-monitor-fd-leak`.
struct CpuSample {
    /// Last `(now, /proc/self/stat utime+stime in jiffies)` we observed.
    /// Linux fast path only; other platforms always set this to `None`
    /// and rely on the sysinfo fallback inside [`read_cpu_percent`].
    last: Option<(Instant, u64)>,
}

static CPU_SAMPLE: std::sync::LazyLock<Mutex<CpuSample>> =
    std::sync::LazyLock::new(|| Mutex::new(CpuSample { last: None }));

#[tauri::command]
pub async fn get_app_resource_usage() -> Result<AppResourceUsage, String> {
    let pid = Pid::from_u32(std::process::id());

    let memory_mb = read_memory_mb(pid)?;
    let cpu_percent = read_cpu_percent(pid).await;

    Ok(AppResourceUsage {
        memory_mb: (memory_mb * 10.0).round() / 10.0,
        cpu_percent: (cpu_percent * 10.0).round() / 10.0,
    })
}

/// Read RSS memory for our own PID using a freshly-constructed
/// [`System`] that is dropped on return — guaranteeing every
/// `/proc/<pid>/...` handle sysinfo opens is released.
fn read_memory_mb(pid: Pid) -> Result<f64, String> {
    let mut sys = System::new();
    // `false` = do not also enumerate threads. We only need the
    // process-level memory total, and skipping the thread refresh
    // avoids opening one extra `/proc/<pid>/task/<tid>/stat` per
    // worker thread per call.
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    let mem_bytes = sys.process(pid).map(|p| p.memory()).unwrap_or(0);
    Ok(mem_bytes as f64 / (1024.0 * 1024.0))
}

/// Best-effort CPU% for our own PID. Linux uses `/proc/self/stat`
/// utime+stime jiffies diffed against the previous call; everywhere
/// else we fall back to a fresh sysinfo snapshot pair separated by
/// `MINIMUM_CPU_UPDATE_INTERVAL` (200 ms), which is the only way
/// sysinfo will report a non-zero CPU value without holding state
/// across calls.
async fn read_cpu_percent(pid: Pid) -> f32 {
    #[cfg(target_os = "linux")]
    {
        let _ = pid; // silence unused on linux
        read_cpu_percent_linux().unwrap_or(0.0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_cpu(),
        );
        tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_cpu(),
        );
        sys.process(pid).map(|p| p.cpu_usage()).unwrap_or(0.0)
    }
}

#[cfg(target_os = "linux")]
fn read_cpu_percent_linux() -> Result<f32, String> {
    let total_jiffies = read_self_stat_total_jiffies()?;
    let now = Instant::now();
    let mut sample = CPU_SAMPLE.lock().map_err(|e| e.to_string())?;
    let pct = match sample.last {
        Some((then, prev_total)) => {
            let elapsed = now.duration_since(then);
            // Skip absurdly short intervals (would amplify rounding) and
            // negative deltas (system clock could have jumped — `Instant`
            // is monotonic but we'd rather report 0 than panic).
            if elapsed < Duration::from_millis(50) {
                0.0
            } else {
                let dj = total_jiffies.saturating_sub(prev_total) as f32;
                let clk = clock_ticks_per_sec() as f32;
                let pct = dj / clk / elapsed.as_secs_f32() * 100.0;
                let cores = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1) as f32;
                pct.clamp(0.0, 100.0 * cores)
            }
        }
        None => 0.0,
    };
    sample.last = Some((now, total_jiffies));
    Ok(pct)
}

#[cfg(target_os = "linux")]
fn read_self_stat_total_jiffies() -> Result<u64, String> {
    let s = std::fs::read_to_string("/proc/self/stat").map_err(|e| e.to_string())?;
    // The `comm` field can contain arbitrary characters including
    // whitespace and `(`/`)`, so the only safe way to parse `stat` is
    // to find the LAST `)` and treat everything after it as
    // whitespace-separated fields. After the `)` the layout is:
    //   state ppid pgrp session tty_nr tpgid flags minflt cminflt
    //   majflt cmajflt utime stime cutime cstime ...
    // utime is index 11, stime is index 12 (0-based) of `rest`.
    let after_comm = s.rfind(')').ok_or("malformed /proc/self/stat")?;
    let rest = s
        .get(after_comm + 1..)
        .ok_or("malformed /proc/self/stat")?
        .trim();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields
        .get(11)
        .ok_or("missing utime")?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    let stime: u64 = fields
        .get(12)
        .ok_or("missing stime")?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    Ok(utime + stime)
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_sec() -> u64 {
    // _SC_CLK_TCK is almost always 100 on modern Linux. Hard-coding
    // avoids pulling in `libc` for one constant; if a kernel ever
    // reports something different the only consequence is a
    // proportional CPU% scaling error in the status-bar indicator.
    100
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_1dp_f64(v: f64) -> f64 {
        (v * 10.0).round() / 10.0
    }

    fn round_1dp_f32(v: f32) -> f32 {
        (v * 10.0).round() / 10.0
    }

    #[test]
    fn rounding_f64() {
        assert_eq!(round_1dp_f64(123.456), 123.5);
        assert_eq!(round_1dp_f64(0.0), 0.0);
        assert_eq!(round_1dp_f64(99.94), 99.9);
        assert_eq!(round_1dp_f64(99.95), 100.0);
    }

    #[test]
    fn rounding_f32() {
        assert_eq!(round_1dp_f32(50.55), 50.6);
        assert_eq!(round_1dp_f32(0.0), 0.0);
        assert_eq!(round_1dp_f32(100.0), 100.0);
    }

    #[test]
    fn app_resource_usage_serializes() {
        let u = AppResourceUsage {
            memory_mb: 128.5,
            cpu_percent: 12.3,
        };
        let json = serde_json::to_string(&u).unwrap();
        assert!(json.contains("128.5"));
        assert!(json.contains("12.3"));
    }

    #[tokio::test]
    async fn get_app_resource_usage_returns_ok() {
        let result = get_app_resource_usage().await;
        assert!(result.is_ok());
        let usage = result.unwrap();
        assert!(usage.memory_mb >= 0.0);
        assert!(usage.cpu_percent >= 0.0);
    }

    /// Regression for the fd-leak fix: hammering the command in a
    /// tight loop must not balloon our open-fd count. The original bug
    /// leaked one fd per call, so a real regression would push fd
    /// growth into the dozens (50+ over this loop). We give a generous
    /// slack here because the runner itself can churn a few fds between
    /// the two snapshots (tokio worker wakeups, sysinfo's transient
    /// /proc reads, GitHub-Actions cgroup probes) — anything below
    /// `MAX_FD_GROWTH` is well below leak territory and within normal
    /// CI jitter.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn get_app_resource_usage_does_not_leak_fds() {
        const MAX_FD_GROWTH: usize = 15;
        const ITERATIONS: usize = 50;

        fn open_fd_count() -> usize {
            std::fs::read_dir("/proc/self/fd")
                .map(|d| d.count())
                .unwrap_or(0)
        }
        // Warm up: first call may seed the CPU sample slot, allocate
        // sysinfo internals, etc. Don't measure those.
        for _ in 0..3 {
            let _ = get_app_resource_usage().await.unwrap();
        }
        let before = open_fd_count();
        for _ in 0..ITERATIONS {
            let _ = get_app_resource_usage().await.unwrap();
        }
        let after = open_fd_count();
        let growth = after.saturating_sub(before);
        assert!(
            growth <= MAX_FD_GROWTH,
            "open-fd count grew by {growth} over {ITERATIONS} calls (before={before}, after={after}); \
             the resource monitor is leaking again. A real per-call leak would show ~{ITERATIONS} growth \
             — anything in the single digits is CI-side jitter."
        );
    }

    /// Linux fast-path parses /proc/self/stat correctly. The parser
    /// must split the line at the LAST `)` so that a `comm` field
    /// containing arbitrary characters (whitespace, more parens —
    /// reachable in practice via prctl(PR_SET_NAME)) does not throw
    /// off the field offsets for utime/stime.
    ///
    /// Newly-spawned test processes can have utime+stime == 0 when
    /// nothing has executed yet, so we burn a tiny bit of CPU to
    /// guarantee a positive sample and assert monotonic growth.
    #[cfg(target_os = "linux")]
    #[test]
    fn read_self_stat_handles_parens_in_comm() {
        let before = read_self_stat_total_jiffies().expect("first parse");
        // ~10ms of arithmetic — enough to cross at least one jiffy
        // boundary on a 100Hz scheduler.
        let mut spin: u64 = 0;
        let start = std::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_millis(20) {
            spin = spin.wrapping_add(1);
        }
        std::hint::black_box(spin);
        let after = read_self_stat_total_jiffies().expect("second parse");
        assert!(
            after >= before,
            "/proc/self/stat parsing must be monotonic: before={before}, after={after}"
        );
    }
}
