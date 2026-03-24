use serde::Serialize;
use sysinfo::{Pid, System};
use std::sync::Mutex;

#[derive(Serialize)]
pub struct AppResourceUsage {
    pub memory_mb: f64,
    pub cpu_percent: f32,
}

static SYS: std::sync::LazyLock<Mutex<System>> = std::sync::LazyLock::new(|| {
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    Mutex::new(sys)
});

#[tauri::command]
pub async fn get_app_resource_usage() -> Result<AppResourceUsage, String> {
    let pid = Pid::from_u32(std::process::id());

    let (memory_mb, cpu_percent) = {
        let mut sys = SYS.lock().map_err(|e| e.to_string())?;
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        match sys.process(pid) {
            Some(proc) => {
                let mem = proc.memory() as f64 / (1024.0 * 1024.0);
                let cpu = proc.cpu_usage();
                (mem, cpu)
            }
            None => (0.0, 0.0),
        }
    };

    Ok(AppResourceUsage {
        memory_mb: (memory_mb * 10.0).round() / 10.0,
        cpu_percent: (cpu_percent * 10.0).round() / 10.0,
    })
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
}
