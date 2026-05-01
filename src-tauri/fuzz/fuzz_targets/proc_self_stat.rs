#![no_main]
//! Fuzz target for the `/proc/self/stat` parser used by the resource
//! monitor to compute CPU% on Linux.
//!
//! Contract: arbitrary bytes (kernel can technically deliver malformed
//! `stat` payloads during a process exec race, and `prctl(PR_SET_NAME)`
//! lets a program embed any byte sequence except `\n` into `comm`)
//! must NEVER panic. Returning `Err(_)` is fine — the resource monitor
//! reports 0% CPU when parsing fails, which is the correct behaviour.

use libfuzzer_sys::fuzz_target;

#[cfg(target_os = "linux")]
fuzz_target!(|data: &[u8]| {
    let s = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return,
    };
    let _ = tablio_lib::commands::system::parse_self_stat_total_jiffies(s);
});

#[cfg(not(target_os = "linux"))]
fuzz_target!(|_data: &[u8]| {
    // The parser is `#[cfg(target_os = "linux")]` so this target is a
    // no-op on macOS/Windows. cargo-fuzz still happily compiles it.
});
