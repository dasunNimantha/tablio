#![no_main]
//! Fuzz target for the `~/.ssh/config` parser.
//!
//! Contract: arbitrary bytes from the user's config file must NEVER
//! cause `resolve_target` to panic — this code runs on every alias
//! lookup in the connection dialog and a panic there crashes the
//! Tauri renderer.
//!
//! The fuzzer also exercises the `target` argument because it's
//! dialog-supplied and equally untrusted from the parser's point of
//! view.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    // First two bytes pick a target length up to 64; the rest is the
    // config body. Splitting the input gives the fuzzer a deterministic
    // way to mutate target vs body independently.
    let target_len = (u16::from_le_bytes([data[0], data[1]]) as usize) % 64;
    let rest = &data[2..];
    if rest.len() < target_len {
        return;
    }
    let target_bytes = &rest[..target_len];
    let content_bytes = &rest[target_len..];

    let target = match std::str::from_utf8(target_bytes) {
        Ok(s) => s,
        Err(_) => return,
    };
    let content = match std::str::from_utf8(content_bytes) {
        Ok(s) => s,
        Err(_) => return,
    };

    let _ = tablio_lib::commands::ssh_config::resolve_target(content, target);
});
