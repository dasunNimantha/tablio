//! Filesystem path normalization helpers shared across the app.
//!
//! Tablio takes user-typed paths in many places: the SQLite database
//! file in the connection dialog, SSH identity files, backup/restore
//! and import/export targets. Users — especially on Linux and macOS —
//! routinely write `~/foo` expecting their shell's tilde expansion,
//! but those values arrive in the backend verbatim because no shell
//! ever touched them.
//!
//! [`expand_tilde`] mirrors the shell behaviour for the two forms we
//! care about (`~` and `~/...`). We deliberately do *not* support
//! `~user` expansion: it requires querying the system password database
//! and almost nobody types it into a GUI dialog.

use std::path::PathBuf;

/// Expand a leading `~` or `~/` against the current user's home
/// directory. All other inputs (including `~user`, absolute paths,
/// relative paths, and Windows paths) are returned unchanged.
///
/// Returns the original input when the home directory cannot be
/// resolved, on the assumption that whatever consumes the path will
/// produce a clearer error than this helper can.
pub fn expand_tilde(path: &str) -> String {
    if path == "~" {
        return match dirs::home_dir() {
            Some(home) => home.to_string_lossy().into_owned(),
            None => path.to_string(),
        };
    }

    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(stripped).to_string_lossy().into_owned();
        }
    }

    // Windows convention: `~\foo` should expand the same as `~/foo`.
    #[cfg(windows)]
    if let Some(stripped) = path.strip_prefix("~\\") {
        if let Some(home) = dirs::home_dir() {
            return home.join(stripped).to_string_lossy().into_owned();
        }
    }

    path.to_string()
}

/// Same as [`expand_tilde`] but returns a [`PathBuf`].
#[allow(dead_code)]
pub fn expand_tilde_path(path: &str) -> PathBuf {
    PathBuf::from(expand_tilde(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_for_absolute_and_relative_paths() {
        assert_eq!(expand_tilde("/etc/hosts"), "/etc/hosts");
        assert_eq!(expand_tilde("./foo.db"), "./foo.db");
        assert_eq!(expand_tilde("foo.db"), "foo.db");
        assert_eq!(expand_tilde(""), "");
    }

    #[test]
    fn does_not_expand_user_form() {
        // `~root/...` is intentionally left alone.
        assert_eq!(expand_tilde("~root/foo"), "~root/foo");
    }

    #[test]
    fn expands_bare_tilde_when_home_known() {
        if let Some(home) = dirs::home_dir() {
            assert_eq!(expand_tilde("~"), home.to_string_lossy());
        }
    }

    #[test]
    fn expands_tilde_slash_when_home_known() {
        if let Some(home) = dirs::home_dir() {
            let expected = home.join("database.db").to_string_lossy().into_owned();
            assert_eq!(expand_tilde("~/database.db"), expected);
        }
    }

    #[test]
    fn expands_nested_tilde_slash() {
        if let Some(home) = dirs::home_dir() {
            let expected = home
                .join("Documents")
                .join("data")
                .join("app.db")
                .to_string_lossy()
                .into_owned();
            assert_eq!(expand_tilde("~/Documents/data/app.db"), expected);
        }
    }

    #[test]
    fn does_not_expand_double_tilde() {
        // `~~` is a literal — only a single leading `~` is treated as
        // the home shortcut.
        assert_eq!(expand_tilde("~~/foo"), "~~/foo");
        assert_eq!(expand_tilde("~~"), "~~");
    }

    #[test]
    fn does_not_expand_tilde_in_middle_of_path() {
        // Tilde anywhere except the very start is just a literal.
        assert_eq!(expand_tilde("/var/~/foo"), "/var/~/foo");
        assert_eq!(expand_tilde("./foo~bar"), "./foo~bar");
        assert_eq!(expand_tilde("a~"), "a~");
    }

    #[test]
    fn untrimmed_leading_whitespace_is_not_treated_as_tilde() {
        // Callers that want shell-like behaviour should `.trim()`
        // first; we deliberately don't, so that paths that legally
        // begin with whitespace round-trip unchanged.
        assert_eq!(expand_tilde(" ~/foo"), " ~/foo");
        assert_eq!(expand_tilde("\t~/foo"), "\t~/foo");
    }

    #[test]
    fn returns_empty_input_unchanged() {
        assert_eq!(expand_tilde(""), "");
    }

    #[test]
    fn idempotent_on_already_resolved_path() {
        if let Some(home) = dirs::home_dir() {
            let resolved = home.join("data.db").to_string_lossy().into_owned();
            assert_eq!(expand_tilde(&resolved), resolved);
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_does_not_expand_tilde_backslash() {
        // On Unix, `\` is not a path separator, so `~\foo` is just a
        // weird literal filename and must pass through unchanged.
        assert_eq!(expand_tilde("~\\foo"), "~\\foo");
    }

    #[cfg(windows)]
    #[test]
    fn windows_expands_tilde_backslash() {
        if let Some(home) = dirs::home_dir() {
            let expected = home.join("foo.db").to_string_lossy().into_owned();
            assert_eq!(expand_tilde("~\\foo.db"), expected);
        }
    }
}
