//! Wrapper config (PLAN T1.5): optional `silt.toml` in the working
//! directory; everything has a default. Env-free, secret-free.

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SiltConfig {
    /// TCP bind address for the feed (D1: private network only).
    pub bind: String,
    /// Spool depth in diffs (D5): catch-up window for reconnecting
    /// subscribers; older gaps answer `GapTooOld`.
    pub spool: usize,
    pub heartbeat_secs: u64,
}

impl Default for SiltConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8585".into(),
            spool: 128,
            heartbeat_secs: 10,
        }
    }
}

impl SiltConfig {
    /// Loads `./silt.toml` if present; defaults otherwise. A malformed file
    /// is a hard error (silently ignoring config is worse than refusing).
    pub fn load() -> eyre::Result<Self> {
        match std::fs::read_to_string("silt.toml") {
            Ok(text) => Ok(toml::from_str(&text)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
}
