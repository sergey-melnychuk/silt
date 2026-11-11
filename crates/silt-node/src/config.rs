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
    /// TCP bind address for the lookup fast path (D9); a separate
    /// listener from the push-only feed. Empty string disables it. Loopback
    /// by default — the protocol has no auth (D1).
    pub lookup_bind: String,
    /// Dedicated OS threads serving lookup reads (D9); never tokio workers.
    pub lookup_workers: usize,
}

impl Default for SiltConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8585".into(),
            spool: 128,
            heartbeat_secs: 10,
            lookup_bind: "127.0.0.1:8586".into(),
            lookup_workers: 16,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bind_loopback_and_enable_lookup() {
        let cfg: SiltConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8585");
        assert_eq!(cfg.lookup_bind, "127.0.0.1:8586");
        assert_eq!(cfg.lookup_workers, 16);
    }

    #[test]
    fn empty_lookup_bind_disables_and_unknown_fields_fail() {
        let cfg: SiltConfig = toml::from_str("lookup_bind = \"\"\nspool = 64").unwrap();
        assert!(cfg.lookup_bind.is_empty());
        assert_eq!(cfg.spool, 64);
        assert!(toml::from_str::<SiltConfig>("lookup_port = 1").is_err());
    }
}
