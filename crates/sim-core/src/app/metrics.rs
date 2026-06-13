use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Protocol-agnostic request/error counters surfaced in the TUI. Adapters record
/// requests under free-form `kind` labels (e.g. `"who_is"`, `"read_property"`,
/// `"read_holding_registers"`, `"browse"`), so the dashboard works for any
/// protocol without the core knowing the label set.
#[derive(Debug, Default)]
pub struct AppMetrics {
    requests: AtomicU64,
    errors: AtomicU64,
    listening: AtomicBool,
    named: Mutex<BTreeMap<String, u64>>,
    last_client: Mutex<Option<(String, Instant)>>,
}

impl AppMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one served request of the given `kind`, optionally noting the
    /// client/peer that issued it.
    pub fn record_request(&self, kind: &str, client: Option<&str>) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut named) = self.named.lock() {
            *named.entry(kind.to_string()).or_insert(0) += 1;
        }
        if let Some(client) = client {
            self.set_last_client(client);
        }
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_listening(&self, listening: bool) {
        self.listening.store(listening, Ordering::Relaxed);
    }

    pub fn set_last_client(&self, client: &str) {
        if let Ok(mut guard) = self.last_client.lock() {
            *guard = Some((client.to_string(), Instant::now()));
        }
    }

    pub fn last_client(&self) -> Option<String> {
        self.last_client
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|(c, _)| c.clone()))
    }

    pub fn request_count(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn error_count(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Per-kind counts, sorted by label, for the dashboard breakdown.
    pub fn named_counts(&self) -> Vec<(String, u64)> {
        self.named
            .lock()
            .map(|named| named.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    }

    pub fn is_listening(&self) -> bool {
        self.listening.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_increment_and_break_down_by_kind() {
        let m = AppMetrics::new();
        m.record_request("who_is", Some("10.0.0.1:47808"));
        m.record_request("read_property", Some("10.0.0.1:47808"));
        m.record_request("read_property", None);
        assert_eq!(m.request_count(), 3);
        let counts = m.named_counts();
        assert_eq!(
            counts,
            vec![("read_property".into(), 2), ("who_is".into(), 1)]
        );
        assert_eq!(m.last_client().as_deref(), Some("10.0.0.1:47808"));
    }
}
