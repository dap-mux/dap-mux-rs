//! Sequence-number mapping for DAP request/response routing.
//!
//! The multiplexer rewrites `seq` on requests forwarded upstream so the
//! adapter sees a single monotonic sequence. When a response comes back, the
//! [`SeqMap`] resolves the proxy seq to the original `(client_id, client_seq)`
//! pair so the response can be routed to the correct client with its original
//! sequence number restored.

use std::collections::HashMap;

/// A request awaiting its response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    pub client_id: String,
    pub client_seq: i64,
}

/// Mapping from proxy seq numbers back to their originating client request.
#[derive(Debug)]
pub struct SeqMap {
    next_seq: i64,
    pending: HashMap<i64, PendingRequest>,
}

impl Default for SeqMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SeqMap {
    /// Create an empty map. The first allocated proxy seq is `1`.
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            pending: HashMap::new(),
        }
    }

    /// The next proxy seq number that will be allocated.
    pub fn next_seq(&self) -> i64 {
        self.next_seq
    }

    /// Allocate a proxy seq number for a client request.
    ///
    /// Returns the proxy-side seq to use when forwarding upstream.
    pub fn allocate(&mut self, client_id: &str, client_seq: i64) -> i64 {
        let proxy_seq = self.next_seq;
        self.next_seq += 1;
        self.pending.insert(
            proxy_seq,
            PendingRequest {
                client_id: client_id.to_string(),
                client_seq,
            },
        );
        tracing::trace!(proxy_seq, client_id, client_seq, "SeqMap: allocated");
        proxy_seq
    }

    /// Look up and remove the pending request for `proxy_seq`.
    ///
    /// Returns `None` if no pending request exists (already resolved or never
    /// allocated).
    pub fn resolve(&mut self, proxy_seq: i64) -> Option<PendingRequest> {
        self.pending.remove(&proxy_seq)
    }

    /// Remove all pending requests for `client_id`, returning the count removed.
    ///
    /// Call this when a client disconnects to avoid leaking stale mappings.
    pub fn cleanup(&mut self, client_id: &str) -> usize {
        let before = self.pending.len();
        self.pending.retain(|_, p| p.client_id != client_id);
        let removed = before - self.pending.len();
        if removed > 0 {
            tracing::debug!(removed, client_id, "SeqMap: cleaned up pending requests");
        }
        removed
    }

    /// Number of requests currently awaiting responses.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of requests from `client_id` currently awaiting responses.
    pub fn pending_for(&self, client_id: &str) -> usize {
        self.pending
            .values()
            .filter(|p| p.client_id == client_id)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_then_resolve() {
        let mut m = SeqMap::new();
        let proxy = m.allocate("helix", 1);
        assert_eq!(proxy, 1);
        assert_eq!(
            m.resolve(proxy),
            Some(PendingRequest {
                client_id: "helix".into(),
                client_seq: 1
            })
        );
        // Resolving again yields nothing.
        assert_eq!(m.resolve(proxy), None);
    }

    #[test]
    fn monotonic_allocation() {
        let mut m = SeqMap::new();
        assert_eq!(m.allocate("a", 10), 1);
        assert_eq!(m.allocate("b", 20), 2);
        assert_eq!(m.next_seq(), 3);
    }

    #[test]
    fn cleanup_removes_only_that_client() {
        let mut m = SeqMap::new();
        m.allocate("a", 1);
        m.allocate("a", 2);
        m.allocate("b", 1);
        assert_eq!(m.cleanup("a"), 2);
        assert_eq!(m.pending_count(), 1);
    }
}
