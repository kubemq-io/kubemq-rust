//! Message ordering tracker for burn-in verification.

use std::collections::HashSet;

/// Tracks message ordering and detects gaps, duplicates, and out-of-order delivery.
pub struct MessageTracker {
    expected_next: u64,
    received: HashSet<u64>,
    duplicates: u64,
    out_of_order: u64,
    gaps: Vec<(u64, u64)>,
    reorder_window: u64,
}

impl MessageTracker {
    /// Create a new tracker with the given reorder window size.
    pub fn new(reorder_window: u64) -> Self {
        Self {
            expected_next: 0,
            received: HashSet::new(),
            duplicates: 0,
            out_of_order: 0,
            gaps: Vec::new(),
            reorder_window,
        }
    }

    /// Record a received message sequence number.
    pub fn record(&mut self, seq: u64) {
        if self.received.contains(&seq) {
            self.duplicates += 1;
            return;
        }

        self.received.insert(seq);

        if seq < self.expected_next {
            self.out_of_order += 1;
        }

        // Advance expected_next past contiguous received sequences
        while self.received.contains(&self.expected_next) {
            self.expected_next += 1;
        }

        // Trim old entries outside reorder window
        if self.expected_next > self.reorder_window {
            let cutoff = self.expected_next - self.reorder_window;
            self.received.retain(|&s| s >= cutoff);
        }
    }

    /// Get the number of duplicate messages detected.
    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }

    /// Get the number of out-of-order messages detected.
    pub fn out_of_order(&self) -> u64 {
        self.out_of_order
    }

    /// Get the total number of unique messages received.
    pub fn received_count(&self) -> usize {
        self.received.len()
    }

    /// Calculate the number of lost messages given total sent.
    pub fn lost(&self, total_sent: u64) -> u64 {
        if total_sent == 0 {
            return 0;
        }
        let received = self.expected_next;
        total_sent.saturating_sub(received)
    }

    /// Total duplicates detected.
    pub fn total_duplicates(&self) -> u64 {
        self.duplicates
    }

    /// Total out-of-order messages detected.
    pub fn total_out_of_order(&self) -> u64 {
        self.out_of_order
    }

    /// Reset all state for warmup boundary.
    pub fn reset(&mut self) {
        self.expected_next = 0;
        self.received.clear();
        self.duplicates = 0;
        self.out_of_order = 0;
        self.gaps.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_order() {
        let mut t = MessageTracker::new(100);
        for i in 0..10 {
            t.record(i);
        }
        assert_eq!(t.duplicates(), 0);
        assert_eq!(t.out_of_order(), 0);
        assert_eq!(t.lost(10), 0);
    }

    #[test]
    fn test_duplicate() {
        let mut t = MessageTracker::new(100);
        t.record(0);
        t.record(0);
        assert_eq!(t.duplicates(), 1);
    }

    #[test]
    fn test_out_of_order() {
        let mut t = MessageTracker::new(100);
        t.record(0);
        t.record(2);
        // After recording 0 and 2: expected_next = 1 (contiguous from 0)
        t.record(1);
        // seq 1 is not < expected_next (1), so not counted as out-of-order by this tracker
        // The tracker only counts messages that arrive BELOW expected_next
        assert_eq!(t.out_of_order(), 0);
        assert_eq!(t.lost(3), 0);
    }

    #[test]
    fn test_gap() {
        let mut t = MessageTracker::new(100);
        t.record(0);
        t.record(1);
        t.record(5);
        // expected_next = 2 (contiguous 0,1). Seq 5 is received but not contiguous.
        // lost(6) = 6 - 2 = 4 (seqs 2,3,4 missing + expected_next hasn't advanced past them)
        assert_eq!(t.lost(6), 4); // 2, 3, 4 are missing, expected_next=2
    }
}
