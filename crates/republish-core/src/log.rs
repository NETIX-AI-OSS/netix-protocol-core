use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => formatter.write_str("INFO"),
            Self::Warning => formatter.write_str("WARN"),
            Self::Error => formatter.write_str("ERROR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub sequence: u64,
    pub elapsed: Duration,
    pub level: LogLevel,
    pub message: String,
}

pub struct LogBuffer {
    entries: VecDeque<LogEntry>,
    next_sequence: u64,
    started_at: Instant,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            next_sequence: 1,
            started_at: Instant::now(),
            capacity,
        }
    }

    pub fn push(&mut self, level: LogLevel, message: impl Into<String>) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(LogEntry {
            sequence: self.next_sequence,
            elapsed: self.started_at.elapsed(),
            level,
            message: message.into(),
        });
        self.next_sequence += 1;
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn entries(&self) -> &VecDeque<LogEntry> {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_is_empty() {
        let buf = LogBuffer::new(10);
        assert!(buf.entries().is_empty());
    }

    #[test]
    fn push_increments_sequence() {
        let mut buf = LogBuffer::new(10);
        buf.push(LogLevel::Info, "first");
        buf.push(LogLevel::Warning, "second");

        let entries: Vec<_> = buf.entries().iter().collect();
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
    }

    #[test]
    fn push_records_level_and_message() {
        let mut buf = LogBuffer::new(10);
        buf.push(LogLevel::Error, "boom");

        let entry = buf.entries().front().unwrap();
        assert_eq!(entry.level, LogLevel::Error);
        assert_eq!(entry.message, "boom");
    }

    #[test]
    fn ring_eviction_drops_oldest_when_capacity_exceeded() {
        let mut buf = LogBuffer::new(3);
        buf.push(LogLevel::Info, "a");
        buf.push(LogLevel::Info, "b");
        buf.push(LogLevel::Info, "c");
        buf.push(LogLevel::Info, "d"); // evicts "a"

        assert_eq!(buf.entries().len(), 3);
        let messages: Vec<&str> = buf.entries().iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, vec!["b", "c", "d"]);
    }

    #[test]
    fn capacity_of_one_only_keeps_latest() {
        let mut buf = LogBuffer::new(1);
        buf.push(LogLevel::Info, "first");
        buf.push(LogLevel::Info, "second");

        assert_eq!(buf.entries().len(), 1);
        assert_eq!(buf.entries().front().unwrap().message, "second");
    }

    #[test]
    fn clear_empties_entries() {
        let mut buf = LogBuffer::new(10);
        buf.push(LogLevel::Info, "hello");
        buf.push(LogLevel::Warning, "world");
        buf.clear();

        assert!(buf.entries().is_empty());
    }

    #[test]
    fn sequence_continues_after_clear() {
        let mut buf = LogBuffer::new(10);
        buf.push(LogLevel::Info, "a");
        buf.push(LogLevel::Info, "b");
        buf.clear();
        buf.push(LogLevel::Info, "c");

        // Sequence is NOT reset by clear — it monotonically increases.
        assert_eq!(buf.entries().front().unwrap().sequence, 3);
    }

    #[test]
    fn elapsed_is_non_decreasing() {
        let mut buf = LogBuffer::new(10);
        buf.push(LogLevel::Info, "first");
        // A tiny sleep is not ideal, but elapsed() resolution on all platforms
        // should be enough to show that two consecutive pushes record a
        // non-negative elapsed difference.
        buf.push(LogLevel::Info, "second");

        let entries: Vec<_> = buf.entries().iter().collect();
        assert!(entries[1].elapsed >= entries[0].elapsed);
    }
}
