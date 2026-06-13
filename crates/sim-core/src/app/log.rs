use std::collections::VecDeque;
use std::sync::Mutex;

const LOG_CAPACITY: usize = 50;

#[derive(Debug, Default)]
pub struct AppLog {
    lines: Mutex<VecDeque<(String, u32)>>,
}

impl AppLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, line: impl Into<String>) {
        let line = line.into();
        if let Ok(mut guard) = self.lines.lock() {
            // Collapse consecutive identical lines into a single entry with a
            // repeat count so error storms don't scroll useful history away.
            if let Some((last, count)) = guard.back_mut() {
                if *last == line {
                    *count += 1;
                    return;
                }
            }
            guard.push_back((line, 1));
            while guard.len() > LOG_CAPACITY {
                guard.pop_front();
            }
        }
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .map(|g| {
                g.iter()
                    .map(|(line, count)| {
                        if *count > 1 {
                            format!("{line} (x{count})")
                        } else {
                            line.clone()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_consecutive_identical_lines() {
        let log = AppLog::new();
        log.push("a");
        log.push("a");
        log.push("a");
        log.push("b");
        log.push("a");
        assert_eq!(log.lines(), vec!["a (x3)", "b", "a"]);
    }

    #[test]
    fn evicts_oldest_entries_beyond_capacity() {
        let log = AppLog::new();
        for i in 0..(LOG_CAPACITY + 5) {
            log.push(format!("line {i}"));
        }
        let lines = log.lines();
        assert_eq!(lines.len(), LOG_CAPACITY);
        assert_eq!(lines.first().unwrap(), "line 5");
    }
}
