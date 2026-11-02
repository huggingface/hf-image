//! Progress for the CLI to draw, as JSON lines on stdout before the result: per layer
//! `{"progress": {"id", "status", "current"?, "total"?}}`, and `{"status"}` for the whole image.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Byte progress is printed at most this often per layer.
const INTERVAL: Duration = Duration::from_millis(100);

/// One layer's progress, named by its digest's first 12 hex digits as `docker pull` does.
pub struct Progress {
    id: String,
    total: Option<u64>,
    current: AtomicU64,
    last: Mutex<Option<Instant>>,
}

impl Progress {
    pub fn new(digest: &str, total: Option<u64>) -> Self {
        let hex = digest.split_once(':').map_or(digest, |(_, h)| h);
        Self { id: hex[..hex.len().min(12)].into(), total, current: AtomicU64::new(0), last: Mutex::new(None) }
    }

    /// A new step, printed at once.
    pub fn status(&self, status: &str) {
        *self.last.lock().unwrap() = Some(Instant::now());
        self.print(status, false);
    }

    /// A new step with byte progress (`Downloading`, `Pushing`), from 0.
    pub fn start(&self, status: &str) {
        self.current.store(0, Ordering::Relaxed);
        *self.last.lock().unwrap() = Some(Instant::now());
        self.print(status, true);
    }

    /// `n` more bytes of the step [`Progress::start`] began.
    pub fn advance(&self, status: &str, n: u64) {
        self.current.fetch_add(n, Ordering::Relaxed);
        let mut last = self.last.lock().unwrap();
        if last.is_some_and(|t| t.elapsed() < INTERVAL) {
            return;
        }
        *last = Some(Instant::now());
        drop(last);
        self.print(status, true);
    }

    fn print(&self, status: &str, bytes: bool) {
        let mut p = serde_json::json!({ "id": self.id, "status": status });
        if bytes {
            p["current"] = self.current.load(Ordering::Relaxed).into();
            if let Some(total) = self.total {
                p["total"] = total.into();
            }
        }
        println!("{}", serde_json::json!({ "progress": p }));
    }

    /// A step of the whole image.
    pub fn image(status: &str) {
        println!("{}", serde_json::json!({ "status": status }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_short_digests() {
        assert_eq!(Progress::new(&format!("sha256:{}", "ab".repeat(32)), None).id, "abababababab");
        assert_eq!(Progress::new("short", None).id, "short");
    }
}
