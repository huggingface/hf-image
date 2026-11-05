//! Progress for the CLI to draw, as JSON lines on stdout before the result:
//! - `{"image": {"name", "direction"}}` once, first;
//! - `{"layer": {"id", "size", "state"}}` as a layer moves: `waiting`, `downloading`, `downloaded`,
//!   `extracting`, `uploading`, `done`, `local` (in the image store already) or `exists` (in the
//!   registry already);
//! - `{"transfer": {"bytes", "total"}}` every [`INTERVAL`] while layer bytes move: of all the
//!   image's, those in the store (pull) or read (push), or there already;
//! - `{"network": {"bytes", "total"}}` likewise on a pull: bytes received, of those planned;
//! - `{"status"}` for a step of the whole image;
//! - `{"summary": {...}}` once, last.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The transfer line is printed this often.
const INTERVAL: Duration = Duration::from_millis(100);

/// The image a pull or push moves; `direction`: `"down"` or `"up"`.
pub fn image(name: &str, direction: &str) {
    print(serde_json::json!({ "image": { "name": name, "direction": direction } }));
}

/// A step of the whole image.
pub fn status(text: &str) {
    print(serde_json::json!({ "status": text }));
}

/// The totals of a pull or push.
pub fn summary(summary: serde_json::Value) {
    print(serde_json::json!({ "summary": summary }));
}

fn print(event: serde_json::Value) {
    println!("{event}");
}

/// One layer, named by its digest's first 12 hex digits as `docker pull` does.
pub struct Layer {
    id: String,
    size: u64,
    bar: Bar,
}

impl Layer {
    pub fn new(digest: &str, size: u64) -> Self {
        let hex = digest.split_once(':').map_or(digest, |(_, h)| h);
        Self { id: hex[..hex.len().min(12)].into(), size, bar: Bar::default() }
    }

    pub fn state(&self, state: &str) {
        print(serde_json::json!({ "layer": { "id": self.id, "size": self.size, "state": state } }));
    }

    /// Counts `n` of its bytes on the bar.
    pub fn add(&self, n: u64) {
        self.bar.add(n);
    }
}

/// Layer bytes to move, and of those, moved or there already: what the bar shows.
#[derive(Clone, Default)]
pub struct Bar {
    total: Arc<AtomicU64>,
    done: Arc<AtomicU64>,
}

impl Bar {
    /// A layer of the image, waiting; its bytes count on this bar.
    pub fn layer(&self, digest: &str, size: u64) -> Layer {
        self.total.fetch_add(size, Ordering::Relaxed);
        let layer = Layer { bar: self.clone(), ..Layer::new(digest, size) };
        layer.state("waiting");
        layer
    }

    pub fn add(&self, n: u64) {
        self.done.fetch_add(n, Ordering::Relaxed);
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Prints the `transfer` events.
    pub fn meter(&self) -> Meter {
        let (done, total) = (self.done.clone(), self.total.clone());
        Meter::start("transfer", move || done.load(Ordering::Relaxed), move || total.load(Ordering::Relaxed))
    }
}

/// Bytes moved, sampled from `bytes` against `total`: printed every [`INTERVAL`].
pub struct Meter {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Moved>,
}

/// What a [`Meter`] saw: the bytes, when the first moved, and the time from the first to the last.
pub struct Moved {
    pub bytes: u64,
    pub first: Option<Instant>,
    pub secs: f64,
}

impl Meter {
    /// Prints `{event: {"bytes", "total"}}`.
    pub fn start(
        event: &'static str,
        bytes: impl Fn() -> u64 + Send + 'static,
        total: impl Fn() -> u64 + Send + 'static,
    ) -> Self {
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(INTERVAL);
            let (mut before, mut seen) = (Instant::now(), 0);
            let (mut first, mut last) = (None, None);
            loop {
                let done = tokio::select! {
                    _ = &mut stopped => true,
                    _ = tick.tick() => false,
                };
                let (now, n) = (Instant::now(), bytes());
                if n > seen {
                    first.get_or_insert(before);
                    last = Some(now);
                }
                (before, seen) = (now, n);
                if done {
                    let secs = last.zip(first).map_or(0.0, |(l, f)| (l - f).as_secs_f64());
                    return Moved { bytes: n, first, secs };
                }
                if n > 0 {
                    print(serde_json::json!({ event: { "bytes": n, "total": total().max(n) } }));
                }
            }
        });
        Self { stop, task }
    }

    pub async fn finish(self) -> Moved {
        let _ = self.stop.send(());
        self.task.await.unwrap_or(Moved { bytes: 0, first: None, secs: 0.0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_short_digests() {
        assert_eq!(Layer::new(&format!("sha256:{}", "ab".repeat(32)), 1).id, "abababababab");
        assert_eq!(Layer::new("short", 1).id, "short");
    }

    #[tokio::test]
    async fn a_meter_times_its_first_to_last_byte() {
        let n = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let meter = Meter::start(
            "transfer",
            {
                let n = n.clone();
                move || n.load(std::sync::atomic::Ordering::Relaxed)
            },
            || 100,
        );
        tokio::time::sleep(INTERVAL * 2).await;
        n.store(60, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(INTERVAL * 3).await;
        n.store(100, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(INTERVAL * 2).await;
        let moved = meter.finish().await;
        assert_eq!(moved.bytes, 100);
        assert!(moved.secs > 0.15 && moved.secs < 0.6, "{}", moved.secs);
    }
}
