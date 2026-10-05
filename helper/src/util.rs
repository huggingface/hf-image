//! Small helpers: stream adapters and human-readable output.

use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use xet_data::processing::DownloadStream;

/// An xet-core download as a stream that ends after the first error.
pub fn xet_stream(stream: DownloadStream) -> impl Stream<Item = anyhow::Result<Bytes>> {
    futures::stream::unfold(Some(stream), |state| async move {
        let mut s = state?;
        match s.next().await {
            Ok(Some(b)) => Some((Ok(b), Some(s))),
            Ok(None) => None,
            Err(e) => Some((Err(anyhow::Error::new(e).context("download failed")), None)),
        }
    })
}

pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1000.0 && u < UNITS.len() - 1 {
        v /= 1000.0;
        u += 1;
    }
    if u == 0 { format!("{n} B") } else { format!("{v:.1} {}", UNITS[u]) }
}

pub fn secs(d: Duration) -> String {
    format!("{:.1}s", d.as_secs_f64())
}

/// Concurrent layer transfers (`HF_IMAGE_CONCURRENCY`).
pub fn concurrency() -> usize {
    std::env::var("HF_IMAGE_CONCURRENCY").ok().and_then(|v| v.parse().ok()).filter(|n| *n > 0).unwrap_or(6)
}

#[cfg(test)]
mod tests {
    #[test]
    fn human_bytes() {
        assert_eq!(super::bytes(999), "999 B");
        assert_eq!(super::bytes(1_500_000), "1.5 MB");
    }
}
