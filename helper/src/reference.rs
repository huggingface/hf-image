//! Image references: `<registry>/<namespace>/<name>[:<tag>][@<digest>]`.

use std::fmt;

use anyhow::{Context, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub registry: String,
    /// `<namespace>/<name>`.
    pub repo: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl ImageRef {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let (rest, digest) = match s.split_once('@') {
            Some((r, d)) => (r, Some(d.to_string())),
            None => (s, None),
        };
        let (registry, path) = rest.split_once('/').context("expected <registry>/<namespace>/<name>")?;
        if !(registry.contains('.') || registry.contains(':') || registry == "localhost") {
            bail!("{s:?} has no registry host (expected e.g. cr.hf.co/<namespace>/<name>)");
        }
        let (path, tag) = match path.rsplit_once(':') {
            Some((p, t)) if !t.contains('/') => (p, Some(t.to_string())),
            _ => (path, None),
        };
        if path.split('/').count() != 2 || path.split('/').any(str::is_empty) {
            bail!("{s:?}: the repository must be <namespace>/<name>");
        }
        if let Some(d) = &digest
            && (!d.starts_with("sha256:") || d.len() != 71)
        {
            bail!("{s:?}: invalid digest");
        }
        Ok(Self { registry: registry.to_ascii_lowercase(), repo: path.to_string(), tag, digest })
    }

    /// Tag or digest to fetch: `latest` by default.
    pub fn reference(&self) -> String {
        self.digest.clone().or_else(|| self.tag.clone()).unwrap_or_else(|| "latest".into())
    }

    pub fn tag_or_latest(&self) -> String {
        self.tag.clone().unwrap_or_else(|| "latest".into())
    }

    /// `<registry>/<repo>:<tag>`, the name local images get.
    pub fn tagged(&self) -> String {
        format!("{}/{}:{}", self.registry, self.repo, self.tag_or_latest())
    }

    /// Plain HTTP for loopback registries and those listed in `HF_IMAGE_INSECURE_REGISTRIES`.
    pub fn base_url(&self) -> String {
        let host = self.registry.rsplit_once(':').map_or(self.registry.as_str(), |(h, _)| h);
        let listed = std::env::var("HF_IMAGE_INSECURE_REGISTRIES")
            .is_ok_and(|v| v.split(',').any(|r| r.trim().eq_ignore_ascii_case(&self.registry)));
        let insecure = listed || host == "localhost" || host.starts_with("127.");
        format!("{}://{}", if insecure { "http" } else { "https" }, self.registry)
    }
}

impl fmt::Display for ImageRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.registry, self.repo)?;
        if let Some(t) = &self.tag {
            write!(f, ":{t}")?;
        }
        if let Some(d) = &self.digest {
            write!(f, "@{d}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_references() {
        let r = ImageRef::parse("cr.hf.co/acme/app:v1").unwrap();
        assert_eq!((r.registry.as_str(), r.repo.as_str(), r.tag.as_deref()), ("cr.hf.co", "acme/app", Some("v1")));
        assert_eq!(r.base_url(), "https://cr.hf.co");
        let r = ImageRef::parse("127.0.0.1:5055/acme/app").unwrap();
        assert_eq!((r.reference(), r.base_url()), ("latest".to_string(), "http://127.0.0.1:5055".to_string()));
        let d = format!("sha256:{}", "a".repeat(64));
        let r = ImageRef::parse(&format!("localhost:5000/a/b:t@{d}")).unwrap();
        assert_eq!(r.reference(), d);
        assert_eq!(r.tagged(), "localhost:5000/a/b:t");
        assert!(ImageRef::parse("acme/app:v1").is_err(), "no registry");
        assert!(ImageRef::parse("cr.hf.co/app").is_err(), "one segment");
        assert!(ImageRef::parse("cr.hf.co/a/b/c").is_err(), "three segments");
    }
}
