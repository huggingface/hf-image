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
            && !crate::oci::is_digest(d)
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
        let insecure = self.loopback() || self.listed("HF_IMAGE_INSECURE_REGISTRIES");
        format!("{}://{}", if insecure { "http" } else { "https" }, self.registry)
    }

    /// `localhost`, or a loopback IP address (`127.0.0.0/8`, `[::1]`), with or without a port.
    fn loopback(&self) -> bool {
        let host = match self.registry.strip_prefix('[') {
            Some(v6) => v6.split(']').next().unwrap_or_default(),
            None => self.registry.split(':').next().unwrap_or_default(),
        };
        host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
    }

    /// Listed in `HF_IMAGE_TOKEN_REGISTRIES`: gets the HF token besides cr.hf.co.
    pub fn gets_token(&self) -> bool {
        self.listed("HF_IMAGE_TOKEN_REGISTRIES")
    }

    /// In the registries the environment variable `var` lists.
    fn listed(&self, var: &str) -> bool {
        std::env::var(var).is_ok_and(|v| self.listed_in(&v))
    }

    /// In `list`, comma-separated `<host>[:<port>]`, matched whole.
    fn listed_in(&self, list: &str) -> bool {
        list.split(',').any(|r| r.trim().eq_ignore_ascii_case(&self.registry))
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
        assert!(ImageRef::parse(&format!("cr.hf.co/a/b@sha256:{}", "../".repeat(21) + "x")).is_err(), "not hex");
    }

    #[test]
    fn only_loopback_addresses_get_plain_http() {
        let base = |image: &str| ImageRef::parse(image).unwrap().base_url();
        for image in ["localhost/a/b", "localhost:5000/a/b", "127.0.0.1:5000/a/b", "127.1.2.3/a/b", "[::1]:5000/a/b"] {
            assert!(base(image).starts_with("http://"), "{image}");
        }
        for image in
            ["127.attacker.example/a/b", "127.0.0.1.example:5000/a/b", "localhost.example/a/b", "[::2]:5000/a/b"]
        {
            assert!(base(image).starts_with("https://"), "{image}");
        }
    }

    #[test]
    fn registry_lists_match_host_and_port_whole() {
        let listed = |image: &str| ImageRef::parse(image).unwrap().listed_in("registry.example, Localhost:5000");
        assert!(listed("localhost:5000/a/b") && listed("registry.example/a/b"));
        for image in ["localhost:5001/a/b", "evil.localhost:5000/a/b", "localhost/a/b", "registry.example:443/a/b"] {
            assert!(!listed(image), "{image}");
        }
    }
}
