//! OCI image types, kept loose: unknown fields survive a round trip.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
pub const OCI_CONFIG: &str = "application/vnd.oci.image.config.v1+json";
pub const OCI_LAYER_TAR: &str = "application/vnd.oci.image.layer.v1.tar";
pub const DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
pub const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
pub const DOCKER_CONFIG: &str = "application/vnd.docker.container.image.v1+json";

/// Every manifest type a pull accepts.
pub const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json";

pub fn sha256(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}

pub fn is_index(media_type: &str) -> bool {
    media_type == OCI_INDEX || media_type == DOCKER_LIST
}

/// Image layers (not attestation or artifact blobs), whatever their compression.
pub fn is_layer(media_type: &str) -> bool {
    media_type.starts_with("application/vnd.oci.image.layer.")
        || media_type.starts_with("application/vnd.docker.image.rootfs.")
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Platform {
    /// `<os>/<arch>[/<variant>]`, normalized.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if !(2..=3).contains(&parts.len()) || parts.iter().any(|p| p.is_empty()) {
            anyhow::bail!("platform {s:?} is not <os>/<arch>[/<variant>]");
        }
        Ok(Self::new(parts[0], parts[1], parts.get(2).copied()))
    }

    /// The platform this machine runs, in OCI terms.
    pub fn host() -> Self {
        Self::new("linux", std::env::consts::ARCH, None)
    }

    /// A normalized platform (uname and Debian arch names accepted).
    pub fn new(os: &str, arch: &str, variant: Option<&str>) -> Self {
        let p = Self {
            os: os.into(),
            architecture: arch.into(),
            variant: variant.map(str::to_string),
            ..Default::default()
        };
        p.normalize()
    }

    /// containerd's normalization: canonical arch names, implied variants dropped, `arm` is `v7` by
    /// default. Unknown fields are dropped.
    pub fn normalize(&self) -> Self {
        let arch = self.architecture.to_ascii_lowercase();
        let variant = self.variant.as_deref().unwrap_or_default().to_ascii_lowercase();
        let (arch, variant) = match arch.as_str() {
            "x86_64" | "x86-64" | "amd64" => ("amd64", if variant == "v1" { "" } else { variant.as_str() }),
            "aarch64" | "arm64" => {
                ("arm64", if matches!(variant.as_str(), "8" | "v8" | "v8.0") { "" } else { &variant })
            }
            "armhf" | "armv7l" => ("arm", "v7"),
            "armel" | "armv6l" => ("arm", "v6"),
            "arm" => match variant.as_str() {
                "" | "7" => ("arm", "v7"),
                "5" => ("arm", "v5"),
                "6" => ("arm", "v6"),
                "8" => ("arm", "v8"),
                v => ("arm", v),
            },
            "i386" | "x86" => ("386", ""),
            a => (a, variant.as_str()),
        };
        Self {
            os: self.os.to_ascii_lowercase(),
            architecture: arch.into(),
            variant: (!variant.is_empty()).then(|| variant.into()),
            extra: BTreeMap::new(),
        }
    }

    /// This normalized platform, then the older variants it also runs, best first (`arm/v8` down to
    /// `v5`, `amd64/vN` down to `v1`), as containerd's `platforms.Only`.
    fn compatible(self) -> Vec<Self> {
        let floor = match self.architecture.as_str() {
            "arm" => 5,
            "amd64" => 1,
            _ => return vec![self],
        };
        let level = self.variant.as_deref().and_then(|v| v.strip_prefix('v')).and_then(|v| v.parse::<u32>().ok());
        match level {
            Some(n) if n > floor => {
                (floor..=n).rev().map(|n| Self::new(&self.os, &self.architecture, Some(&format!("v{n}")))).collect()
            }
            _ => vec![self],
        }
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.os, self.architecture)?;
        match &self.variant {
            Some(v) => write!(f, "/{v}"),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub config: Descriptor,
    #[serde(default)]
    pub layers: Vec<Descriptor>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Manifest {
    /// Sets the layers to their tar (digest = diff_id, the given sizes), with OCI media types.
    pub fn to_tar(&mut self, diffs: &[String], sizes: Vec<u64>) {
        for ((l, diff), size) in self.layers.iter_mut().zip(diffs).zip(sizes) {
            if is_layer(&l.media_type) {
                l.media_type = OCI_LAYER_TAR.into();
                l.digest = diff.clone();
            }
            l.size = size;
            if let Some(a) = l.annotations.as_mut() {
                a.retain(|k, _| !k.starts_with("containerd.io/"));
            }
        }
        if self.config.media_type == DOCKER_CONFIG {
            self.config.media_type = OCI_CONFIG.into();
        }
        self.media_type = Some(OCI_MANIFEST.into());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub manifests: Vec<Descriptor>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Index {
    /// The image manifest for `want`, else for the best platform it also runs; attestation
    /// manifests are skipped.
    pub fn select(&self, want: &Platform) -> Option<&Descriptor> {
        let images: Vec<(Platform, &Descriptor)> = self
            .manifests
            .iter()
            .filter(|d| !d.annotations.as_ref().is_some_and(|a| a.contains_key("vnd.docker.reference.type")))
            .filter_map(|d| Some((d.platform.as_ref()?.normalize(), d)))
            .collect();
        want.normalize().compatible().iter().find_map(|p| images.iter().find(|(q, _)| q == p).map(|(_, d)| *d))
    }
}

/// `rootfs.diff_ids` of an image config.
pub fn diff_ids(config: &[u8]) -> anyhow::Result<Vec<String>> {
    #[derive(Deserialize)]
    struct RootFs {
        #[serde(default)]
        diff_ids: Vec<String>,
    }
    #[derive(Deserialize)]
    struct Config {
        rootfs: RootFs,
    }
    Ok(serde_json::from_slice::<Config>(config)?.rootfs.diff_ids)
}

/// Chain IDs of a layer stack (the snapshot names containerd unpacks to).
pub fn chain_ids(diff_ids: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(diff_ids.len());
    for d in diff_ids {
        let next = match out.last() {
            None => d.clone(),
            Some(prev) => sha256(format!("{prev} {d}").as_bytes()),
        };
        out.push(next);
    }
    out
}

/// What a layer blob is, from its first bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

impl Compression {
    pub fn sniff(head: &[u8]) -> Self {
        match head {
            [0x1f, 0x8b, ..] => Self::Gzip,
            [0x28, 0xb5, 0x2f, 0xfd, ..] => Self::Zstd,
            _ => Self::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_ids_follow_containerd() {
        let a = format!("sha256:{}", "a".repeat(64));
        let b = format!("sha256:{}", "b".repeat(64));
        let ids = chain_ids(&[a.clone(), b.clone()]);
        assert_eq!(ids[0], a);
        assert_eq!(ids[1], sha256(format!("{a} {b}").as_bytes()));
    }

    #[test]
    fn unknown_fields_survive() {
        let raw = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"x","digest":"sha256:00","size":1,"data":"e30="},"layers":[],"annotations":{"k":"v"},"subject":{"mediaType":"m","digest":"d","size":2}}"#;
        let m: Manifest = serde_json::from_str(raw).unwrap();
        let back = serde_json::to_value(&m).unwrap();
        assert_eq!(back["annotations"]["k"], "v");
        assert_eq!(back["subject"]["size"], 2);
        assert_eq!(back["config"]["data"], "e30=");
    }

    #[test]
    fn platform_selection_skips_attestations() {
        let idx: Index = serde_json::from_value(serde_json::json!({
            "schemaVersion": 2,
            "manifests": [
                {"mediaType": OCI_MANIFEST, "digest": "sha256:att", "size": 1, "platform": {"os": "unknown", "architecture": "unknown"},
                 "annotations": {"vnd.docker.reference.type": "attestation-manifest"}},
                {"mediaType": OCI_MANIFEST, "digest": "sha256:arm", "size": 1, "platform": {"os": "linux", "architecture": "arm64"}},
            ]
        }))
        .unwrap();
        assert_eq!(idx.select(&Platform::parse("linux/arm64").unwrap()).unwrap().digest, "sha256:arm");
        assert!(idx.select(&Platform::parse("linux/amd64").unwrap()).is_none());
    }

    /// An index with one image per `(digest, os/arch[/variant])`.
    fn index(entries: &[(&str, &str)]) -> Index {
        let manifests: Vec<_> = entries
            .iter()
            .map(|(digest, p)| {
                let mut parts = p.split('/');
                let (os, arch, variant) = (parts.next().unwrap(), parts.next().unwrap(), parts.next());
                let mut platform = serde_json::json!({"os": os, "architecture": arch});
                if let Some(v) = variant {
                    platform["variant"] = v.into();
                }
                serde_json::json!({"mediaType": OCI_MANIFEST, "digest": digest, "size": 1, "platform": platform})
            })
            .collect();
        serde_json::from_value(serde_json::json!({"schemaVersion": 2, "manifests": manifests})).unwrap()
    }

    fn pick<'a>(idx: &'a Index, platform: &str) -> Option<&'a str> {
        idx.select(&Platform::parse(platform).unwrap()).map(|d| d.digest.as_str())
    }

    #[test]
    fn platform_selection_matches_the_arm_variant() {
        let idx = index(&[("v6", "linux/arm/v6"), ("v7", "linux/arm/v7")]);
        assert_eq!(pick(&idx, "linux/arm/v7"), Some("v7"));
        assert_eq!(pick(&idx, "linux/arm"), Some("v7"));
        assert_eq!(pick(&idx, "linux/arm/v6"), Some("v6"));
        assert_eq!(pick(&idx, "linux/arm/v8"), Some("v7"));
        assert_eq!(pick(&idx, "linux/arm/v5"), None);
        assert_eq!(pick(&index(&[("v6", "linux/arm/v6")]), "linux/arm/v7"), Some("v6"));
        assert_eq!(pick(&index(&[("bare", "linux/arm")]), "linux/arm/v7"), Some("bare"));
    }

    #[test]
    fn platform_selection_treats_arm64_v8_as_arm64() {
        let plain = index(&[("arm64", "linux/arm64")]);
        let v8 = index(&[("arm64", "linux/arm64/v8")]);
        for idx in [&plain, &v8] {
            assert_eq!(pick(idx, "linux/arm64"), Some("arm64"));
            assert_eq!(pick(idx, "linux/arm64/v8"), Some("arm64"));
            assert_eq!(pick(idx, "linux/aarch64"), Some("arm64"));
        }
        assert_eq!(pick(&index(&[("v7", "linux/arm/v7")]), "linux/arm64"), None);
    }

    #[test]
    fn platform_selection_falls_back_to_older_amd64_variants() {
        let idx = index(&[("v1", "linux/amd64"), ("v3", "linux/amd64/v3")]);
        assert_eq!(pick(&idx, "linux/amd64/v3"), Some("v3"));
        assert_eq!(pick(&idx, "linux/amd64/v4"), Some("v3"));
        assert_eq!(pick(&idx, "linux/amd64/v2"), Some("v1"));
        assert_eq!(pick(&idx, "linux/x86_64"), Some("v1"));
        assert_eq!(pick(&idx, "linux/amd64/v1"), Some("v1"));
        assert_eq!(pick(&index(&[("v3", "linux/amd64/v3")]), "linux/amd64"), None);
    }

    #[test]
    fn platform_selection_without_a_match() {
        let idx = index(&[("amd64", "linux/amd64"), ("arm64", "linux/arm64")]);
        assert_eq!(pick(&idx, "linux/riscv64"), None);
        assert_eq!(pick(&idx, "windows/amd64"), None);
    }

    #[test]
    fn platform_parsing() {
        assert_eq!(Platform::parse("linux/arm/v7").unwrap().to_string(), "linux/arm/v7");
        assert_eq!(Platform::parse("linux/armhf").unwrap().to_string(), "linux/arm/v7");
        assert_eq!(Platform::parse("linux/arm64/v8").unwrap().to_string(), "linux/arm64");
        assert_eq!(Platform::parse("Linux/X86_64").unwrap().to_string(), "linux/amd64");
        for bad in ["linux", "linux/", "/amd64", "linux/arm/", "linux/arm/v7/x"] {
            assert!(Platform::parse(bad).is_err(), "{bad}");
        }
    }
}
