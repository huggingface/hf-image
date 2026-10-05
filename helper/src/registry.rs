//! OCI distribution client for one repository, plus the Hugging Face registry's Xet endpoints (`_hf/*`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use bytes::Bytes;
use reqwest::{Method, RequestBuilder, Response, StatusCode, header};
use serde::{Deserialize, Serialize};

use crate::auth::Tokens;
use crate::oci::{MANIFEST_ACCEPT, sha256};
use crate::reference::ImageRef;

#[derive(Debug, Clone)]
pub struct BlobInfo {
    /// `None` when the registry sends no `Content-Length`.
    pub size: Option<u64>,
    pub xet_hash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CasToken {
    pub cas_url: String,
    pub access_token: String,
    pub exp: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub digest: String,
    pub xet_hash: String,
    pub size: u64,
}

pub struct Manifest {
    pub bytes: Bytes,
    pub media_type: String,
    pub digest: String,
}

pub struct Registry {
    /// No redirects: blob `GET`s are inspected before following.
    api: reqwest::Client,
    /// Follows redirects to signed URLs (drops credentials across hosts).
    download: reqwest::Client,
    base: String,
    pub repo: String,
    scope: String,
    tokens: Tokens,
    /// CAS tokens by write access.
    cas_tokens: tokio::sync::Mutex<std::collections::HashMap<bool, CasToken>>,
}

impl Registry {
    pub fn new(r: &ImageRef, hf_token: Option<String>, push: bool) -> anyhow::Result<Arc<Self>> {
        let ua = concat!("hf-image/", env!("CARGO_PKG_VERSION"));
        let api = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(ua)
            .connect_timeout(Duration::from_secs(15))
            .build()?;
        let download = reqwest::Client::builder().user_agent(ua).connect_timeout(Duration::from_secs(15)).build()?;
        let base = r.base_url();
        let actions = if push { "pull,push" } else { "pull" };
        Ok(Arc::new(Self {
            tokens: Tokens::new(api.clone(), base.clone(), hf_token),
            api,
            download,
            scope: format!("repository:{}:{actions}", r.repo),
            cas_tokens: Default::default(),
            repo: r.repo.clone(),
            base,
        }))
    }

    fn url(&self, suffix: &str) -> String {
        format!("{}/v2/{}{suffix}", self.base, self.repo)
    }

    /// Sends with the repo token; a 401 refreshes the token once.
    async fn send(&self, build: impl Fn(&reqwest::Client) -> RequestBuilder) -> anyhow::Result<Response> {
        for attempt in 0..2 {
            let mut req = build(&self.api);
            if let Some(t) = self.tokens.token(&self.scope).await? {
                req = req.bearer_auth(t);
            }
            let resp = req.send().await.context("registry request failed")?;
            if resp.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                self.tokens.invalidate(&self.scope).await;
                continue;
            }
            return Ok(resp);
        }
        unreachable!()
    }

    pub async fn manifest(&self, reference: &str) -> anyhow::Result<Option<Manifest>> {
        let url = self.url(&format!("/manifests/{reference}"));
        let resp = self.send(|c| c.get(&url).header(header::ACCEPT, MANIFEST_ACCEPT)).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = check(resp, "manifest fetch").await?;
        let media_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .to_string();
        let bytes = resp.bytes().await?;
        let digest = sha256(&bytes);
        if reference.starts_with("sha256:") && reference != digest {
            bail!("manifest {reference} has digest {digest}");
        }
        Ok(Some(Manifest { bytes, media_type, digest }))
    }

    pub async fn put_manifest(&self, reference: &str, bytes: &Bytes, media_type: &str) -> anyhow::Result<String> {
        let url = self.url(&format!("/manifests/{reference}"));
        let resp = self.send(|c| c.put(&url).header(header::CONTENT_TYPE, media_type).body(bytes.clone())).await?;
        check(resp, "manifest push").await?;
        Ok(sha256(bytes))
    }

    /// `HEAD` of a blob: its size and, on the Hugging Face registry, its Xet hash.
    pub async fn blob(&self, digest: &str) -> anyhow::Result<Option<BlobInfo>> {
        let url = self.url(&format!("/blobs/{digest}"));
        let resp = self.send(|c| c.head(&url)).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = check(resp, "blob lookup").await?;
        let h = resp.headers();
        let size = h.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
        let xet_hash = h.get("x-xet-hash").and_then(|v| v.to_str().ok()).map(str::to_string);
        Ok(Some(BlobInfo { size, xet_hash }))
    }

    /// `GET` of a blob, following the registry's redirect to a signed URL when it sends one.
    pub async fn download(&self, digest: &str) -> anyhow::Result<Response> {
        let url = self.url(&format!("/blobs/{digest}"));
        let resp = self.send(|c| c.get(&url)).await?;
        if !resp.status().is_redirection() {
            return check(resp, "blob download").await;
        }
        check(self.download.get(redirect(&url, &resp)?).send().await?, "blob download").await
    }

    /// A small blob (configs, attestations), checked against its digest.
    pub async fn fetch_blob(&self, digest: &str) -> anyhow::Result<Bytes> {
        let bytes = self.download(digest).await?.bytes().await?;
        if sha256(&bytes) != digest {
            bail!("blob {digest} does not match its digest");
        }
        Ok(bytes)
    }

    /// A CAS token for the repo, reused until two minutes before it expires.
    pub async fn xet_token(&self, write: bool) -> anyhow::Result<CasToken> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        if let Some(t) = self.cas_tokens.lock().await.get(&write)
            && t.exp > now + 120
        {
            return Ok(t.clone());
        }
        let url = self.url(&format!("/_hf/xet-token?op={}", if write { "write" } else { "read" }));
        let resp = self.send(|c| c.get(&url)).await?;
        if resp.status() == StatusCode::NOT_FOUND || resp.status() == StatusCode::BAD_REQUEST {
            bail!("this registry does not support Xet transfers (no _hf/xet-token endpoint)");
        }
        let t: CasToken = check(resp, "Xet token").await?.json().await?;
        self.cas_tokens.lock().await.insert(write, t.clone());
        Ok(t)
    }

    /// Registers blobs uploaded to the repo's Xet storage; returns which were new.
    pub async fn register(&self, blobs: &[Registration]) -> anyhow::Result<Vec<bool>> {
        let mut created = Vec::with_capacity(blobs.len());
        for chunk in blobs.chunks(500) {
            let url = self.url("/_hf/blobs");
            let body = serde_json::json!({ "blobs": chunk });
            let resp = self.send(|c| c.post(&url).json(&body)).await?;
            let v: serde_json::Value = check(resp, "blob registration").await?.json().await?;
            let items = v["blobs"].as_array().context("invalid registration response")?;
            created.extend(items.iter().map(|b| b["created"].as_bool().unwrap_or(false)));
        }
        Ok(created)
    }

    /// Where a blob `GET` redirects to (signed download URL).
    pub async fn blob_location(&self, digest: &str) -> anyhow::Result<String> {
        let url = self.url(&format!("/blobs/{digest}"));
        let resp = self.send(|c| c.get(&url)).await?;
        if !resp.status().is_redirection() {
            check(resp, "blob download").await?;
            bail!("blob {digest}: expected a redirect");
        }
        Ok(redirect(&url, &resp)?.to_string())
    }

    /// Raw request against the repository (the local gateway proxies manifests with it).
    pub async fn request(
        &self,
        method: Method,
        suffix: &str,
        headers: header::HeaderMap,
        body: Bytes,
    ) -> anyhow::Result<Response> {
        let url = self.url(suffix);
        self.send(|c| c.request(method.clone(), &url).headers(headers.clone()).body(body.clone())).await
    }
}

/// The absolute `Location` of a redirect from `url`.
fn redirect(url: &str, resp: &Response) -> anyhow::Result<reqwest::Url> {
    let loc = resp.headers().get(header::LOCATION).and_then(|v| v.to_str().ok()).context("redirect")?;
    Ok(reqwest::Url::parse(url)?.join(loc)?)
}

async fn check(resp: Response, what: &str) -> anyhow::Result<Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let message = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["errors"][0]["message"].as_str().map(str::to_string))
        .unwrap_or(body);
    bail!("{what}: {status}: {}", message.trim())
}
