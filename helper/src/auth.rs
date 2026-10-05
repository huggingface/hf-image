//! The registry's bearer-token handshake, with the HF token the CLI hands over on stdin.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct Challenge {
    realm: String,
    service: String,
}

/// Registry tokens for one registry, cached per scope until shortly before expiry.
pub struct Tokens {
    http: reqwest::Client,
    base: String,
    hf_token: Option<String>,
    /// The registry's bearer challenge once probed: `None` when `/v2/` needs no token.
    challenge: Mutex<Option<Option<Challenge>>>,
    cache: Mutex<HashMap<String, (String, Instant)>>,
}

impl Tokens {
    pub fn new(http: reqwest::Client, base: String, hf_token: Option<String>) -> Self {
        Self { http, base, hf_token, challenge: Mutex::new(None), cache: Mutex::new(HashMap::new()) }
    }

    pub async fn invalidate(&self, scope: &str) {
        self.cache.lock().await.remove(scope);
    }

    /// A registry token for `scope` (`repository:<name>:pull,push`); `None` when the registry does
    /// not ask for one.
    pub async fn token(&self, scope: &str) -> anyhow::Result<Option<String>> {
        if let Some((t, until)) = self.cache.lock().await.get(scope)
            && *until > Instant::now()
        {
            return Ok(Some(t.clone()));
        }
        let Some(ch) = self.challenge().await? else { return Ok(None) };
        let mut req = self.http.get(&ch.realm).query(&[("service", ch.service.as_str()), ("scope", scope)]);
        if let Some(t) = &self.hf_token {
            req = req.basic_auth("hf", Some(t));
        }
        let resp = req.send().await.context("token request failed")?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            bail!("the Hugging Face token was refused (run `hf auth login` or set HF_TOKEN)");
        }
        if !resp.status().is_success() {
            bail!("token request answered {}", resp.status());
        }
        #[derive(Deserialize)]
        struct Issued {
            #[serde(default)]
            token: Option<String>,
            #[serde(default)]
            access_token: Option<String>,
            #[serde(default)]
            expires_in: Option<u64>,
        }
        let issued: Issued = resp.json().await.context("invalid token response")?;
        let token = issued.token.or(issued.access_token).context("token response without a token")?;
        let ttl = Duration::from_secs(issued.expires_in.unwrap_or(300).saturating_sub(30).max(10));
        self.cache.lock().await.insert(scope.to_string(), (token.clone(), Instant::now() + ttl));
        Ok(Some(token))
    }

    async fn challenge(&self) -> anyhow::Result<Option<Challenge>> {
        let mut cached = self.challenge.lock().await;
        if let Some(c) = &*cached {
            return Ok(c.clone());
        }
        let resp = self.http.get(format!("{}/v2/", self.base)).send().await.context("registry unreachable")?;
        let ch = if resp.status() == StatusCode::UNAUTHORIZED {
            let header = resp
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .context("401 without a WWW-Authenticate challenge")?;
            Some(parse_challenge(header).context("unsupported WWW-Authenticate challenge")?)
        } else {
            None
        };
        *cached = Some(ch.clone());
        Ok(ch)
    }
}

fn parse_challenge(header: &str) -> Option<Challenge> {
    let params = header.strip_prefix("Bearer ").or_else(|| header.strip_prefix("bearer "))?;
    let mut realm = None;
    let mut service = String::new();
    for part in split_params(params) {
        let (k, v) = part.split_once('=')?;
        let v = v.trim().trim_matches('"').to_string();
        match k.trim() {
            "realm" => realm = Some(v),
            "service" => service = v,
            _ => {}
        }
    }
    Some(Challenge { realm: realm?, service })
}

/// Splits `a="x,y",b="z"` on commas outside quotes.
fn split_params(s: &str) -> Vec<&str> {
    let (mut out, mut start, mut quoted) = (Vec::new(), 0, false);
    for (i, c) in s.char_indices() {
        match c {
            '"' => quoted = !quoted,
            ',' if !quoted => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenges() {
        let c = parse_challenge(
            r#"Bearer realm="https://huggingface.co/api/registry/token",service="container_registry",scope="repository:a/b:pull,push""#,
        )
        .unwrap();
        assert_eq!(c.realm, "https://huggingface.co/api/registry/token");
        assert_eq!(c.service, "container_registry");
        assert!(parse_challenge("Basic realm=x").is_none());
    }
}
