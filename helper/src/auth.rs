//! The registry's bearer-token handshake, with the HF token the CLI hands over on stdin.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use tokio::sync::Mutex;

/// The registry that gets the HF token without being listed in `HF_IMAGE_TOKEN_REGISTRIES`.
const HF_REGISTRY: &str = "https://cr.hf.co";
/// The only realm `HF_REGISTRY` gets the HF token sent to.
const HF_REALM: &str = "https://huggingface.co/api/registry/token";

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
    /// Listed in `HF_IMAGE_TOKEN_REGISTRIES`: the HF token goes to its realm.
    listed: bool,
    /// The registry's bearer challenge once probed: `None` when `/v2/` needs no token.
    challenge: Mutex<Option<Option<Challenge>>>,
    cache: Mutex<HashMap<String, (String, Instant)>>,
}

impl Tokens {
    pub fn new(http: reqwest::Client, base: String, hf_token: Option<String>, listed: bool) -> Self {
        Self { http, base, hf_token, listed, challenge: Mutex::new(None), cache: Mutex::new(HashMap::new()) }
    }

    /// Why the HF token must not go to `realm`; `None` if it may: cr.hf.co's only to the Hub's
    /// realm, a listed registry's to an https realm (any realm when the registry is plain http).
    fn withheld(&self, realm: &str) -> Option<String> {
        if self.base == HF_REGISTRY {
            return (realm != HF_REALM).then(|| format!("{realm} is not the Hub's token endpoint"));
        }
        if !self.listed {
            return Some("it goes only to cr.hf.co and the registries in HF_IMAGE_TOKEN_REGISTRIES".into());
        }
        let https = Url::parse(realm).is_ok_and(|u| u.scheme() == "https");
        (!https && !self.base.starts_with("http://")).then(|| format!("{realm} is not https"))
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
        let withheld = self.hf_token.as_ref().and_then(|_| self.withheld(&ch.realm));
        if let Some(t) = &self.hf_token
            && withheld.is_none()
        {
            req = req.basic_auth("hf", Some(t));
        }
        let resp = req.send().await.context("token request failed")?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            if let Some(why) = withheld {
                bail!("the registry needs a token; the Hugging Face token was not sent: {why}");
            }
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
    use std::sync::{Arc, Mutex as SyncMutex};

    use axum::Router;
    use axum::http::HeaderMap;
    use axum::http::header::{AUTHORIZATION, LOCATION, WWW_AUTHENTICATE};
    use axum::response::IntoResponse;
    use axum::routing::get;

    use super::*;
    use crate::registry::Registry;

    fn tokens(base: &str, listed: bool) -> Tokens {
        Tokens::new(reqwest::Client::new(), base.into(), Some("hf_secret".into()), listed)
    }

    #[test]
    fn cr_hf_co_sends_the_token_only_to_the_hub_realm() {
        for listed in [false, true] {
            let cr = tokens(HF_REGISTRY, listed);
            assert_eq!(cr.withheld(HF_REALM), None);
            for realm in [
                "https://registry.example/token",
                "http://huggingface.co/api/registry/token",
                "https://huggingface.co:8443/api/registry/token",
                "https://huggingface.co.registry.example/api/registry/token",
                "https://huggingface.co@registry.example/api/registry/token",
                "https://huggingface.co/api/registry/token/x",
            ] {
                assert!(cr.withheld(realm).is_some(), "{realm}");
            }
        }
    }

    #[test]
    fn other_registries_get_the_token_only_when_listed() {
        let unlisted =
            ["https://registry-1.docker.io", "http://cr.hf.co", "https://cr.hf.co:443", "http://127.0.0.1:5000"];
        for base in unlisted {
            assert!(tokens(base, false).withheld(HF_REALM).is_some(), "{base}");
        }
        let listed = tokens("https://registry.example", true);
        assert_eq!(listed.withheld("https://auth.example/token"), None);
        assert!(listed.withheld("http://auth.example/token").is_some());
        assert!(listed.withheld("auth.example/token").is_some());
        assert_eq!(tokens("http://localhost:5000", true).withheld("http://localhost:8080/token"), None);
    }

    async fn listen(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    type Seen = Arc<SyncMutex<Vec<Option<String>>>>;

    /// A loopback server answering `status` (to `location`, if any) with a token; its URL and the
    /// `Authorization` of each request it got.
    async fn endpoint(status: StatusCode, location: Option<String>) -> (String, Seen) {
        let seen = Seen::default();
        let record = seen.clone();
        let answer = move |headers: HeaderMap| async move {
            record.lock().unwrap().push(headers.get(AUTHORIZATION).map(|v| v.to_str().unwrap().to_string()));
            let mut resp = (status, r#"{"token":"registry-token"}"#).into_response();
            if let Some(l) = location {
                resp.headers_mut().insert(LOCATION, l.parse().unwrap());
            }
            resp
        };
        (listen(Router::new().fallback(answer)).await, seen)
    }

    /// One token request, with the registry's own client, to a loopback registry naming `realm`.
    async fn token_request(realm: &str, listed: bool) -> anyhow::Result<Option<String>> {
        let challenge = format!(r#"Bearer realm="{realm}/token",service="registry""#);
        let probe = move || async move { (StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, challenge)]) };
        let registry = listen(Router::new().route("/v2/", get(probe))).await;
        let tokens = Tokens::new(Registry::api_client().unwrap(), registry, Some("hf_secret".into()), listed);
        tokens.token("repository:a/b:pull").await
    }

    #[tokio::test]
    async fn an_unlisted_registry_gets_no_token() {
        let (realm, seen) = endpoint(StatusCode::OK, None).await;
        assert_eq!(token_request(&realm, false).await.unwrap().as_deref(), Some("registry-token"));
        assert_eq!(*seen.lock().unwrap(), [None]);

        let (realm, seen) = endpoint(StatusCode::UNAUTHORIZED, None).await;
        assert_eq!(
            token_request(&realm, false).await.unwrap_err().to_string(),
            "the registry needs a token; the Hugging Face token was not sent: \
             it goes only to cr.hf.co and the registries in HF_IMAGE_TOKEN_REGISTRIES"
        );
        assert_eq!(*seen.lock().unwrap(), [None]);
    }

    #[tokio::test]
    async fn a_listed_plain_http_registry_gets_the_token() {
        let (realm, seen) = endpoint(StatusCode::OK, None).await;
        assert_eq!(token_request(&realm, true).await.unwrap().as_deref(), Some("registry-token"));
        assert_eq!(*seen.lock().unwrap(), [Some("Basic aGY6aGZfc2VjcmV0".to_string())]);
    }

    #[tokio::test]
    async fn the_token_does_not_follow_a_redirect_from_the_realm() {
        let (elsewhere, seen) = endpoint(StatusCode::OK, None).await;
        let (realm, _) = endpoint(StatusCode::TEMPORARY_REDIRECT, Some(format!("{elsewhere}/token"))).await;
        let err = token_request(&realm, true).await.unwrap_err();
        assert_eq!(err.to_string(), "token request answered 307 Temporary Redirect");
        assert!(seen.lock().unwrap().is_empty());
    }

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
