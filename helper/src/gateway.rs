//! The build endpoint BuildKit pushes into, served by the helper on loopback of BuildKit's network:
//! blobs stream straight to Xet, in one upload session committed and registered at once right
//! before the manifest that needs them. A blob the registry already has answers `HEAD` with 200,
//! so the pusher never reads it.
//!
//! Every request carries Basic credentials made for this gateway, which the CLI hands BuildKit in a
//! temporary Docker config: they keep other processes of that network namespace out.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine as _;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tokio::sync::{Mutex, Notify};

use crate::registry::{Registration, Registry};
use crate::xet::{FileUpload, Uploader, Xet};

#[derive(Default)]
struct Stats {
    blobs_skipped: AtomicUsize,
    blobs_uploaded: AtomicUsize,
    bytes_new: AtomicU64,
}

/// Where a gateway listens, and the credentials it takes.
#[derive(Serialize, Deserialize)]
pub struct Endpoint {
    pub addr: SocketAddr,
    /// A Docker config's `auth` for `addr`: base64 of `hf-image:<random password>`.
    pub auth: String,
}

/// What a gateway received.
#[derive(Serialize, Deserialize)]
pub struct Summary {
    /// Manifests pushed: (reference, digest).
    pub pushed: Vec<(String, String)>,
    pub blobs_uploaded: usize,
    pub blobs_skipped: usize,
    pub bytes_new: u64,
}

struct Session {
    upload: Option<FileUpload>,
    hasher: Sha256,
    size: u64,
    started: std::time::Instant,
}

/// Blobs uploaded since the last manifest push.
#[derive(Default)]
struct Batch {
    uploader: Option<Uploader>,
    pending: HashMap<String, Registration>,
    /// File uploads started in `uploader` and neither finished nor abandoned.
    in_flight: usize,
}

type St = State<Arc<Gateway>>;

pub struct Gateway {
    registry: Arc<Registry>,
    xet: Arc<Xet>,
    /// [`Endpoint::auth`]: requests carry it as `Authorization: Basic <auth>`.
    auth: String,
    uploads: Mutex<HashMap<String, Arc<Mutex<Session>>>>,
    batch: Mutex<Batch>,
    idle: Notify,
    /// Manifests pushed: (reference, digest).
    pushed: Mutex<Vec<(String, String)>>,
    stats: Stats,
}

impl Gateway {
    pub fn new(registry: Arc<Registry>, xet: Arc<Xet>) -> Arc<Self> {
        let password = uuid::Uuid::new_v4().simple().to_string();
        let auth = base64::engine::general_purpose::STANDARD.encode(format!("hf-image:{password}"));
        Arc::new(Self {
            registry,
            xet,
            auth,
            uploads: Mutex::new(HashMap::new()),
            batch: Mutex::new(Batch::default()),
            idle: Notify::new(),
            pushed: Mutex::new(Vec::new()),
            stats: Stats::default(),
        })
    }

    /// Serves until the returned handle is aborted.
    pub async fn serve(self: &Arc<Self>, addr: SocketAddr) -> anyhow::Result<(Endpoint, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let router = Router::new()
            .route("/v2/", get(|| async { StatusCode::OK }))
            .route("/v2/{ns}/{name}/blobs/uploads/", axum::routing::post(start))
            .route("/v2/{ns}/{name}/blobs/uploads/{id}", axum::routing::patch(patch).put(put).get(status))
            .route("/v2/{ns}/{name}/blobs/{digest}", get(blob).head(blob))
            .route("/v2/{ns}/{name}/manifests/{reference}", get(manifest).head(manifest).put(put_manifest))
            .layer(axum::middleware::from_fn_with_state(self.clone(), authenticate))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(self.clone());
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok((Endpoint { addr: local, auth: self.auth.clone() }, join))
    }

    /// Whether `authorization` is `Basic <auth>`, compared in constant time.
    fn authorized(&self, authorization: &[u8]) -> bool {
        let Some((scheme, credentials)) = authorization.split_at_checked(6) else { return false };
        scheme.eq_ignore_ascii_case(b"basic ") && bool::from(credentials.ct_eq(self.auth.as_bytes()))
    }

    pub async fn summary(&self) -> Summary {
        Summary {
            pushed: self.pushed.lock().await.clone(),
            blobs_uploaded: self.stats.blobs_uploaded.load(Ordering::Relaxed),
            blobs_skipped: self.stats.blobs_skipped.load(Ordering::Relaxed),
            bytes_new: self.stats.bytes_new.load(Ordering::Relaxed),
        }
    }

    fn allowed(&self, ns: &str, name: &str) -> bool {
        format!("{ns}/{name}") == self.registry.repo
    }

    /// Starts a file in the current upload session (opening one if needed).
    async fn start_file(&self, size: Option<u64>) -> anyhow::Result<FileUpload> {
        let mut b = self.batch.lock().await;
        if b.uploader.is_none() {
            b.uploader = Some(self.xet.uploader().await?);
        }
        let file = b.uploader.as_ref().expect("opened").file("blob", size)?;
        b.in_flight += 1;
        Ok(file)
    }

    async fn end_file(&self, done: Option<Registration>) {
        let mut b = self.batch.lock().await;
        b.in_flight -= 1;
        if let Some(r) = done {
            b.pending.insert(r.digest.clone(), r);
        }
        self.idle.notify_waiters();
    }

    /// Drops a session's upload and frees its slot in the batch.
    async fn abandon(&self, session: &Arc<Mutex<Session>>) {
        self.uploads.lock().await.retain(|_, s| !Arc::ptr_eq(s, session));
        if session.lock().await.upload.take().is_some() {
            self.end_file(None).await;
        }
    }

    async fn pending_size(&self, digest: &str) -> Option<u64> {
        self.batch.lock().await.pending.get(digest).map(|r| r.size)
    }

    /// Commits the session and registers its blobs.
    async fn flush(&self) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        let (uploader, pending) = loop {
            let idle = self.idle.notified();
            {
                let mut b = self.batch.lock().await;
                if b.in_flight == 0 {
                    break (b.uploader.take(), std::mem::take(&mut b.pending));
                }
            }
            idle.await;
        };
        let Some(uploader) = uploader else { return Ok(()) };
        uploader.finalize().await?;
        let registrations: Vec<Registration> = pending.into_values().collect();
        if !registrations.is_empty() {
            self.registry.register(&registrations).await?;
        }
        tracing::info!(blobs = registrations.len(), secs = started.elapsed().as_secs_f64(), "flushed");
        Ok(())
    }
}

/// Answers 401 with a Basic challenge unless the request carries the gateway's credentials.
async fn authenticate(State(gw): St, request: Request, next: Next) -> Response {
    let authorization = request.headers().get(header::AUTHORIZATION).map(HeaderValue::as_bytes).unwrap_or_default();
    if gw.authorized(authorization) {
        return next.run(request).await;
    }
    let mut r = oci_error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "authentication required");
    r.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static(r#"Basic realm="hf-image""#));
    r
}

fn unknown_repo() -> Response {
    oci_error(StatusCode::NOT_FOUND, "NAME_UNKNOWN", "unknown repository")
}

fn oci_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({ "errors": [{ "code": code, "message": message }] });
    (status, [(header::CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

fn internal(e: anyhow::Error) -> Response {
    eprintln!("hf-image gateway: {e:#}");
    oci_error(StatusCode::INTERNAL_SERVER_ERROR, "UNKNOWN", &format!("{e:#}"))
}

async fn blob(State(gw): St, Path((ns, name, digest)): Path<(String, String, String)>, method: Method) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let exists = |size: u64| {
        let mut r = StatusCode::OK.into_response();
        let h = r.headers_mut();
        h.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
        h.insert("docker-content-digest", HeaderValue::from_str(&digest).unwrap_or(HeaderValue::from_static("")));
        r
    };
    if method == Method::HEAD
        && let Some(size) = gw.pending_size(&digest).await
    {
        return exists(size);
    }
    match gw.registry.blob(&digest).await {
        Ok(Some(info)) if method == Method::HEAD => {
            gw.stats.blobs_skipped.fetch_add(1, Ordering::Relaxed);
            exists(info.size.unwrap_or_default())
        }
        Ok(Some(_)) => match gw.registry.blob_location(&digest).await {
            Ok(loc) => (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, loc)]).into_response(),
            Err(e) => internal(e),
        },
        Ok(None) => oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob unknown"),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct StartQuery {
    digest: Option<String>,
    mount: Option<String>,
}

async fn start(
    State(gw): St,
    Path((ns, name)): Path<(String, String)>,
    Query(q): Query<StartQuery>,
    body: Body,
) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    if let Some(d) = &q.mount
        && matches!(gw.registry.blob(d).await, Ok(Some(_)))
    {
        return created(&ns, &name, d);
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let session = Arc::new(Mutex::new(Session {
        upload: None,
        hasher: Sha256::new(),
        size: 0,
        started: std::time::Instant::now(),
    }));
    if let Some(d) = q.digest {
        return match feed(&gw, &session, body).await {
            Ok(_) => finish(&gw, &ns, &name, session, &d).await,
            Err(e) => internal(e),
        };
    }
    gw.uploads.lock().await.insert(id.clone(), session);
    accepted(&ns, &name, &id, 0)
}

fn accepted(ns: &str, name: &str, id: &str, offset: u64) -> Response {
    let loc = format!("/v2/{ns}/{name}/blobs/uploads/{id}");
    let range = format!("0-{}", offset.saturating_sub(1));
    (
        StatusCode::ACCEPTED,
        [
            (header::LOCATION, loc),
            (header::RANGE, range),
            ("docker-upload-uuid".parse().expect("static"), id.to_string()),
        ],
    )
        .into_response()
}

fn created(ns: &str, name: &str, digest: &str) -> Response {
    let loc = format!("/v2/{ns}/{name}/blobs/{digest}");
    (
        StatusCode::CREATED,
        [(header::LOCATION, loc), ("docker-content-digest".parse().expect("static"), digest.to_string())],
    )
        .into_response()
}

/// Appends a request body to a session, starting its Xet upload on the first bytes. A body that
/// fails or stops early (the client went away) abandons the session.
async fn feed(gw: &Arc<Gateway>, session: &Arc<Mutex<Session>>, body: Body) -> anyhow::Result<u64> {
    let mut feeding = Feeding { gw: gw.clone(), session: session.clone(), done: false };
    let mut s = session.lock().await;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if s.upload.is_none() {
            s.upload = Some(gw.start_file(None).await?);
        }
        s.hasher.update(&chunk);
        s.size += chunk.len() as u64;
        s.upload.as_mut().expect("started").write(chunk).await?;
    }
    feeding.done = true;
    Ok(s.size)
}

/// Abandons the session when dropped before the body was fed to its end.
struct Feeding {
    gw: Arc<Gateway>,
    session: Arc<Mutex<Session>>,
    done: bool,
}

impl Drop for Feeding {
    fn drop(&mut self) {
        if !self.done {
            let (gw, session) = (self.gw.clone(), self.session.clone());
            tokio::spawn(async move { gw.abandon(&session).await });
        }
    }
}

async fn session(gw: &Gateway, id: &str) -> Option<Arc<Mutex<Session>>> {
    gw.uploads.lock().await.get(id).cloned()
}

async fn patch(State(gw): St, Path((ns, name, id)): Path<(String, String, String)>, body: Body) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let Some(session) = session(&gw, &id).await else {
        return oci_error(StatusCode::NOT_FOUND, "BLOB_UPLOAD_UNKNOWN", "upload unknown");
    };
    match feed(&gw, &session, body).await {
        Ok(offset) => accepted(&ns, &name, &id, offset),
        Err(e) => internal(e),
    }
}

async fn status(State(gw): St, Path((ns, name, id)): Path<(String, String, String)>) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let Some(session) = session(&gw, &id).await else {
        return oci_error(StatusCode::NOT_FOUND, "BLOB_UPLOAD_UNKNOWN", "upload unknown");
    };
    let offset = session.lock().await.size;
    let mut r = accepted(&ns, &name, &id, offset);
    *r.status_mut() = StatusCode::NO_CONTENT;
    r
}

#[derive(Deserialize)]
struct DigestQuery {
    digest: String,
}

async fn put(
    State(gw): St,
    Path((ns, name, id)): Path<(String, String, String)>,
    Query(q): Query<DigestQuery>,
    body: Body,
) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let Some(session) = gw.uploads.lock().await.remove(&id) else {
        return oci_error(StatusCode::NOT_FOUND, "BLOB_UPLOAD_UNKNOWN", "upload unknown");
    };
    if let Err(e) = feed(&gw, &session, body).await {
        return internal(e);
    }
    finish(&gw, &ns, &name, session, &q.digest).await
}

/// Checks the digest and ends the file upload; the blob is registered with the next manifest.
async fn finish(gw: &Gateway, ns: &str, name: &str, session: Arc<Mutex<Session>>, digest: &str) -> Response {
    let mut s = session.lock().await;
    let file = match s.upload.take() {
        Some(f) => Ok(f),
        None => gw.start_file(Some(0)).await,
    };
    let file = match file {
        Ok(f) => f,
        Err(e) => return internal(e),
    };
    let got = format!("sha256:{}", hex::encode(std::mem::take(&mut s.hasher).finalize()));
    if got != digest {
        drop(file);
        gw.end_file(None).await;
        return oci_error(StatusCode::BAD_REQUEST, "DIGEST_INVALID", &format!("uploaded content is {got}"));
    }
    match file.finish().await {
        Ok(done) => {
            gw.stats.bytes_new.fetch_add(done.new_bytes, Ordering::Relaxed);
            tracing::info!(
                digest,
                size = s.size,
                new = done.new_bytes,
                secs = s.started.elapsed().as_secs_f64(),
                "blob received"
            );
            let r = Registration { digest: digest.into(), xet_hash: done.hash, size: s.size };
            gw.end_file(Some(r)).await;
            gw.stats.blobs_uploaded.fetch_add(1, Ordering::Relaxed);
            created(ns, name, digest)
        }
        Err(e) => {
            gw.end_file(None).await;
            internal(e)
        }
    }
}

async fn manifest(
    State(gw): St,
    Path((ns, name, reference)): Path<(String, String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let mut fwd = HeaderMap::new();
    if let Some(a) = headers.get(header::ACCEPT) {
        fwd.insert(header::ACCEPT, a.clone());
    }
    match gw.registry.request(method, &format!("/manifests/{reference}"), fwd, Bytes::new()).await {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut out = HeaderMap::new();
            for k in [header::CONTENT_TYPE, header::CONTENT_LENGTH] {
                if let Some(v) = resp.headers().get(k.as_str()).and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok())
                {
                    out.insert(k, v);
                }
            }
            if let Some(v) =
                resp.headers().get("docker-content-digest").and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok())
            {
                out.insert("docker-content-digest", v);
            }
            let body = resp.bytes().await.unwrap_or_default();
            (status, out, body).into_response()
        }
        Err(e) => internal(e),
    }
}

async fn put_manifest(
    State(gw): St,
    Path((ns, name, reference)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !gw.allowed(&ns, &name) {
        return unknown_repo();
    }
    let media_type =
        headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or(crate::oci::OCI_MANIFEST);
    let pushed = async {
        gw.flush().await?;
        gw.registry.put_manifest(&reference, &body, media_type).await
    };
    match pushed.await {
        Ok(digest) => {
            gw.pushed.lock().await.push((reference, digest.clone()));
            let loc = format!("/v2/{ns}/{name}/manifests/{digest}");
            (StatusCode::CREATED, [(header::LOCATION, loc), ("docker-content-digest".parse().expect("static"), digest)])
                .into_response()
        }
        Err(e) => internal(e),
    }
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::STANDARD;

    use super::*;
    use crate::reference::ImageRef;

    /// A gateway for `acme/app` on an anonymous loopback registry whose manifests read "manifest".
    async fn gateway() -> (String, Endpoint) {
        let upstream = Router::new()
            .route("/v2/", get(|| async { StatusCode::OK }))
            .route("/v2/{ns}/{name}/manifests/{reference}", get(|| async { "manifest" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let registry = Registry::new(&ImageRef::parse(&format!("{addr}/acme/app")).unwrap(), None, true).unwrap();
        let gw = Gateway::new(registry.clone(), Xet::new(registry).unwrap());
        let (endpoint, _) = gw.serve(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
        (format!("http://{}", endpoint.addr), endpoint)
    }

    #[tokio::test]
    async fn requests_need_the_gateway_credentials() {
        let (base, endpoint) = gateway().await;
        let password = String::from_utf8(STANDARD.decode(&endpoint.auth).unwrap()).unwrap();
        let password = password.strip_prefix("hf-image:").unwrap();
        assert_eq!(password.len(), 32);
        let http = reqwest::Client::new();
        let get = |path: &str, authorization: Option<String>| {
            let request = http.get(format!("{base}{path}"));
            match authorization {
                Some(a) => request.header(header::AUTHORIZATION, a).send(),
                None => request.send(),
            }
        };
        let wrong = [
            format!("Basic {}", STANDARD.encode("hf-image:not-the-password")),
            format!("Basic {}x", endpoint.auth),
            format!("Bearer {}", endpoint.auth),
            endpoint.auth.clone(),
        ];
        for path in ["/v2/", "/v2/acme/app/manifests/v1", &format!("/v2/{password}/acme/app/manifests/v1")] {
            let refused = get(path, None).await.unwrap();
            assert_eq!(refused.status(), StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(refused.headers()[header::WWW_AUTHENTICATE], r#"Basic realm="hf-image""#);
            for w in &wrong {
                assert_eq!(get(path, Some(w.clone())).await.unwrap().status(), StatusCode::UNAUTHORIZED, "{path} {w}");
            }
        }

        let basic = |scheme: &str| Some(format!("{scheme} {}", endpoint.auth));
        assert_eq!(get("/v2/", basic("Basic")).await.unwrap().status(), StatusCode::OK);
        assert_eq!(get("/v2/", basic("basic")).await.unwrap().status(), StatusCode::OK);
        let manifest = get("/v2/acme/app/manifests/v1", basic("Basic")).await.unwrap();
        assert_eq!(manifest.text().await.unwrap(), "manifest");
        let other = get("/v2/acme/other/manifests/v1", basic("Basic")).await.unwrap();
        assert_eq!(other.status(), StatusCode::NOT_FOUND);
    }
}
