//! Minimal Basic-auth HTTP client for the oracle.
//!
//! The oracle does not link the generated SDK; it speaks the wire protocol
//! directly with `reqwest`. This avoids dragging the JS SDK build into Rust
//! testing and keeps the oracle a black-box observer of Bun.
//!
//! Auth follows [`CONTRACT.md`](../CONTRACT.md):
//! - `Authorization: Basic base64(username:password)` whenever
//!   `KILO_SERVER_PASSWORD` is set.
//! - `username` defaults to `kilo` (matches Bun's middleware seam).
//!
//! Directory scoping: the SDK sets `x-kilo-directory` on the client and lets
//! `client.ts` rewrite it into a `directory` query parameter on GET/HEAD. We
//! mirror that exactly: the directory is provided once at construction time
//! and the client appends it as a query parameter on every request.

use std::time::Duration;

use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Method, RequestBuilder, Response, Url};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{OracleError, OracleResult};

/// Reusable HTTP client targeting one Bun sidecar instance.
#[derive(Debug, Clone)]
pub struct OracleClient {
    inner: reqwest::Client,
    base: Url,
    directory: Option<String>,
    auth_header: Option<HeaderValue>,
}

impl OracleClient {
    /// Build a client targeting `http://<host>:<port>` with the given
    /// directory scope and Basic-auth password. `username` defaults to
    /// `kilo` when `None`.
    pub fn new(
        host: &str,
        port: u16,
        directory: Option<String>,
        username: Option<&str>,
        password: Option<&str>,
    ) -> OracleResult<Self> {
        let base = Url::parse(&format!("http://{host}:{port}/"))
            .map_err(|e| OracleError::Other(format!("invalid sidecar base url: {e}")))?;

        let auth_header = match password {
            Some(pw) => {
                let user = username.unwrap_or("kilo");
                let token =
                    base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pw}"));
                let value = HeaderValue::from_str(&format!("Basic {token}"))
                    .map_err(|e| OracleError::Other(format!("auth header: {e}")))?;
                Some(value)
            }
            None => None,
        };

        let inner = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            // SSE traces can be many seconds long; rely on stop conditions.
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(OracleError::Http)?;

        Ok(Self {
            inner,
            base,
            directory,
            auth_header,
        })
    }

    /// Convenience: build with no directory scope (used for `/global/*`).
    pub fn unscoped(host: &str, port: u16, password: Option<&str>) -> OracleResult<Self> {
        Self::new(host, port, None, None, password)
    }

    pub fn base(&self) -> &Url {
        &self.base
    }

    /// Internal: build a `RequestBuilder` with auth + directory query if
    /// applicable.
    fn request(&self, method: Method, path: &str) -> OracleResult<RequestBuilder> {
        // Strip a leading slash because `Url::join` treats absolute paths as
        // root-anchored, which would clobber the base path on non-default
        // bases (we only have `/` today, but stay defensive).
        let path = path.trim_start_matches('/');
        let mut url = self
            .base
            .join(path)
            .map_err(|e| OracleError::Other(format!("invalid path {path}: {e}")))?;

        if let Some(dir) = &self.directory {
            // Only attach for GET/HEAD; POST/PATCH/DELETE put it in the body.
            if matches!(method, Method::GET | Method::HEAD) {
                if !url.query_pairs().any(|(k, _)| k == "directory") {
                    url.query_pairs_mut().append_pair("directory", dir);
                }
            }
        }

        let mut headers = HeaderMap::new();
        if let Some(auth) = &self.auth_header {
            headers.insert(AUTHORIZATION, auth.clone());
        }
        // L1: only attach `Content-Type: application/json` to methods that
        // actually carry a body. GET / HEAD / DELETE without a body should
        // not advertise a body media type — some intermediaries (and Bun's
        // own middleware seam) treat that as a malformed request.
        let has_body = !matches!(method, Method::GET | Method::HEAD | Method::DELETE);
        if has_body {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        Ok(self.inner.request(method, url).headers(headers))
    }

    /// `GET /global/health`. Returns the parsed JSON body.
    pub async fn global_health(&self) -> OracleResult<serde_json::Value> {
        self.get_json("/global/health").await
    }

    /// `GET <path>` returning JSON.
    pub async fn get_json(&self, path: &str) -> OracleResult<serde_json::Value> {
        let resp = self.request(Method::GET, path)?.send().await?;
        let resp = check_status(resp, "GET", path).await?;
        let bytes = resp.bytes().await.map_err(OracleError::Http)?;
        if bytes.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// `POST <path>` with optional JSON body, returning JSON.
    pub async fn post_json<B: Serialize>(
        &self,
        path: &str,
        body: Option<&B>,
    ) -> OracleResult<serde_json::Value> {
        let mut req = self.request(Method::POST, path)?;
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let resp = check_status(resp, "POST", path).await?;
        let bytes = resp.bytes().await.map_err(OracleError::Http)?;
        if bytes.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// `PATCH <path>` with JSON body, returning JSON.
    pub async fn patch_json<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> OracleResult<serde_json::Value> {
        let req = self.request(Method::PATCH, path)?.json(body);
        let resp = req.send().await?;
        let resp = check_status(resp, "PATCH", path).await?;
        let bytes = resp.bytes().await.map_err(OracleError::Http)?;
        if bytes.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// `DELETE <path>`, returning JSON (often null).
    pub async fn delete_json(&self, path: &str) -> OracleResult<serde_json::Value> {
        let resp = self.request(Method::DELETE, path)?.send().await?;
        let resp = check_status(resp, "DELETE", path).await?;
        let bytes = resp.bytes().await.map_err(OracleError::Http)?;
        if bytes.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Open `GET /global/event` as a streaming response. The caller is
    /// responsible for consuming the stream.
    ///
    /// Sets `Accept: text/event-stream` (L2): Bun routes by Accept header
    /// when both SSE and JSON encodings would be valid for the same path,
    /// and the extension's SSE adapter sets the same Accept header.
    pub async fn open_global_event_stream(&self) -> OracleResult<Response> {
        let resp = self
            .request(Method::GET, "/global/event")?
            .header(ACCEPT, HeaderValue::from_static("text/event-stream"))
            .send()
            .await?;
        check_status(resp, "GET", "/global/event").await
    }

    /// Generic JSON-typed GET. Convenience wrapper around [`Self::get_json`].
    pub async fn get_typed<T: DeserializeOwned>(&self, path: &str) -> OracleResult<T> {
        let v = self.get_json(path).await?;
        Ok(serde_json::from_value(v)?)
    }
}

async fn check_status(resp: Response, method: &str, path: &str) -> OracleResult<Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Err(OracleError::HttpStatus {
        method: method.to_string(),
        path: path.to_string(),
        status,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_header_uses_default_username() {
        // Indirect smoke test: build a client and look at the first byte of
        // the encoded credentials.
        let client = OracleClient::new("127.0.0.1", 9999, None, None, Some("hunter2")).unwrap();
        let header = client
            .auth_header
            .as_ref()
            .expect("password should produce an auth header");
        let s = header.to_str().unwrap();
        assert!(s.starts_with("Basic "));
        let token = &s[6..];
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(token)
            .unwrap();
        assert_eq!(decoded, b"kilo:hunter2");
    }

    #[test]
    fn no_password_no_auth_header() {
        let client = OracleClient::new("127.0.0.1", 9999, None, None, None).unwrap();
        assert!(client.auth_header.is_none());
    }
}
