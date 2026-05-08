//! Axum middleware: header→query rewrite (so non-SDK callers can target
//! directory-scoped routes by header) and HTTP basic auth.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose, Engine};

use crate::util::encoding::decode_query;
use crate::AppState;

/// Rewrite `x-kilo-directory` and `x-kilo-workspace` request headers into
/// `?directory=` / `?workspace=` query parameters for GET/HEAD requests, so
/// non-SDK callers (e.g. the oracle harness, curl scripts) can target a
/// directory-scoped route by header — matching the behavior of the SDK at
/// `packages/sdk/js/src/v2/client.ts`. Existing query values take precedence.
pub(crate) async fn directory_header_rewrite(mut req: Request, next: Next) -> Response {
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return next.run(req).await;
    }

    let dir = req
        .headers()
        .get("x-kilo-directory")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let ws = req
        .headers()
        .get("x-kilo-workspace")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    if dir.is_none() && ws.is_none() {
        return next.run(req).await;
    }

    let uri = req.uri().clone();
    let new_path_and_query =
        rewrite_path_and_query(uri.path(), uri.query(), dir.as_deref(), ws.as_deref());

    let mut builder = Uri::builder().path_and_query(new_path_and_query);
    if let Some(scheme) = uri.scheme_str() {
        builder = builder.scheme(scheme);
    }
    if let Some(authority) = uri.authority() {
        builder = builder.authority(authority.as_str());
    }
    if let Ok(new_uri) = builder.build() {
        *req.uri_mut() = new_uri;
    }

    req.headers_mut().remove("x-kilo-directory");
    req.headers_mut().remove("x-kilo-workspace");

    next.run(req).await
}

/// Pure helper: given a path, an existing query string, and any
/// directory/workspace values pulled from headers, return the rewritten
/// `path[?query]`. Existing query keys win (matches the SDK's
/// `!url.searchParams.has(key)` guard in `packages/sdk/js/src/v2/client.ts`).
pub(crate) fn rewrite_path_and_query(
    path: &str,
    query: Option<&str>,
    directory: Option<&str>,
    workspace: Option<&str>,
) -> String {
    let mut params: Vec<(String, String)> = query
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();

    let has = |key: &str, params: &[(String, String)]| params.iter().any(|(k, _)| k == key);
    if let Some(value) = directory {
        if !has("directory", &params) {
            params.push(("directory".to_string(), value.to_string()));
        }
    }
    if let Some(value) = workspace {
        if !has("workspace", &params) {
            params.push(("workspace".to_string(), value.to_string()));
        }
    }

    let new_query = params
        .into_iter()
        .map(|(k, v)| if v.is_empty() { k } else { format!("{k}={v}") })
        .collect::<Vec<_>>()
        .join("&");

    if new_query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{new_query}")
    }
}

/// Mirror Hono's `c.req.valid("json") ?? {}` request tolerance for body-bearing
/// methods. Hey-api's generated SDK (`packages/sdk/js/src/v2/gen/client/client.gen.ts:58-61`)
/// deletes `Content-Type` on POST/PUT/PATCH when the body is empty, so a no-arg
/// SDK call like `client.session.create({ directory })` lands at the server with
/// no header AND a zero-length body. Bun's Hono accepts that and the route's
/// zod validator returns `{}`; axum's strict `Json<T>` extractor rejects with
/// 415 + `"Expected request with `Content-Type: application/json`"`.
///
/// This middleware fills the Bun-compat gap for exactly that pattern: if the
/// request is a body-bearing method, has no `Content-Type`, and has an empty
/// body, inject `Content-Type: application/json` plus a literal `{}` payload so
/// the downstream `Json<T>` extractor deserializes a default value. Any request
/// with an explicit `Content-Type` passes through untouched. Any request without
/// `Content-Type` is buffered up to a tiny cap so real JSON payloads can still
/// be forwarded intact with the missing header filled in, while larger/streamed
/// bodies pass through untouched.
pub(crate) async fn json_body_lenient(req: Request, next: Next) -> Response {
    if !matches!(
        *req.method(),
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    ) {
        return next.run(req).await;
    }

    if req.headers().contains_key(header::CONTENT_TYPE) {
        return next.run(req).await;
    }

    let declared_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    const MAX_JSON_REWRITE_BYTES: usize = 64 * 1024;
    if declared_len.is_some_and(|len| len > MAX_JSON_REWRITE_BYTES as u64) {
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    let bytes = match to_bytes(body, MAX_JSON_REWRITE_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return next.run(Request::from_parts(parts, Body::empty())).await,
    };
    let body = if bytes.is_empty() {
        parts
            .headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("2"));
        Bytes::from_static(b"{}")
    } else {
        if let Ok(value) = HeaderValue::from_str(&bytes.len().to_string()) {
            parts.headers.insert(header::CONTENT_LENGTH, value);
        }
        bytes
    };
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    let req = Request::from_parts(parts, Body::from(body));
    next.run(req).await
}

pub(crate) async fn auth(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    if req.method() == Method::OPTIONS || state.password.is_none() {
        return next.run(req).await;
    }

    if authorized(&state, req.headers(), req.uri().query()) {
        return next.run(req).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"kilo\"")],
    )
        .into_response()
}

pub(crate) fn authorized(state: &AppState, headers: &HeaderMap, query: Option<&str>) -> bool {
    let Some(password) = state.password.as_deref() else {
        return true;
    };
    let expected = format!("{}:{}", state.username, password);
    let Some(value) = credential(headers, query) else {
        return false;
    };

    value == expected
}

pub(crate) fn credential(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
    query
        .and_then(auth_token)
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        })
        .map(|value| match value.strip_prefix("Basic ") {
            Some(value) => value.to_string(),
            None => value,
        })
        .and_then(|value| general_purpose::STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
}

pub(crate) fn auth_token(query: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == "auth_token").then(|| decode_query(value))
    })
}
