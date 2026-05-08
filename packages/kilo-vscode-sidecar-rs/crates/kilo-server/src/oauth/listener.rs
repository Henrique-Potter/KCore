//! Loopback HTTP listener that catches the post-authorize browser redirect.
//!
//! The OpenAI authorize URL points the user agent at
//! `http://localhost:1455/auth/callback`. We bind that port lazily on
//! `oauth_authorize`, run a tiny request parser, and dispatch back into the
//! `routes::config::oauth_callback` handler so the browser path and the
//! direct-API path share one token-exchange code path.
//!
//! `oauth_html` + `html_escape` render the static success/failure page
//! shown to the user once the redirect completes.

use std::io::ErrorKind;
use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::AppState;

use super::url::query_params;

pub(crate) async fn ensure_oauth_listener(state: Arc<AppState>) -> std::io::Result<()> {
    {
        let guard = state.oauth_listener.lock().unwrap();
        if guard.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Ok(());
        }
    }

    let listener = match TcpListener::bind(state.oauth_listener_addr).await {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::AddrInUse => {
            let guard = state.oauth_listener.lock().unwrap();
            if guard.as_ref().is_some_and(|handle| !handle.is_finished()) {
                return Ok(());
            }
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    let next = state.clone();
    let task = tokio::spawn(async move {
        if let Err(err) = run_oauth_listener(next, listener).await {
            eprintln!("[kilo-server] openai oauth callback listener stopped: {err}");
        }
    });
    let mut guard = state.oauth_listener.lock().unwrap();
    if guard.as_ref().is_some_and(|handle| !handle.is_finished()) {
        return Ok(());
    }
    *guard = Some(task);
    Ok(())
}

pub(crate) async fn run_oauth_listener(
    state: Arc<AppState>,
    listener: TcpListener,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let mut buf = [0; 4096];
            let read = match stream.read(&mut buf).await {
                Ok(read) => read,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..read]);
            let line = req.lines().next().unwrap_or_default();
            let target = line.split_whitespace().nth(1).unwrap_or("/");
            let (status, body) = oauth_browser_callback(state, target).await;
            let res = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(res.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

async fn oauth_browser_callback(state: Arc<AppState>, target: &str) -> (&'static str, String) {
    let Some((path, query)) = target.split_once('?') else {
        return (
            "404 Not Found",
            oauth_html("Kilo", "OAuth callback route not found."),
        );
    };
    if path != "/auth/callback" {
        return (
            "404 Not Found",
            oauth_html("Kilo", "OAuth callback route not found."),
        );
    }
    let params = query_params(query);
    if let Some(err) = params.get("error") {
        return (
            "200 OK",
            oauth_html(
                "Kilo authentication failed",
                params
                    .get("error_description")
                    .map(String::as_str)
                    .unwrap_or(err),
            ),
        );
    }
    let Some(code) = params.get("code") else {
        return (
            "400 Bad Request",
            oauth_html("Kilo authentication failed", "Missing authorization code."),
        );
    };
    let Some(csrf) = params.get("state") else {
        return (
            "400 Bad Request",
            oauth_html("Kilo authentication failed", "Missing OAuth state."),
        );
    };

    let res = crate::routes::config::complete_browser_oauth_callback(state, code, csrf).await;
    if res.status().is_success() {
        return (
            "200 OK",
            oauth_html(
                "Kilo authentication complete",
                "You can close this window and return to Kilo.",
            ),
        );
    }
    (
        "500 Internal Server Error",
        oauth_html(
            "Kilo authentication failed",
            "Kilo could not complete the OpenAI token exchange.",
        ),
    )
}

pub(crate) fn oauth_html(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><h1>{}</h1><p>{}</p></body></html>",
        html_escape(title),
        html_escape(title),
        html_escape(body)
    )
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
