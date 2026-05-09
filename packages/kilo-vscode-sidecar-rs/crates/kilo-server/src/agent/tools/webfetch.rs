use std::sync::atomic::AtomicBool;
use std::time::Duration;

use serde_json::{json, Value};

const MAX_RESPONSE_SIZE: usize = 5 * 1024 * 1024;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 120;

pub(crate) async fn fake_webfetch_cancel(
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let raw = input
        .get("url")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "url is required".to_string())?;
    let url = normalized_url(raw)?;
    let format = input
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("markdown");
    if !matches!(format, "markdown" | "text" | "html") {
        return Err("format must be markdown, text, or html".to_string());
    }
    let timeout = input
        .get("timeout")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_SECS)
        .clamp(1, MAX_TIMEOUT_SECS);
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout))
        .user_agent("kilo")
        .build()
        .map_err(|err| format!("webfetch client failed: {err}"))?;
    let response = client
        .get(&url)
        .header(reqwest::header::ACCEPT, accept_header(format))
        .send()
        .await
        .map_err(|err| format!("webfetch request failed: {err}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("webfetch request failed with status {status}"));
    }
    if let Some(length) = response.content_length() {
        if length as usize > MAX_RESPONSE_SIZE {
            return Err("Response too large (exceeds 5MB limit)".to_string());
        }
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = response
        .bytes()
        .await
        .map_err(|err| format!("webfetch body failed: {err}"))?;
    if bytes.len() > MAX_RESPONSE_SIZE {
        return Err("Response too large (exceeds 5MB limit)".to_string());
    }
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    let text = String::from_utf8_lossy(&bytes).to_string();
    let output = if content_type.contains("text/html") && format != "html" {
        html_text(&text)
    } else {
        text
    };
    let title = format!("{url} ({content_type})");
    Ok((
        title,
        output,
        json!({
            "url": url,
            "format": format,
            "contentType": content_type,
            "bytes": bytes.len()
        }),
    ))
}

fn normalized_url(raw: &str) -> Result<String, String> {
    if raw.starts_with("https://") {
        return Ok(raw.to_string());
    }
    if let Some(rest) = raw.strip_prefix("http://") {
        return Ok(format!("https://{rest}"));
    }
    Err("URL must start with http:// or https://".to_string())
}

fn accept_header(format: &str) -> &'static str {
    match format {
        "markdown" => {
            "text/markdown;q=1.0, text/x-markdown;q=0.9, text/plain;q=0.8, text/html;q=0.7, */*;q=0.1"
        }
        "text" => "text/plain;q=1.0, text/markdown;q=0.9, text/html;q=0.8, */*;q=0.1",
        "html" => "text/html;q=1.0, application/xhtml+xml;q=0.9, text/plain;q=0.8, */*;q=0.1",
        _ => "*/*",
    }
}

fn html_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut tag = false;
    for ch in html.chars() {
        match ch {
            '<' => tag = true,
            '>' => {
                tag = false;
                out.push(' ');
            }
            _ if !tag => out.push(ch),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}
