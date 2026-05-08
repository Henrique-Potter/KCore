//! `MessageCursor` encode/decode in the Bun-compatible base64 shape.
//!
//! Step 8 of the kilo-server module split: lifted out of `routes/messages.rs`
//! so the encoding is reusable by any future caller (link headers, fixture
//! tooling) without dragging in the message route handler module.

use base64::{engine::general_purpose, Engine};
use kilo_store::MessageCursor;
use serde_json::json;

pub(crate) fn encode_cursor(cursor: &MessageCursor) -> Option<String> {
    let data = json!({ "id": cursor.id, "time": cursor.time }).to_string();
    Some(general_purpose::URL_SAFE_NO_PAD.encode(data))
}

pub(crate) fn decode_cursor(input: &str) -> Option<MessageCursor> {
    let data = general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| general_purpose::URL_SAFE.decode(input))
        .ok()?;
    let data: serde_json::Value = serde_json::from_slice(&data).ok()?;
    let id = data.get("id")?.as_str()?.to_string();
    let time = data.get("time")?.as_i64()?;
    Some(MessageCursor { id, time })
}
