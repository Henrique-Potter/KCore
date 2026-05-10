//! BOM-aware text I/O for the Rust port of Bun's `kilocode/encoding.ts`.
//!
//! Lean target: detection is BOM-only (no `chardetng`/`encoding_rs`).
//! Supported encodings: UTF-8 (with or without BOM), UTF-16 LE BOM,
//! UTF-16 BE BOM. Files without a BOM and without valid UTF-8 fall
//! back to UTF-8 lossy and are treated as UTF-8 on write.
//!
//! UTF-16 codecs are open-coded — the workspace deliberately avoids
//! `encoding_rs` for the lean target.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Utf8,
    Utf8Bom,
    Utf16Le,
    Utf16Be,
}

const UTF8_BOM: [u8; 3] = [0xef, 0xbb, 0xbf];
const UTF16_LE_BOM: [u8; 2] = [0xff, 0xfe];
const UTF16_BE_BOM: [u8; 2] = [0xfe, 0xff];

pub fn detect(bytes: &[u8]) -> Encoding {
    if bytes.starts_with(&UTF8_BOM) {
        return Encoding::Utf8Bom;
    }
    if bytes.starts_with(&UTF16_LE_BOM) {
        return Encoding::Utf16Le;
    }
    if bytes.starts_with(&UTF16_BE_BOM) {
        return Encoding::Utf16Be;
    }
    Encoding::Utf8
}

/// Strip BOM if present, decode to a Rust `String`, return both the
/// decoded text and the detected encoding so callers can preserve it
/// on write-back.
pub fn read_to_string(bytes: &[u8]) -> (String, Encoding) {
    let enc = detect(bytes);
    match enc {
        Encoding::Utf8 => (String::from_utf8_lossy(bytes).into_owned(), enc),
        Encoding::Utf8Bom => {
            let body = &bytes[UTF8_BOM.len()..];
            (String::from_utf8_lossy(body).into_owned(), enc)
        }
        Encoding::Utf16Le => {
            let body = &bytes[UTF16_LE_BOM.len()..];
            (decode_utf16_le(body), enc)
        }
        Encoding::Utf16Be => {
            let body = &bytes[UTF16_BE_BOM.len()..];
            (decode_utf16_be(body), enc)
        }
    }
}

/// Re-encode `text` for `encoding`, prepending the original BOM if any.
pub fn write_bytes(text: &str, encoding: Encoding) -> Vec<u8> {
    let body = strip_leading_bom_char(text);
    match encoding {
        Encoding::Utf8 => body.as_bytes().to_vec(),
        Encoding::Utf8Bom => {
            let mut out = Vec::with_capacity(UTF8_BOM.len() + body.len());
            out.extend_from_slice(&UTF8_BOM);
            out.extend_from_slice(body.as_bytes());
            out
        }
        Encoding::Utf16Le => {
            let mut out = Vec::with_capacity(UTF16_LE_BOM.len() + body.len() * 2);
            out.extend_from_slice(&UTF16_LE_BOM);
            for unit in body.encode_utf16() {
                out.extend_from_slice(&unit.to_le_bytes());
            }
            out
        }
        Encoding::Utf16Be => {
            let mut out = Vec::with_capacity(UTF16_BE_BOM.len() + body.len() * 2);
            out.extend_from_slice(&UTF16_BE_BOM);
            for unit in body.encode_utf16() {
                out.extend_from_slice(&unit.to_be_bytes());
            }
            out
        }
    }
}

fn strip_leading_bom_char(text: &str) -> &str {
    text.strip_prefix('\u{feff}').unwrap_or(text)
}

fn decode_utf16_le(bytes: &[u8]) -> String {
    decode_utf16(bytes, u16::from_le_bytes)
}

fn decode_utf16_be(bytes: &[u8]) -> String {
    decode_utf16(bytes, u16::from_be_bytes)
}

/// Decode an even-length UTF-16 byte stream. If `bytes.len()` is odd the
/// trailing byte is malformed input — `chunks_exact(2)` would silently
/// discard it. Instead we decode the even prefix, append U+FFFD for the
/// dangling byte, and log a single warning so the truncation is visible.
fn decode_utf16(bytes: &[u8], from_bytes: fn([u8; 2]) -> u16) -> String {
    let units = bytes
        .chunks_exact(2)
        .map(|c| from_bytes([c[0], c[1]]))
        .collect::<Vec<_>>();
    let mut out = String::from_utf16_lossy(&units);
    if bytes.len() % 2 == 1 {
        eprintln!("[kilo-server] truncated UTF-16 file: odd byte length");
        out.push('\u{fffd}');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_utf8_bom() {
        let bytes = [0xef, 0xbb, 0xbf, b'h', b'i'];
        assert_eq!(detect(&bytes), Encoding::Utf8Bom);
    }

    #[test]
    fn detect_plain_utf8() {
        assert_eq!(detect(b"hello"), Encoding::Utf8);
    }

    #[test]
    fn detect_utf16_le() {
        let bytes = [0xff, 0xfe, 0x68, 0x00, 0x69, 0x00];
        assert_eq!(detect(&bytes), Encoding::Utf16Le);
    }

    #[test]
    fn detect_utf16_be() {
        let bytes = [0xfe, 0xff, 0x00, 0x68, 0x00, 0x69];
        assert_eq!(detect(&bytes), Encoding::Utf16Be);
    }

    #[test]
    fn utf8_bom_round_trip() {
        let original = b"\xef\xbb\xbfhello world";
        let (text, enc) = read_to_string(original);
        assert_eq!(enc, Encoding::Utf8Bom);
        assert_eq!(text, "hello world");
        let written = write_bytes(&text, enc);
        assert_eq!(written, original);
    }

    #[test]
    fn utf16_le_round_trip() {
        let mut original = Vec::new();
        original.extend_from_slice(&UTF16_LE_BOM);
        for unit in "héllo".encode_utf16() {
            original.extend_from_slice(&unit.to_le_bytes());
        }
        let (text, enc) = read_to_string(&original);
        assert_eq!(enc, Encoding::Utf16Le);
        assert_eq!(text, "héllo");
        let written = write_bytes(&text, enc);
        assert_eq!(written, original);
    }

    #[test]
    fn utf16_be_round_trip() {
        let mut original = Vec::new();
        original.extend_from_slice(&UTF16_BE_BOM);
        for unit in "wörld".encode_utf16() {
            original.extend_from_slice(&unit.to_be_bytes());
        }
        let (text, enc) = read_to_string(&original);
        assert_eq!(enc, Encoding::Utf16Be);
        assert_eq!(text, "wörld");
        let written = write_bytes(&text, enc);
        assert_eq!(written, original);
    }

    #[test]
    fn plain_utf8_round_trip() {
        let original = b"plain text";
        let (text, enc) = read_to_string(original);
        assert_eq!(enc, Encoding::Utf8);
        assert_eq!(text, "plain text");
        assert_eq!(write_bytes(&text, enc), original);
    }

    #[test]
    fn encoding_odd_length_utf16_returns_replacement_char() {
        // BOM + 'h' (0x68 0x00) + dangling odd byte (0x69). Decoder must
        // return the leading 'h' followed by a U+FFFD replacement char,
        // without panicking.
        let bytes = [0xff, 0xfe, 0x68, 0x00, 0x69];
        let (text, enc) = read_to_string(&bytes);
        assert_eq!(enc, Encoding::Utf16Le);
        assert!(text.starts_with('h'), "expected leading 'h': {text:?}");
        assert!(text.contains('\u{fffd}'), "expected U+FFFD in {text:?}");

        // Same check on BE path.
        let bytes = [0xfe, 0xff, 0x00, 0x68, 0x69];
        let (text, enc) = read_to_string(&bytes);
        assert_eq!(enc, Encoding::Utf16Be);
        assert!(text.starts_with('h'), "expected leading 'h': {text:?}");
        assert!(text.contains('\u{fffd}'), "expected U+FFFD in {text:?}");
    }

    #[test]
    fn double_bom_avoided_on_write() {
        // Caller may have round-tripped text that still has a leading
        // U+FEFF; write_bytes must not emit two BOMs.
        let text = "\u{feff}hi";
        let written = write_bytes(text, Encoding::Utf8Bom);
        assert_eq!(written, b"\xef\xbb\xbfhi");
    }
}
