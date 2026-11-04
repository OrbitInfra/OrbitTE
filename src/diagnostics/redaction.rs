use super::{RedactionMetadata, SanitizedValue};
use regex_lite::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, net::IpAddr, sync::OnceLock};

const REDACTED: &str = "[redacted]";

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RedactionMode {
    #[default]
    Default,
    Strict,
    None,
}
impl RedactionMode {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Strict => "strict",
            Self::None => "none",
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    #[default]
    Public,
    Deployment,
    Secret,
    RawAddress,
}

/// The pseudonym dictionary belongs to one snapshot.
pub struct Redactor {
    metadata: RedactionMetadata,
    pseudonyms: HashMap<String, String>,
    known: Vec<String>,
}
impl Redactor {
    pub fn new(mode: RedactionMode) -> Self {
        Self {
            metadata: RedactionMetadata {
                mode,
                fields_redacted: 0,
            },
            pseudonyms: HashMap::new(),
            known: Vec::new(),
        }
    }
    pub fn metadata(&self) -> RedactionMetadata {
        self.metadata
    }
    fn pseudonym(&mut self, v: &Value) -> String {
        let next = self.pseudonyms.len() + 1;
        self.pseudonyms
            .entry(v.to_string())
            .or_insert_with(|| format!("entity-{next}"))
            .clone()
    }
    pub fn sanitize(
        &mut self,
        field: &str,
        value: Value,
        sensitivity: Sensitivity,
    ) -> SanitizedValue {
        let field = field.to_ascii_lowercase();
        let sensitive = matches!(sensitivity, Sensitivity::Secret | Sensitivity::RawAddress)
            || [
                "token",
                "password",
                "passwd",
                "credential",
                "secret",
                "private_key",
                "api_key",
                "access_key",
                "rkey",
                "lkey",
                "raw_address",
                "ipc_handle",
                "payload",
            ]
            .iter()
            .any(|s| field.contains(s));
        let replacement = if sensitive {
            Some(REDACTED.to_owned())
        } else if self.metadata.mode == RedactionMode::Strict
            && sensitivity == Sensitivity::Deployment
        {
            if let Some(s) = value
                .as_str()
                .filter(|s| s.len() >= 3 && !self.known.iter().any(|k| k == s))
            {
                self.known.push(s.into());
                self.known.sort_by_key(|k| std::cmp::Reverse(k.len()));
            }
            Some(self.pseudonym(&value))
        } else {
            None
        };
        let redacted = replacement.is_some();
        self.metadata.fields_redacted += u64::from(redacted);
        SanitizedValue {
            available: true,
            observed: Some(replacement.map_or(value, Value::String)),
            redacted,
        }
    }
    pub fn sanitize_text(&mut self, text: &str) -> String {
        let mut result = text.to_owned();
        let Some(patterns) = secret_patterns() else {
            self.metadata.fields_redacted += 1;
            return REDACTED.into();
        };
        for (pattern, group) in patterns {
            let mut output = String::new();
            let mut copied = 0;
            for caps in pattern.captures_iter(&result) {
                let Some(m) = caps.get(*group) else {
                    continue;
                };
                let raw = m.as_str();
                let quoted = raw.len() >= 2
                    && matches!(raw.as_bytes()[0], b'"' | b'\'')
                    && raw.as_bytes().first() == raw.as_bytes().last();
                let edge = usize::from(quoted);
                if raw[edge..raw.len() - edge] == *REDACTED {
                    continue;
                }
                output.push_str(&result[copied..m.start() + edge]);
                output.push_str(REDACTED);
                copied = m.end() - edge;
                self.metadata.fields_redacted += 1;
            }
            if copied != 0 {
                output.push_str(&result[copied..]);
                result = output;
            }
        }
        if self.metadata.mode != RedactionMode::Strict {
            return result;
        }
        for known in &self.known {
            let mut output = String::new();
            let mut copied = 0;
            for (start, _) in result.match_indices(known) {
                let end = start + known.len();
                if bounded(&result, start, end) {
                    output.push_str(&result[copied..start]);
                    let key = Value::String(known.clone()).to_string();
                    output.push_str(self.pseudonyms.get(&key).map_or(REDACTED, String::as_str));
                    copied = end;
                    self.metadata.fields_redacted += 1;
                }
            }
            if copied != 0 {
                output.push_str(&result[copied..]);
                result = output;
            }
        }
        let mut out = String::new();
        let mut i = 0;
        while i < result.len() {
            let start = i;
            while result
                .as_bytes()
                .get(i)
                .is_some_and(|b| b.is_ascii_hexdigit() || matches!(b, b':' | b'.'))
            {
                i += 1;
            }
            if i == start {
                let Some(c) = result[i..].chars().next() else {
                    break;
                };
                out.push(c);
                i += c.len_utf8();
            } else {
                self.address_run(&mut out, &result, start, i);
            }
        }
        out
    }

    fn address_run(&mut self, out: &mut String, text: &str, mut start: usize, mut end: usize) {
        while start < end && text.as_bytes()[start] == b'.' {
            out.push('.');
            start += 1;
        }
        let last = end;
        while start < end && text.as_bytes()[end - 1] == b'.' {
            end -= 1;
        }
        // Split label and port separators iteratively, preserving IPv6 double colons.
        while start < end {
            let s = &text[start..end];
            if s.len() <= 64 && bounded(text, start, end) && is_address(s) {
                out.push_str(&self.pseudonym(&Value::String(s.into())));
                self.metadata.fields_redacted += 1;
                break;
            }
            let separator = s.bytes().enumerate().position(|(i, c)| {
                c == b':'
                    && (i == 0 || s.as_bytes()[i - 1] != b':')
                    && s.as_bytes().get(i + 1) != Some(&b':')
            });
            let Some(at) = separator else {
                out.push_str(s);
                break;
            };
            if bounded(text, start, start + at) && is_address(&s[..at]) {
                out.push_str(&self.pseudonym(&Value::String(s[..at].into())));
                self.metadata.fields_redacted += 1;
            } else {
                out.push_str(&s[..at]);
            }
            out.push(':');
            start += at + 1;
        }
        out.push_str(&text[end..last]);
    }
}
fn bounded(text: &str, start: usize, end: usize) -> bool {
    let word = |b: &u8| b.is_ascii_alphanumeric() || *b == b'_';
    !text.as_bytes()[..start].last().is_some_and(word)
        && !text.as_bytes().get(end).is_some_and(word)
}
fn is_address(s: &str) -> bool {
    if let Ok(addr) = s.parse::<IpAddr>() {
        return addr.is_ipv4() || s.bytes().any(|b| b.is_ascii_digit());
    }
    let n = s.split(':').count();
    let width = match n {
        6 | 8 | 16 => 2,
        4 => 4,
        _ => return false,
    };
    s.split(':')
        .all(|g| g.len() == width && g.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn secret_patterns() -> Option<&'static Vec<(Regex, usize)>> {
    static PATTERNS: OnceLock<Option<Vec<(Regex, usize)>>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            (r#"(?i)([a-z][a-z0-9+.\-]*://)([^\s/@]+)@"#,2),
            (r#"(?i)(authorization["']?\s*[:=]\s*["']?)((?:bearer|basic|digest|token)\s+)?([^\s"',;]+)"#,3),
            (r#"(?i)\b(bearer|basic)\s+([^\s"',;]+)"#,2),
            (r#"(?i)(["']?[a-z0-9_.\-]*(?:password|passwd|token|secret|credential|private_key|access_key|api_key|apikey|rkey|lkey)[a-z0-9_.\-]*["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s"',;&}\]]+)"#,2),
            (r#"(?i)\b([rl]key)\s+(0x[0-9a-f]+|[0-9]+)"#,2),
            (r#"(?i)\b(0x[0-9a-f]{8,})\b"#,1),
        ].into_iter().map(|(pattern,i)|Some((Regex::new(pattern).ok()?,i))).collect()
    }).as_ref()
}
