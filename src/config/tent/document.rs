use super::{Result, invalid};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(super) const MAX_BYTES: usize = 1 << 20;

pub(super) fn read(path: &Path, key: &str) -> Result<String> {
    // A writer that keeps a FIFO open must not hold up engine construction.
    let mut options = File::options();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| invalid(key, "cannot open configuration file"))?;
    if !file.metadata().is_ok_and(|m| m.is_file()) {
        return Err(invalid(key, "expected a regular file"));
    }
    let mut bytes = String::new();
    file.take((MAX_BYTES + 1) as u64)
        .read_to_string(&mut bytes)
        .map_err(|_| invalid(key, "cannot read UTF-8 configuration file"))?;
    if bytes.len() > MAX_BYTES {
        return Err(invalid(key, "file exceeds 1 MiB"));
    }
    Ok(bytes)
}

// Parsing into Value directly would erase duplicate fields before the selector
// and QoS owners could reject them. The JSON reader also bounds nesting depth.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Json;
        impl<'de> Visitor<'de> for Json {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON with unique object keys")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut m: M,
            ) -> std::result::Result<Unique, M::Error> {
                let mut o = Map::new();
                while let Some(k) = m.next_key::<String>()? {
                    if o.contains_key(&k) {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                    o.insert(k, m.next_value::<Unique>()?.0);
                }
                Ok(Unique(Value::Object(o)))
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut s: S,
            ) -> std::result::Result<Unique, S::Error> {
                let mut a = Vec::new();
                while let Some(Unique(v)) = s.next_element()? {
                    a.push(v);
                }
                Ok(Unique(Value::Array(a)))
            }
            fn visit_unit<E>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_bool<E>(self, v: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_i64<E>(self, v: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_u64<E>(self, v: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_f64<E>(self, v: f64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
        }
        d.deserialize_any(Json)
    }
}

pub(super) fn parse(text: &str, source: &str) -> Result<Value> {
    if text.len() > MAX_BYTES {
        return Err(invalid(source, "JSON exceeds 1 MiB"));
    }
    let Unique(doc) = serde_json::from_str(text)
        .map_err(|_| invalid(source, "invalid JSON or duplicate field"))?;
    let Value::Object(fields) = doc else {
        return Err(invalid("$", "expected an object root"));
    };
    let mut root = serde_json::json!({});
    // Config::get prefers a nested value, then tries a full slash key at the root.
    let (flat, nested): (Vec<_>, Vec<_>) =
        fields.into_iter().partition(|(key, _)| key.contains('/'));
    for (key, val) in nested {
        check_key(&key)?;
        root[&key] = val;
    }
    // Decide eligibility against the original nested document. One flat
    // subtree cannot shadow another, more specific, full-path spelling.
    let mut missing = Vec::new();
    for (key, val) in flat {
        check_key(&key)?;
        if get(&root, &key).is_none() {
            missing.push((key, val));
        }
    }
    missing.sort_by_key(|(key, _)| key.matches('/').count());
    for (key, val) in missing {
        set(&mut root, &key, val)?;
    }
    Ok(root)
}

pub(super) fn check_key(path: &str) -> Result<()> {
    if path.len() > 1024
        || path.split('/').count() > 32
        || path
            .split('/')
            .any(|s| s.is_empty() || s.chars().any(char::is_control))
    {
        return Err(invalid("$", "malformed configuration path"));
    }
    Ok(())
}
pub(super) fn get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut at = root;
    for key in path.split('/') {
        at = at.get(key)?;
    }
    Some(at)
}
pub(super) fn set(root: &mut Value, path: &str, value: Value) -> Result<()> {
    let mut at = root;
    for key in path.split('/') {
        at = at
            .as_object_mut()
            .ok_or_else(|| invalid(path, "parent must be an object"))?
            .entry(key)
            .or_insert_with(|| Value::Object(Map::new()));
    }
    *at = value;
    Ok(())
}
