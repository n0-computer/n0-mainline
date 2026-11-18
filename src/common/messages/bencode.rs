//! Minimal bencode decoder and encoder for KRPC messages.
//!
//! KRPC messages are small dictionaries nested at most three levels deep, so this decodes
//! into a borrowed [`Value`] tree with a hard nesting limit instead of using a general
//! purpose (and unbounded) decoder.

use std::{borrow::Cow, fmt};

/// Deepest nesting of lists and dictionaries accepted when decoding.
///
/// KRPC needs three levels (message, arguments, list of values). The extra headroom lets
/// unknown keys carry small nested values; the limit also bounds the recursion of
/// [`decode`].
pub(super) const MAX_DEPTH: usize = 8;

/// A decoded bencode value, borrowing byte strings from the input.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Value<'a> {
    Int(i64),
    Bytes(Cow<'a, [u8]>),
    List(Vec<Value<'a>>),
    Dict(Dict<'a>),
}

/// A bencode dictionary with unique keys, in the order they were decoded or inserted.
#[derive(Debug, Clone, PartialEq, Default)]
pub(super) struct Dict<'a>(Vec<(&'a [u8], Value<'a>)>);

impl<'a> Dict<'a> {
    /// Returns the value stored under `key`, if any.
    pub fn get(&self, key: &[u8]) -> Option<&Value<'a>> {
        self.0.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// Adds an entry; the caller ensures keys are unique.
    pub fn insert(&mut self, key: &'a [u8], value: Value<'a>) {
        debug_assert!(self.get(key).is_none());
        self.0.push((key, value));
    }

    /// Adds an entry if `value` is `Some`.
    pub fn insert_opt(&mut self, key: &'a [u8], value: Option<Value<'a>>) {
        if let Some(value) = value {
            self.insert(key, value);
        }
    }
}

/// Error decoding a bencode message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BencodeError {
    /// The input ended inside a value.
    UnexpectedEnd,
    /// A byte that does not start a bencode value.
    InvalidToken,
    /// An integer or string length that does not parse.
    InvalidNumber,
    /// Lists and dictionaries nest deeper than [`MAX_DEPTH`].
    TooDeep,
    /// A dictionary key that is not a byte string.
    NonStringKey,
    /// A dictionary key that appears twice.
    DuplicateKey,
    /// A required key is missing.
    MissingKey(&'static str),
    /// A key holds a value of the wrong type, length or range.
    InvalidValue(&'static str),
    /// The value of `y` or `q` is not a known message type.
    UnknownMessageType,
    /// A response matches none of the known response shapes.
    UnknownResponse,
}

impl fmt::Display for BencodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "bencode input ended unexpectedly"),
            Self::InvalidToken => write!(f, "invalid bencode token"),
            Self::InvalidNumber => write!(f, "invalid bencode integer or string length"),
            Self::TooDeep => write!(f, "bencode nested deeper than {MAX_DEPTH} levels"),
            Self::NonStringKey => write!(f, "bencode dictionary key is not a string"),
            Self::DuplicateKey => write!(f, "duplicate bencode dictionary key"),
            Self::MissingKey(key) => write!(f, "missing key `{key}`"),
            Self::InvalidValue(key) => write!(f, "invalid value for key `{key}`"),
            Self::UnknownMessageType => write!(f, "unknown KRPC message type"),
            Self::UnknownResponse => write!(f, "unknown KRPC response shape"),
        }
    }
}

impl std::error::Error for BencodeError {}

/// Decodes the bencode value at the start of `bytes`.
///
/// Bytes after the end of the first value are ignored, as KRPC implementations have
/// traditionally done.
pub(super) fn decode(bytes: &[u8]) -> Result<Value<'_>, BencodeError> {
    let mut decoder = Decoder { rest: bytes };
    decoder.value(0)
}

struct Decoder<'a> {
    rest: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn value(&mut self, depth: usize) -> Result<Value<'a>, BencodeError> {
        match self.peek()? {
            b'i' => {
                self.rest = &self.rest[1..];
                let digits = self.take_until(b'e')?;
                Ok(Value::Int(parse_number(digits)?))
            }
            b'0'..=b'9' => Ok(Value::Bytes(Cow::Borrowed(self.bytes()?))),
            b'l' => {
                let depth = enter(depth)?;
                self.rest = &self.rest[1..];
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.value(depth)?);
                }
                self.rest = &self.rest[1..];
                Ok(Value::List(items))
            }
            b'd' => {
                let depth = enter(depth)?;
                self.rest = &self.rest[1..];
                let mut dict = Dict::default();
                while self.peek()? != b'e' {
                    if !self.peek()?.is_ascii_digit() {
                        return Err(BencodeError::NonStringKey);
                    }
                    let key = self.bytes()?;
                    let value = self.value(depth)?;
                    dict.0.push((key, value));
                }
                self.rest = &self.rest[1..];
                // Checked once at the end: checking each key on insertion is quadratic in
                // the number of keys, which an attacker controls.
                let mut keys: Vec<_> = dict.0.iter().map(|(k, _)| *k).collect();
                keys.sort_unstable();
                if keys.windows(2).any(|w| w[0] == w[1]) {
                    return Err(BencodeError::DuplicateKey);
                }
                Ok(Value::Dict(dict))
            }
            _ => Err(BencodeError::InvalidToken),
        }
    }

    fn bytes(&mut self) -> Result<&'a [u8], BencodeError> {
        let digits = self.take_until(b':')?;
        let len: usize = parse_number(digits)?;
        if len > self.rest.len() {
            return Err(BencodeError::UnexpectedEnd);
        }
        let (bytes, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(bytes)
    }

    fn peek(&self) -> Result<u8, BencodeError> {
        self.rest
            .first()
            .copied()
            .ok_or(BencodeError::UnexpectedEnd)
    }

    /// Returns the bytes before the next `end` and consumes them and `end`.
    fn take_until(&mut self, end: u8) -> Result<&'a [u8], BencodeError> {
        let pos = self
            .rest
            .iter()
            .position(|b| *b == end)
            .ok_or(BencodeError::UnexpectedEnd)?;
        let taken = &self.rest[..pos];
        self.rest = &self.rest[pos + 1..];
        Ok(taken)
    }
}

fn enter(depth: usize) -> Result<usize, BencodeError> {
    if depth >= MAX_DEPTH {
        return Err(BencodeError::TooDeep);
    }
    Ok(depth + 1)
}

fn parse_number<T: std::str::FromStr>(digits: &[u8]) -> Result<T, BencodeError> {
    std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(BencodeError::InvalidNumber)
}

/// Encodes `value` into a new buffer, writing dictionary keys in sorted order.
pub(super) fn encode_to_vec(value: &Value<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

fn encode(value: &Value<'_>, out: &mut Vec<u8>) {
    match value {
        Value::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(bytes) => encode_bytes(bytes, out),
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(Dict(entries)) => {
            let mut entries: Vec<_> = entries.iter().collect();
            entries.sort_by_key(|(k, _)| *k);
            out.push(b'd');
            for (key, value) in entries {
                encode_bytes(key, out);
                encode(value, out);
            }
            out.push(b'e');
        }
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

/// Typed access to the entries of a [`Dict`].
///
/// Each getter fails if the key is present with a value of the wrong type; the `opt_`
/// variants return `None` if the key is absent.
impl Dict<'_> {
    pub fn opt_bytes(&self, key: &'static str) -> Result<Option<&[u8]>, BencodeError> {
        match self.get(key.as_bytes()) {
            None => Ok(None),
            Some(Value::Bytes(b)) => Ok(Some(b)),
            Some(_) => Err(BencodeError::InvalidValue(key)),
        }
    }

    pub fn bytes(&self, key: &'static str) -> Result<&[u8], BencodeError> {
        self.opt_bytes(key)?.ok_or(BencodeError::MissingKey(key))
    }

    pub fn opt_array<const N: usize>(
        &self,
        key: &'static str,
    ) -> Result<Option<[u8; N]>, BencodeError> {
        self.opt_bytes(key)?
            .map(|b| b.try_into().map_err(|_| BencodeError::InvalidValue(key)))
            .transpose()
    }

    pub fn array<const N: usize>(&self, key: &'static str) -> Result<[u8; N], BencodeError> {
        self.opt_array(key)?.ok_or(BencodeError::MissingKey(key))
    }

    pub fn opt_int<T: TryFrom<i64>>(&self, key: &'static str) -> Result<Option<T>, BencodeError> {
        match self.get(key.as_bytes()) {
            None => Ok(None),
            Some(Value::Int(i)) => T::try_from(*i)
                .map(Some)
                .map_err(|_| BencodeError::InvalidValue(key)),
            Some(_) => Err(BencodeError::InvalidValue(key)),
        }
    }

    pub fn int<T: TryFrom<i64>>(&self, key: &'static str) -> Result<T, BencodeError> {
        self.opt_int(key)?.ok_or(BencodeError::MissingKey(key))
    }

    pub fn dict(&self, key: &'static str) -> Result<&Self, BencodeError> {
        match self.get(key.as_bytes()) {
            None => Err(BencodeError::MissingKey(key)),
            Some(Value::Dict(d)) => Ok(d),
            Some(_) => Err(BencodeError::InvalidValue(key)),
        }
    }

    pub fn list(&self, key: &'static str) -> Result<&[Value<'_>], BencodeError> {
        match self.get(key.as_bytes()) {
            None => Err(BencodeError::MissingKey(key)),
            Some(Value::List(l)) => Ok(l),
            Some(_) => Err(BencodeError::InvalidValue(key)),
        }
    }

    /// A list whose items are all byte strings.
    pub fn bytes_list(&self, key: &'static str) -> Result<Vec<&[u8]>, BencodeError> {
        self.list(key)?
            .iter()
            .map(|v| match v {
                Value::Bytes(b) => Ok(b.as_ref()),
                _ => Err(BencodeError::InvalidValue(key)),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::{collection, prelude::*};
    use test_strategy::proptest;

    use super::*;

    fn values() -> impl Strategy<Value = Value<'static>> {
        let leaf = prop_oneof![
            any::<i64>().prop_map(Value::Int),
            collection::vec(any::<u8>(), 0..32).prop_map(|bytes| Value::Bytes(Cow::Owned(bytes))),
        ];
        leaf.prop_recursive(3, 32, 4, |inner| {
            // Static keys keep the generated tree owned without leaking allocations.
            // Include empty and non-UTF-8 keys, which bencode permits.
            let keys = prop::sample::select(vec![
                &b""[..],
                &b"a"[..],
                &b"aa"[..],
                &b"b"[..],
                &b"\xff"[..],
            ]);
            prop_oneof![
                collection::vec(inner.clone(), 0..4).prop_map(Value::List),
                collection::btree_map(keys, inner, 0..4)
                    .prop_map(|entries| Value::Dict(Dict(entries.into_iter().collect()))),
            ]
        })
    }

    #[proptest]
    fn value_roundtrip_and_truncation(#[strategy(values())] value: Value<'static>, cut: usize) {
        let encoded = encode_to_vec(&value);
        assert_eq!(decode(&encoded).unwrap(), value);
        // A proper prefix of a single encoded value must never decode successfully.
        assert!(decode(&encoded[..cut % encoded.len()]).is_err());
    }

    #[proptest]
    fn dictionary_order_and_duplicates(
        #[strategy(collection::btree_map(collection::vec(any::<u8>(), 0..16), any::<i64>(), 1..8))]
        entries: std::collections::BTreeMap<Vec<u8>, i64>,
    ) {
        let dict = Dict(
            entries
                .iter()
                .map(|(k, v)| (k.as_slice(), Value::Int(*v)))
                .collect(),
        );
        let canonical = encode_to_vec(&Value::Dict(dict.clone()));
        // Build unsorted wire input directly: the encoder would sort it for us.
        let mut wire = vec![b'd'];
        for (key, value) in dict.0.iter().rev() {
            encode_bytes(key, &mut wire);
            encode(value, &mut wire);
        }
        wire.push(b'e');
        assert_eq!(encode_to_vec(&decode(&wire).unwrap()), canonical);
        wire.pop();
        let (key, value) = &dict.0[0];
        encode_bytes(key, &mut wire);
        encode(value, &mut wire);
        wire.push(b'e');
        assert_eq!(decode(&wire), Err(BencodeError::DuplicateKey));
    }

    #[proptest]
    fn arbitrary_input_is_safe_and_canonicalizes(
        #[strategy(collection::vec(any::<u8>(), 0..2048))] input: Vec<u8>,
    ) {
        // Any input may be rejected, but decoding must not panic. Accepted input
        // must have a stable canonical encoding (trailing bytes are allowed).
        if let Ok(value) = decode(&input) {
            let canonical = encode_to_vec(&value);
            assert_eq!(encode_to_vec(&decode(&canonical).unwrap()), canonical);
        }
    }

    #[proptest]
    fn mixed_container_depth_limit(
        #[strategy(collection::vec(any::<bool>(), 0..=16))] containers: Vec<bool>,
        value: i64,
    ) {
        let mut wire = Vec::new();
        for dict in &containers {
            wire.extend_from_slice(if *dict { b"d1:k" } else { b"l" });
        }
        encode(&Value::Int(value), &mut wire);
        wire.extend(std::iter::repeat_n(b'e', containers.len()));
        if containers.len() <= MAX_DEPTH {
            assert!(decode(&wire).is_ok());
        } else {
            assert_eq!(decode(&wire), Err(BencodeError::TooDeep));
        }
    }

    fn as_dict(value: Value<'_>) -> Dict<'_> {
        match value {
            Value::Dict(dict) => Some(dict),
            _ => None,
        }
        .expect("not a dict")
    }

    fn roundtrip(input: &[u8]) {
        assert_eq!(encode_to_vec(&decode(input).unwrap()), input);
    }

    #[test]
    fn decode_encode_roundtrip() {
        roundtrip(b"i0e");
        roundtrip(b"i-42e");
        roundtrip(b"0:");
        roundtrip(b"4:spam");
        roundtrip(b"le");
        roundtrip(b"de");
        roundtrip(b"l4:spami7ee");
        roundtrip(b"d3:bar4:spam3:fooi42e1:xld1:ai1eeee");
    }

    #[test]
    fn encode_sorts_keys() {
        let value = decode(b"d1:bi2e1:ai1ee").unwrap();
        assert_eq!(encode_to_vec(&value), b"d1:ai1e1:bi2ee");
    }

    #[test]
    fn ignores_trailing_bytes() {
        assert_eq!(decode(b"i1eXYZ").unwrap(), Value::Int(1));
    }

    #[test]
    fn rejects_malformed() {
        use BencodeError::*;
        for (input, err) in [
            (&b""[..], UnexpectedEnd),
            (b"x", InvalidToken),
            (b"i12", UnexpectedEnd),
            (b"ie", InvalidNumber),
            (b"i1x2e", InvalidNumber),
            (b"i99999999999999999999e", InvalidNumber),
            (b"5:abc", UnexpectedEnd),
            (b"99999999999999999999999:a", InvalidNumber),
            (b"l", UnexpectedEnd),
            (b"li1e", UnexpectedEnd),
            (b"d", UnexpectedEnd),
            (b"di1ei2ee", NonStringKey),
            (b"d1:a", UnexpectedEnd),
            (b"d1:ai1e1:ai2ee", DuplicateKey),
            (b"d1:ai1e1:bi2e1:ai3ee", DuplicateKey),
            (b"d1:ad1:xi1e1:xi2eee", DuplicateKey),
        ] {
            assert_eq!(
                decode(input),
                Err(err),
                "{}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn decodes_values() {
        let value = decode(b"d1:bli-3e3:\x00\xff:e1:ad1:x0:ee").unwrap();
        let dict = as_dict(value);
        assert_eq!(
            dict.get(b"b"),
            Some(&Value::List(vec![
                Value::Int(-3),
                Value::Bytes(Cow::Borrowed(b"\x00\xff:"))
            ]))
        );
        assert_eq!(dict.dict("a").unwrap().bytes("x").unwrap(), b"");
        assert_eq!(dict.list("b").unwrap().len(), 2);
        assert_eq!(dict.bytes("b"), Err(BencodeError::InvalidValue("b")));
        assert_eq!(dict.bytes("c"), Err(BencodeError::MissingKey("c")));
    }

    #[test]
    fn many_keys() {
        // Unsorted, distinct keys: decoding must stay fast, and a late duplicate is found.
        let mut input = b"d".to_vec();
        for i in (0..10_000u32).rev() {
            input.extend_from_slice(format!("5:{i:05}0:").as_bytes());
        }
        let mut with_duplicate = input.clone();
        input.push(b'e');
        let dict = as_dict(decode(&input).unwrap());
        assert_eq!(dict.0.len(), 10_000);
        with_duplicate.extend_from_slice(b"5:042420:e");
        assert_eq!(decode(&with_duplicate), Err(BencodeError::DuplicateKey));
    }

    #[test]
    fn nesting_limit() {
        let nested = |depth: usize| {
            let mut v = vec![b'l'; depth];
            v.extend(std::iter::repeat_n(b'e', depth));
            v
        };
        assert!(decode(&nested(MAX_DEPTH)).is_ok());
        assert_eq!(decode(&nested(MAX_DEPTH + 1)), Err(BencodeError::TooDeep));
        // Would overflow the stack of a decoder without a limit.
        let deep = [b"d1:a".as_slice(), &[b'l'; 1_000_000]].concat();
        assert_eq!(decode(&deep), Err(BencodeError::TooDeep));
    }
}
