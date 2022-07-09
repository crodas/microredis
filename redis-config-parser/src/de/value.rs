use super::{args::SeqArgsDeserializer, Error};
use serde::de;
use std::{borrow::Cow, fmt, str::FromStr};

pub struct ValueDeserializer<'a> {
    pub input: Cow<'a, str>,
}

pub struct Fmt<F>(pub F)
where
    F: Fn(&mut fmt::Formatter) -> fmt::Result;

impl<F> fmt::Debug for Fmt<F>
where
    F: Fn(&mut fmt::Formatter) -> fmt::Result,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (self.0)(f)
    }
}

impl<'de> de::Deserializer<'de> for ValueDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: de::Visitor<'de>,
    {
        match i64::from_str(&self.input) {
            Ok(number) => visitor.visit_i64(number),
            Err(_) => match self.input.to_ascii_lowercase().as_str() {
                "true" | "yes" => visitor.visit_bool(true),
                "false" | "no" => visitor.visit_bool(false),
                _ => {
                    // is there a better hack?
                    match format!("{:?}", Fmt(|f| visitor.expecting(f))).as_str() {
                        "a sequence" => visitor.visit_seq(SeqArgsDeserializer {
                            input: vec![self.input],
                            id: 0,
                        }),
                        "option" => visitor.visit_some(ValueDeserializer { input: self.input }),
                        _ => visitor.visit_str(&self.input),
                    }
                }
            },
        }
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string seq
        bytes byte_buf map unit newtype_struct
        ignored_any unit_struct tuple_struct tuple option identifier
        enum struct
    }
}
