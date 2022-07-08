use super::Error;
use serde::de;
use std::{borrow::Cow, str::FromStr};

pub struct ValueDeserializer<'a> {
    pub input: Cow<'a, str>,
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
                _ => visitor.visit_str(&self.input),
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
