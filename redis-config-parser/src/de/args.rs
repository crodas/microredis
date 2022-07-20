use super::{value::ValueDeserializer, Error};
use crate::parser::Args;
use serde::de;
use std::borrow::Cow;

pub struct ArgsDeserializer<'a> {
    pub input: Args<'a>,
}

impl<'de> de::Deserializer<'de> for ArgsDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: de::Visitor<'de>,
    {
        match self.input {
            Args::None => visitor.visit_none(),
            Args::Single(v) => {
                let des = ValueDeserializer { input: v };
                des.deserialize_any(visitor)
            }
            Args::Multiple(v) => visitor.visit_seq(SeqArgsDeserializer { input: v, id: 0 }),
        }
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string seq
        bytes byte_buf map unit newtype_struct
        ignored_any unit_struct tuple_struct tuple option identifier
        enum struct
    }
}

pub struct SeqArgsDeserializer<'a> {
    pub input: Vec<Cow<'a, str>>,
    pub id: usize,
}

impl<'de> de::SeqAccess<'de> for SeqArgsDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        let value = match self.input.get(self.id) {
            Some(v) => v,
            None => return Ok(None),
        };
        let out = seed
            .deserialize(ValueDeserializer {
                input: value.clone(),
            })
            .map(Some);
        self.id += 1;
        out
    }
}
