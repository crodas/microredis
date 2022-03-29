//! # Value Type mod
//!
use crate::value::Value;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

/// Value Type
#[derive(EnumString, Display, PartialEq, Copy, Clone)]
pub enum ValueTyp {
    /// Set
    #[strum(ascii_case_insensitive)]
    Set,
    /// Hash
    #[strum(ascii_case_insensitive)]
    Hash,
    /// List
    #[strum(ascii_case_insensitive)]
    List,
    /// Fallback
    #[strum(ascii_case_insensitive)]
    String,
}

/// Type
pub struct Typ {
    typ: ValueTyp,
    is_negated: bool,
}

impl Typ {
    /// Returns the type from a given value
    pub fn get_type(value: &Value) -> ValueTyp {
        match value {
            Value::Hash(_) => ValueTyp::Hash,
            Value::List(_) => ValueTyp::List,
            Value::Set(_) => ValueTyp::Set,
            _ => ValueTyp::String,
        }
    }

    /// Whether the current type is negated or not
    pub fn is_negated(&self) -> bool {
        self.is_negated
    }

    /// Checks if a given value is of the same type
    pub fn is_value_type(&self, value: &Value) -> bool {
        let t = Self::get_type(value);
        if self.is_negated {
            t != self.typ
        } else {
            t == self.typ
        }
    }
}

impl FromStr for Typ {
    type Err = strum::ParseError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.chars().next() == Some('!') {
            Ok(Self {
                typ: ValueTyp::from_str(&input[1..])?,
                is_negated: true,
            })
        } else {
            Ok(Self {
                typ: ValueTyp::from_str(input)?,
                is_negated: false,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn type_parsing_test_1() {
        let t = Typ::from_str("!set").unwrap();
        assert!(t.is_negated());
    }

    #[test]
    fn type_parsing_test_2() {
        let t = Typ::from_str("set").unwrap();
        assert!(!t.is_negated());
    }
}
