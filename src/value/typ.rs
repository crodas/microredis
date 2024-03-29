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
//
// The type is a filter to query the database for a values with certain types. The type can be negated
pub struct Typ {
    typ: ValueTyp,
    is_negated: bool,
}

impl Typ {
    /// Whether the current type is negated or not
    pub fn is_negated(&self) -> bool {
        self.is_negated
    }

    /// Checks if a given value is of the same typ
    pub fn check_type(&self, value: &Value) -> bool {
        let t = value.typ();
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
        if let Some(stripped_input) = input.strip_prefix('!') {
            Ok(Self {
                typ: ValueTyp::from_str(stripped_input)?,
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
