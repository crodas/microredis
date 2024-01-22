//! # Thin wrapper for f64 numbers to provide safe maths (checked_add) for incr/hincr operations
use num_traits::CheckedAdd;
use std::{
    convert::{TryFrom, TryInto},
    num::ParseFloatError,
    ops::{Add, Deref},
    str::FromStr,
};

use crate::error::Error;

use super::Value;

/// Float struct (a thing wrapper on top of f64)
#[derive(Copy, Debug, Clone, PartialEq)]
pub struct Float(f64);

impl Deref for Float {
    type Target = f64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<f64> for Float {
    fn from(f: f64) -> Self {
        Float(f)
    }
}

impl TryFrom<&Value> for Float {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Ok(Float(value.try_into()?))
    }
}

impl From<Float> for Value {
    fn from(val: Float) -> Self {
        Value::Float(val.0)
    }
}

impl Add for Float {
    type Output = Float;
    fn add(self, rhs: Self) -> Self::Output {
        Float(self.0 + rhs.0)
    }
}

impl ToString for Float {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl FromStr for Float {
    type Err = ParseFloatError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Float(s.parse::<f64>()?))
    }
}

impl CheckedAdd for Float {
    fn checked_add(&self, v: &Self) -> Option<Self> {
        let n = self.0 + v.0;
        if n.is_finite() {
            Some(Float(n))
        } else {
            None
        }
    }
}
