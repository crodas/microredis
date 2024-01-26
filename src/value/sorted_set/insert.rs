use crate::error::Error;
use bytes::Bytes;
use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum IPolicy {
    NX,
    XX,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum UPolicyScore {
    LT,
    GT,
}

#[derive(Debug, Default, Clone)]
/// Insert option
pub struct IOption {
    pub(crate) insert_policy: Option<IPolicy>,
    pub(crate) update_policy_score: Option<UPolicyScore>,
    /// Modify the return value from the number of new elements added, to the
    /// total number of elements changed (CH is an abbreviation of changed).
    /// Changed elements are new elements added and elements already existing
    /// for which the score was updated. So elements specified in the command
    /// line having the same score as they had in the past are not counted.
    /// Note: normally the return value of ZADD only counts the number of new
    /// elements added.
    pub return_change: bool,
    /// Increments instead of adding
    pub incr: bool,
}

impl IOption {
    /// Creates a new instance where only incr is set to true
    pub fn incr() -> Self {
        Self {
            incr: true,
            ..Default::default()
        }
    }
    /// Creates a new instance
    pub fn new(args: &mut VecDeque<Bytes>) -> Result<Self, Error> {
        let mut update_policy = None;
        let mut update_policy_score = None;
        let mut return_change = false;
        let mut incr = false;
        while let Some(t) = args.front() {
            let command = String::from_utf8_lossy(t);
            match command.to_uppercase().as_str() {
                "NX" => {
                    if update_policy == Some(IPolicy::XX) {
                        return Err(Error::OptsNotCompatible("XX AND NX".to_owned()));
                    }
                    update_policy = Some(IPolicy::NX);
                    args.pop_front();
                }
                "XX" => {
                    if update_policy == Some(IPolicy::NX) {
                        return Err(Error::OptsNotCompatible("XX AND NX".to_owned()));
                    }
                    update_policy = Some(IPolicy::XX);
                    args.pop_front();
                }
                "LT" => {
                    if update_policy == Some(IPolicy::NX)
                        || update_policy_score == Some(UPolicyScore::GT)
                    {
                        return Err(Error::OptsNotCompatible("GT, LT, and/or NX".to_owned()));
                    }
                    update_policy_score = Some(UPolicyScore::LT);
                    args.pop_front();
                }
                "GT" => {
                    if update_policy == Some(IPolicy::NX)
                        || update_policy_score == Some(UPolicyScore::LT)
                    {
                        return Err(Error::OptsNotCompatible("GT, LT, and/or NX".to_owned()));
                    }
                    update_policy_score = Some(UPolicyScore::GT);
                    args.pop_front();
                }
                "CH" => {
                    return_change = true;
                    args.pop_front();
                }
                "INCR" => {
                    incr = true;
                    args.pop_front();
                }
                _ => break,
            }
        }
        Ok(Self {
            insert_policy: update_policy,
            update_policy_score,
            return_change,
            incr,
        })
    }
}

/// Insert result
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IResult {
    /// No operation has taken place
    NoOp,
    /// A new element has been added
    Inserted,
    /// An element has been updated
    Updated,
}
