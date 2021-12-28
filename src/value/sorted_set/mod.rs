//! # Sorted Set module
use bytes::Bytes;
use float_ord::FloatOrd;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    ops::Bound,
};

mod insert;

pub use insert::{IOption, IResult};
use insert::{IPolicy, UPolicyScore};

/// Sorted set structure
#[derive(Debug, Clone)]
pub struct SortedSet {
    set: HashMap<Bytes, (FloatOrd<f64>, usize)>,
    order: BTreeMap<(FloatOrd<f64>, Bytes), usize>,
}

impl PartialEq for SortedSet {
    fn eq(&self, other: &SortedSet) -> bool {
        self.order == other.order
    }
}

impl SortedSet {
    /// Creates a new instance
    pub fn new() -> Self {
        Self {
            set: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    /// Clears the map, removing all elements.
    pub fn clear(&mut self) {
        self.set.clear();
        self.order.clear();
    }

    /// Returns the number of elements in the set
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Adds a value to the set.
    /// If the set did not have this value present, true is returned.
    ///
    /// If the set did have this value present, false is returned.
    pub fn insert(&mut self, score: FloatOrd<f64>, value: Bytes, option: &IOption) -> IResult {
        if let Some((current_score, _)) = self.set.get(&value).cloned() {
            if option.insert_policy == Some(IPolicy::NX) {
                return IResult::NoOp;
            }
            let cmp = current_score.cmp(&score);
            let update_based_on_score =
                option
                    .update_policy_score
                    .map_or(true, |policy| match policy {
                        UPolicyScore::LT => cmp == std::cmp::Ordering::Less,
                        UPolicyScore::GT => cmp == std::cmp::Ordering::Greater,
                    });

            if !update_based_on_score {
                return IResult::NoOp;
            }
            // remove the previous order entry
            self.order.remove(&(current_score, value.clone()));

            let score = if option.incr {
                FloatOrd(current_score.0 + score.0)
            } else {
                score
            };

            // update and insert the new order entry
            self.set.insert(value.clone(), (score, 0));
            self.order.insert((score, value), 0);

            self.update_value_position();
            IResult::Updated
        } else {
            if option.insert_policy == Some(IPolicy::XX) {
                return IResult::NoOp;
            }
            self.set.insert(value.clone(), (score, 0));
            self.order.insert((score, value), 0);
            self.update_value_position();
            IResult::Inserted
        }
    }

    /// Returns a reference to the score in the set, if any, that is equal to the given value.
    pub fn get_score(&self, value: &Bytes) -> Option<FloatOrd<f64>> {
        self.set.get(value).map(|(value, _)| *value)
    }

    /// Returns all the values sorted by their score
    pub fn get_values(&self) -> Vec<Bytes> {
        self.order.keys().map(|(_, value)| value.clone()).collect()
    }

    #[inline]
    fn convert_to_range(
        min: Bound<FloatOrd<f64>>,
        max: Bound<FloatOrd<f64>>,
    ) -> (Bound<(FloatOrd<f64>, Bytes)>, Bound<(FloatOrd<f64>, Bytes)>) {
        let min_bytes = Bytes::new();
        let max_bytes = Bytes::copy_from_slice(&vec![255u8; 4096]);

        (
            match min {
                Bound::Included(value) => Bound::Included((value, min_bytes.clone())),
                Bound::Excluded(value) => Bound::Excluded((value, max_bytes.clone())),
                Bound::Unbounded => Bound::Unbounded,
            },
            match max {
                Bound::Included(value) => Bound::Included((value, max_bytes)),
                Bound::Excluded(value) => Bound::Excluded((value, min_bytes)),
                Bound::Unbounded => Bound::Unbounded,
            },
        )
    }

    /// Get total number of values in a score range
    pub fn count_values_by_score_range(
        &self,
        min: Bound<FloatOrd<f64>>,
        max: Bound<FloatOrd<f64>>,
    ) -> usize {
        self.order.range(Self::convert_to_range(min, max)).count()
    }

    /// Get values in a score range
    pub fn get_values_by_score_range(
        &self,
        min: Bound<FloatOrd<f64>>,
        max: Bound<FloatOrd<f64>>,
    ) -> Vec<Bytes> {
        self.order
            .range(Self::convert_to_range(min, max))
            .map(|(k, _)| k.1.clone())
            .collect()
    }

    /// Adds the position in the set to each value based on their score
    #[inline]
    fn update_value_position(&mut self) {
        let mut i = 0;
        for ((_, key), value) in self.order.iter_mut() {
            *value = i;
            if let Some(value) = self.set.get_mut(key) {
                value.1 = i;
            }
            i += 1;
        }
    }

    /// Return the position into the set based on their score
    pub fn get_value_pos(&self, value: &Bytes) -> Option<usize> {
        Some(self.set.get(value)?.1)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_usage() {
        let mut set: SortedSet = SortedSet::new();
        let mut op = IOption::default();
        op.insert_policy = Some(IPolicy::NX);

        assert_eq!(
            set.insert(FloatOrd(1.0), "2".into(), &op),
            IResult::Inserted
        );
        assert_eq!(
            set.insert(FloatOrd(0.0), "3".into(), &op),
            IResult::Inserted
        );
        assert_eq!(set.insert(FloatOrd(33.1), "3".into(), &op), IResult::NoOp);

        op.insert_policy = None;
        op.incr = true;
        assert_eq!(set.insert(FloatOrd(2.0), "2".into(), &op), IResult::Updated);

        //assert_eq!(vec![3, 2], set.get_values());
        assert_eq!(Some(FloatOrd(3.0)), set.get_score(&"2".into()));
        assert_eq!(Some(1), set.get_value_pos(&"2".into()));
        assert_eq!(Some(0), set.get_value_pos(&"3".into()));
        assert_eq!(None, set.get_value_pos(&"5".into()));
    }
}
