//! # Sorted Set module
use std::{
    collections::{btree_map::Iter, BTreeMap, HashMap},
    hash::Hash,
};

/// Sorted set structure
#[derive(Debug, Clone)]
pub struct SortedSet<S: Clone + PartialEq + Ord, V: Clone + Eq + Hash> {
    set: HashMap<V, (S, usize)>,
    order: BTreeMap<S, V>,
    position_updated: bool,
}

impl<S: PartialEq + Clone + Ord, V: Eq + Clone + Hash> PartialEq for SortedSet<S, V> {
    fn eq(&self, other: &SortedSet<S, V>) -> bool {
        self.order == other.order
    }
}

impl<S: PartialEq + Clone + Ord, V: Eq + Clone + Hash> SortedSet<S, V> {
    /// Creates a new instance
    pub fn new() -> Self {
        Self {
            set: HashMap::new(),
            order: BTreeMap::new(),
            position_updated: true,
        }
    }

    /// Clears the map, removing all elements.
    pub fn clear(&mut self) {
        self.set.clear();
        self.order.clear();
    }

    /// Gets an iterator over the entries of the map, sorted by score.
    pub fn iter(&self) -> Iter<'_, S, V> {
        self.order.iter()
    }

    /// Adds a value to the set.
    /// If the set did not have this value present, true is returned.
    ///
    /// If the set did have this value present, false is returned.
    pub fn insert(&mut self, score: S, value: &V) -> bool {
        if self.set.get(value).is_none() {
            self.set.insert(value.clone(), (score.clone(), 0));
            self.order.insert(score, value.clone());
            self.position_updated = false;
            true
        } else {
            false
        }
    }

    /// Returns a reference to the score in the set, if any, that is equal to the given value.
    pub fn get_score(&self, value: &V) -> Option<&S> {
        self.set.get(value).map(|(value, _)| value)
    }

    /// Returns all the values sorted by their score
    pub fn get_values(&self) -> Vec<V> {
        self.order.values().cloned().collect()
    }

    /// Adds the position in the set to each value based on their score
    fn update_value_position(&mut self) {
        let mut i = 0;
        for element in self.order.values() {
            if let Some(value) = self.set.get_mut(element) {
                value.1 = i;
            }
            i += 1;
        }
        self.position_updated = true;
    }

    /// Return the position into the set based on their score
    pub fn get_value_pos(&mut self, value: &V) -> Option<usize> {
        if self.position_updated {
            Some(self.set.get(value)?.1)
        } else {
            self.update_value_position();
            Some(self.set.get(value)?.1)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_usage() {
        let mut set: SortedSet<i64, i64> = SortedSet::new();
        assert!(set.insert(1, &2));
        assert!(set.insert(0, &3));
        assert!(!set.insert(33, &3));
        assert_eq!(vec![3, 2], set.get_values());
        assert_eq!(Some(1), set.get_value_pos(&2));
        assert_eq!(Some(0), set.get_value_pos(&3));
        assert_eq!(None, set.get_value_pos(&5));
    }
}
