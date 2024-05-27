use std::collections::HashSet;
use crate::search::ranking::path_visitor::Edge;
use crate::search::utils::bit_set::BitSet;

pub struct DeadEndsCache {
    // conditions and next could/should be part of the same vector
    conditions: Vec<Edge>,
    next: Vec<Self>,
    pub forbidden: HashSet<Edge>,
}
impl Clone for DeadEndsCache {
    fn clone(&self) -> Self {
        Self {
            conditions: self.conditions.clone(),
            next: self.next.clone(),
            forbidden: self.forbidden.clone(),
        }
    }
}
impl DeadEndsCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            conditions: vec![],
            next: vec![],
            forbidden: HashSet::with_capacity(capacity),
        }
    }
    pub fn forbid_condition(&mut self, edge: Edge) {
        self.forbidden.insert(edge);
    }

    fn advance(&mut self, condition: Edge) -> Option<&mut Self> {
        if let Some(idx) = self.conditions.iter().position(|c| *c == condition) {
            Some(&mut self.next[idx])
        } else {
            None
        }
    }
    pub fn forbidden_conditions_for_all_prefixes_up_to(
        &mut self,
        prefix: impl Iterator<Item = Edge>,
    ) -> HashSet<Edge> {
        let mut forbidden = self.forbidden.clone();
        let mut cursor = self;
        for c in prefix {
            if let Some(next) = cursor.advance(c) {
                cursor = next;
                forbidden.extend(cursor.forbidden.iter().copied());
            } else {
                break;
            }
        }
        forbidden
    }
    pub fn forbidden_conditions_after_prefix(
        &mut self,
        prefix: impl Iterator<Item = Edge>,
    ) -> Option<&HashSet<Edge>> {
        let mut cursor = self;
        for c in prefix {
            if let Some(next) = cursor.advance(c) {
                cursor = next;
            } else {
                return None;
            }
        }
        Some(&cursor.forbidden)
    }
    pub fn forbid_condition_after_prefix(
        &mut self,
        mut prefix: impl Iterator<Item = Edge>,
        forbidden: Edge,
    ) {
        match prefix.next() {
            None => {
                self.forbidden.insert(forbidden);
            }
            Some(first_condition) => {
                if let Some(idx) = self.conditions.iter().position(|c| *c == first_condition) {
                    return self.next[idx].forbid_condition_after_prefix(prefix, forbidden);
                }
                let mut rest = DeadEndsCache {
                    conditions: vec![],
                    next: vec![],
                    forbidden: HashSet::with_capacity(self.forbidden.len()),
                };
                rest.forbid_condition_after_prefix(prefix, forbidden);
                self.conditions.push(first_condition);
                self.next.push(rest);
            }
        }
    }

    // pub fn debug_print(&self, indent: usize) {
    //     println!("{} {:?}", " ".repeat(indent), self.forbidden.iter().collect::<Vec<_>>());
    //     for (condition, next) in self.conditions.iter().zip(self.next.iter()) {
    //         println!("{} {condition}:", " ".repeat(indent));
    //         next.debug_print(indent + 2);
    //     }
    // }
}
