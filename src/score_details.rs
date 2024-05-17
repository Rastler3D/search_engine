use std::cmp::Ordering;

use itertools::Itertools;
use serde::Serialize;

use crate::{ distance_between_two_points};

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreDetails {
    Words(Words),
    Typo(Typo),
    Proximity(Proximity),
    Exactness(ExactWords),
    Attribute(Attribute),
    Sort(Sort),
    Vector(Vector),
}

#[derive(Clone, Copy)]
pub enum ScoreValue<'a> {
    Score(f64),
    Sort(&'a Sort),
}

enum RankOrValue<'a> {
    Rank(Rank),
    Sort(&'a Sort),
    Score(f64),
}

impl ScoreDetails {
    pub fn local_score(&self) -> Option<f64> {
        self.rank().map(Rank::local_score)
    }

    pub fn rank(&self) -> Option<Rank> {
        match self {
            ScoreDetails::Words(details) => Some(details.rank()),
            ScoreDetails::Typo(details) => Some(details.rank()),
            ScoreDetails::Proximity(details) => Some(details.rank()),
            ScoreDetails::Attribute(details) => Some(details.rank()),
            ScoreDetails::Exactness(details) => Some(details.rank()),
            ScoreDetails::Sort(_) => None,
            ScoreDetails::Vector(_) => None,
        }
    }

    pub fn global_score<'a>(details: impl Iterator<Item = &'a Self> + 'a) -> f64 {
        Self::score_values(details)
            .find_map(|x| {
                let ScoreValue::Score(score) = x else {
                    return None;
                };
                Some(score)
            })
            .unwrap_or(1.0f64)
    }

    pub fn score_values<'a>(
        details: impl Iterator<Item = &'a Self> + 'a,
    ) -> impl Iterator<Item = ScoreValue<'a>> + 'a {
        details
            .map(ScoreDetails::rank_or_value)
            .coalesce(|left, right| match (left, right) {
                (RankOrValue::Rank(left), RankOrValue::Rank(right)) => {
                    Ok(RankOrValue::Rank(Rank::merge(left, right)))
                }
                (left, right) => Err((left, right)),
            })
            .map(|rank_or_value| match rank_or_value {
                RankOrValue::Rank(r) => ScoreValue::Score(r.local_score()),
                RankOrValue::Sort(s) => ScoreValue::Sort(s),
                RankOrValue::Score(s) => ScoreValue::Score(s),
            })
    }

    fn rank_or_value(&self) -> RankOrValue<'_> {
        match self {
            ScoreDetails::Words(w) => RankOrValue::Rank(w.rank()),
            ScoreDetails::Typo(t) => RankOrValue::Rank(t.rank()),
            ScoreDetails::Proximity(p) => RankOrValue::Rank(p.rank()),
            ScoreDetails::Attribute(f) => RankOrValue::Rank(f.rank()),
            ScoreDetails::Exactness(e) => RankOrValue::Rank(e.rank()),
            ScoreDetails::Sort(sort) => RankOrValue::Sort(sort),
            ScoreDetails::Vector(vector) => {
                RankOrValue::Score(vector.similarity.as_ref().map(|s| *s as f64).unwrap_or(0.0f64))
            }
        }
    }

    pub fn to_json_map<'a>(
        details: impl Iterator<Item = &'a Self>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut order = 0;
        let mut details_map = serde_json::Map::default();
        for details in details {
            match details {
                ScoreDetails::Words(words) => {
                    let words_details = serde_json::json!({
                            "order": order,
                            "matchingWords": words.matching_words,
                            "maxMatchingWords": words.max_matching_words,
                            "score": words.rank().local_score(),
                    });
                    details_map.insert("words".into(), words_details);
                    order += 1;
                }
                ScoreDetails::Typo(typo) => {
                    let typo_details = serde_json::json!({
                        "order": order,
                        "typoCount": typo.typo_count,
                        "maxTypoCount": typo.max_typo_count,
                        "score": typo.rank().local_score(),
                    });
                    details_map.insert("typo".into(), typo_details);
                    order += 1;
                }
                ScoreDetails::Proximity(proximity) => {
                    let proximity_details = serde_json::json!({
                        "order": order,
                        "proximity": proximity.proximity,
                        "maxProximity": proximity.max_proximity,
                        "score": proximity.rank().local_score(),
                    });
                    details_map.insert("proximity".into(), proximity_details);
                    order += 1;
                }
                ScoreDetails::Attribute(attribute) => {
                    let fid_details = serde_json::json!({
                        "order": order,
                        "attribute": attribute.attribute,
                        "maxAttribute": attribute.max_attribute,
                        "score": attribute.rank().local_score(),
                    });
                    details_map.insert("attribute".into(), fid_details);
                    order += 1;
                }
                ScoreDetails::Exactness(exact_words) => {
                    let exactness_details = serde_json::json!({
                        "order": order,
                        "exactWords": exact_words.exact_words,
                        "maxExactWords": exact_words.max_exact_words,
                        "score": exact_words.rank().local_score(),
                    });
                    details_map.insert("exactness".into(), exactness_details);
                    order += 1;
                }
                ScoreDetails::Sort(details) => {
                    let sort = format!("{}:{}", details.field_name, if details.ascending { "asc" } else { "desc" });
                    let value = details.value.clone() ;
                    let sort_details = serde_json::json!({
                        "order": order,
                        "value": value,
                    });
                    details_map.insert(sort, sort_details);
                    order += 1;
                }
                ScoreDetails::Vector(s) => {
                    let similarity = s.similarity.as_ref();

                    let details = serde_json::json!({
                        "order": order,
                        "similarity": similarity,
                    });
                    details_map.insert("vectorSort".into(), details);
                    order += 1;
                }
            }
        }
        details_map
    }
}



#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Words {
    pub matching_words: u32,
    pub max_matching_words: u32,
}

impl Words {
    pub fn rank(&self) -> Rank {
        Rank { rank: self.matching_words, max_rank: self.max_matching_words }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        Self { matching_words: rank.rank, max_matching_words: rank.max_rank }
    }
}

/// Structure that is super similar to [`Words`], but whose semantics is a bit distinct.
///
/// In exactness, the number of matching words can actually be 0 with a non-zero score,
/// if no words from the query appear exactly in the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExactWords {
    pub exact_words: u32,
    pub max_exact_words: u32,
}

impl ExactWords {
    pub fn rank(&self) -> Rank {
        // 0 matching words means last rank (1)
        Rank { rank: self.exact_words + 1, max_rank: self.max_exact_words + 1 }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        // last rank (1) means that 0 words from the query appear exactly in the document.
        // first rank (max_rank) means that (max_rank - 1) words from the query appear exactly in the document.
        Self {
            exact_words: rank.rank.saturating_sub(1),
            max_exact_words: rank.max_rank.saturating_sub(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Proximity {
    pub proximity: u32,
    pub max_proximity: u32,
}

impl Proximity {
    pub fn rank(&self) -> Rank {
        Rank {
            rank: (self.max_proximity + 1).saturating_sub(self.proximity),
            max_rank: (self.max_proximity + 1),
        }
    }

    pub fn from_rank(rank: Rank) -> Proximity {
        Proximity {
            proximity: rank.max_rank.saturating_sub(rank.rank),
            max_proximity: rank.max_rank.saturating_sub(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Typo {
    pub typo_count: u32,
    pub max_typo_count: u32,
}

impl Typo {
    pub fn rank(&self) -> Rank {
        Rank {
            rank: (self.max_typo_count + 1).saturating_sub(self.typo_count),
            max_rank: (self.max_typo_count + 1),
        }
    }

    pub fn from_rank(rank: Rank) -> Typo {
        Typo {
            typo_count: rank.max_rank.saturating_sub(rank.rank),
            max_typo_count: rank.max_rank.saturating_sub(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attribute {
    pub attribute: u32,
    pub max_attribute: u32,
}

impl Attribute {
    pub fn rank(&self) -> Rank {
        Rank { rank: self.attribute + 1, max_rank: self.max_attribute + 1 }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        Self {
            attribute: rank.rank.saturating_sub(1),
            max_attribute: rank.max_rank.saturating_sub(1),
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rank {
    /// The ordinal rank, such that `max_rank` is the first rank, and 0 is the last rank.
    ///
    /// The higher the better. Documents with a rank of 0 have a score of 0 and are typically never returned
    /// (they don't match the query).
    pub rank: u32,
    /// The maximum possible rank. Documents with this rank have a score of 1.
    ///
    /// The max rank should not be 0.
    pub max_rank: u32,
}

impl Rank {
    pub fn local_score(self) -> f64 {
        self.rank as f64 / self.max_rank as f64
    }

    pub fn global_score(details: impl Iterator<Item = Self>) -> f64 {
        let mut rank = Rank { rank: 1, max_rank: 1 };
        for inner_rank in details {
            rank = Rank::merge(rank, inner_rank);
        }
        rank.local_score()
    }

    pub fn merge(mut outer: Rank, inner: Rank) -> Rank {
        outer.rank = outer.rank.saturating_sub(1);

        outer.rank *= inner.max_rank;
        outer.max_rank *= inner.max_rank;

        outer.rank += inner.rank;

        outer
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub field_name: String,
    pub ascending: bool,
    pub value: serde_json::Value,
}

impl PartialOrd for Sort {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.field_name != other.field_name {
            return None;
        }
        if self.ascending != other.ascending {
            return None;
        }
        match (&self.value, &other.value) {
            (serde_json::Value::Null, serde_json::Value::Null) => Some(Ordering::Equal),
            (serde_json::Value::Null, _) => Some(Ordering::Less),
            (_, serde_json::Value::Null) => Some(Ordering::Greater),
            // numbers are always before strings
            (serde_json::Value::Number(_), serde_json::Value::String(_)) => Some(Ordering::Greater),
            (serde_json::Value::String(_), serde_json::Value::Number(_)) => Some(Ordering::Less),
            (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
                // FIXME: unwrap permitted here?
                let order = left.as_f64().unwrap().partial_cmp(&right.as_f64().unwrap())?;
                // 12 < 42, and when ascending, we want to see 12 first, so the smallest.
                // Hence, when ascending, smaller is better
                Some(if self.ascending { order.reverse() } else { order })
            }
            (serde_json::Value::String(left), serde_json::Value::String(right)) => {
                let order = left.cmp(right);
                // Taking e.g. "a" and "z"
                // "a" < "z", and when ascending, we want to see "a" first, so the smallest.
                // Hence, when ascending, smaller is better
                Some(if self.ascending { order.reverse() } else { order })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Vector {
    pub similarity: Option<f32>,
}

