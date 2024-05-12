#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreDetails {
    Words(Words),
    Typo(Typo),
    Proximity(Rank),
    Exactness(ExactWords),
    // Sort(Sort),
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
    pub matching_words: u32,
    pub max_matching_words: u32,
}

impl ExactWords {
    pub fn rank(&self) -> Rank {
        // 0 matching words means last rank (1)
        Rank { rank: self.matching_words + 1, max_rank: self.max_matching_words + 1 }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        // last rank (1) means that 0 words from the query appear exactly in the document.
        // first rank (max_rank) means that (max_rank - 1) words from the query appear exactly in the document.
        Self {
            matching_words: rank.rank.saturating_sub(1),
            max_matching_words: rank.max_rank.saturating_sub(1),
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

    // max_rank = max_typo + 1
    // max_typo = max_rank - 1
    //
    // rank = max_typo - typo + 1
    // rank = max_rank - 1 - typo + 1
    // rank + typo = max_rank
    // typo = max_rank - rank
    pub fn from_rank(rank: Rank) -> Typo {
        Typo {
            typo_count: rank.max_rank.saturating_sub(rank.rank),
            max_typo_count: rank.max_rank.saturating_sub(1),
        }
    }
}