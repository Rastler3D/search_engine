use std::collections::BTreeSet;
use analyzer::token::BorrowedToken;
use crate::search::query_parser::{OriginalTerm, Term, TermKind};

pub struct PhraseBuilder {
    words: Vec<String>,
    pub is_negative: bool,
    start: usize,
    end: usize,
}

impl PhraseBuilder {
    pub fn empty(is_negative: bool) -> Self {
        Self { words: Default::default(), is_negative, start: usize::MAX, end: usize::MAX }
    }

    fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    // precondition: token has kind Word or StopWord
    pub fn push_word(&mut self, word: String , position: usize) {
        if self.is_empty() {
            self.start = position;
        }
        self.end = position;
        self.words.push(word);
    }

    pub fn build(self) -> Option<Term> {
        if self.is_empty() {
            return None;
        }
        Some(Term{
            term_kind: TermKind::Exact(OriginalTerm::Phrase(self.words)),
            is_negative: self.is_negative,
            position: self.start..=self.end,

        })
    }
}