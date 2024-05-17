use std::cmp::Reverse;
use std::{fmt, slice};
use std::ops::{Deref, RangeInclusive};

use charabia::Token;
use itertools::Itertools;
use roaring::RoaringBitmap;
use analyzer::token::{BorrowedToken, OwnedToken};
use crate::search::context::Context;
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{DerivativeTerm, OriginalTerm, Term, TermKind};


pub struct LocatedMatchingPhrase {
    pub value: Vec<String>,
    pub positions: RangeInclusive<WordId>,
}

pub struct LocatedMatchingWord {
    pub value: String,
    pub positions: RangeInclusive<WordId>,
    pub is_prefix: bool,
}

/// Structure created from a query tree
/// referencing words that match the given query tree.
#[derive(Default)]
pub struct MatchingWords {
    phrases: Vec<LocatedMatchingPhrase>,
    words: Vec<LocatedMatchingWord>,
}

impl MatchingWords {
    pub fn new(ctx: impl Context, query_graph: QueryGraph) -> Self {
        let mut phrases = Vec::new();
        let mut words = Vec::new();

        // Extract and centralize the different phrases and words to match stored in a QueryTerm
        // and wrap them in dedicated structures.
        for node in query_graph.nodes {
            resolve_matching(node, &mut phrases, &mut words);
        }

        // Sort word to put prefixes at the bottom prioritizing the exact matches.
        words.sort_unstable_by_key(|word| (word.is_prefix, Reverse(word.positions.clone().count())));

        Self {
            phrases,
            words,
        }
    }

    /// Returns an iterator over terms that match or partially match the given token.
    pub fn match_token<'a, 'b>(&'a self, token: &'a OwnedToken<'b>) -> MatchesIter<'a, 'b> {
        MatchesIter { matching_words: self, phrases: self.phrases.iter(), token }
    }

    /// Try to match the token with one of the located_words.
    fn match_unique_words<'a, 'b>(&'a self, token: &'b OwnedToken<'_>) -> Option<MatchType<'a>> {
        for located_words in &self.words {
            let word = &located_words.value;

            if located_words.is_prefix && token.text.starts_with(word) {
                let Some((char_index, c)) =
                    word.char_indices().last()
                else {
                    continue;
                };
                let prefix_length = char_index + c.len_utf8();
                let char_len = token.original_lengths(prefix_length).0;
                let ids = &located_words.positions;
                return Some(MatchType::Full { char_len, ids });
            // else we exact match the token.
            } else if token.text == *word {
                let char_len = token.original_lengths(token.text.len()).0;
                let ids = &located_words.positions;
                return Some(MatchType::Full { char_len, ids });
            }
        }

        None
    }
}


fn resolve_matching(node: GraphNode, located_phrases: &mut Vec<LocatedMatchingPhrase>, located_words: &mut Vec<LocatedMatchingWord>){
    let NodeData::Term(term) = node.data else { return; };

    match term {
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Word(word)) |
        TermKind::Exact(OriginalTerm::Word(word)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word, ..), ..), position, ..
        } => {
            located_words.push(LocatedMatchingWord{
                value: word,
                positions: position,
                is_prefix: false,
            })
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Prefix(prefix)) |
        TermKind::Exact(OriginalTerm::Prefix(prefix)), position, ..
        } => {
            located_words.push(LocatedMatchingWord{
                value: prefix,
                positions: position,
                is_prefix: true,
            })
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Phrase(phrase)) |
        TermKind::Exact(OriginalTerm::Phrase(phrase)), position, ..
        } => {
            located_phrases.push(LocatedMatchingPhrase{
                value: phrase,
                positions: position,
            })
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Typo(words, ..) | DerivativeTerm::Synonym(words), ..) , position, .. } => {
            for word in words{
                located_words.push(LocatedMatchingWord{
                    value: word,
                    positions: position.clone(),
                    is_prefix: false,
                })
            }

        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(words, .. ), ..), position, .. } => {
            for word in words{
                located_phrases.push(LocatedMatchingPhrase{
                    value: vec![word.0, word.1],
                    positions: position.clone(),
                })
            }
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::SynonymPhrase(words, .. ), ..), position, .. } => {
            for word in words{
                located_phrases.push(LocatedMatchingPhrase{
                    value: word,
                    positions: position.clone(),
                })
            }
        },
        Term { term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _), position, .. } => {
            for prefix in prefixes{
                located_words.push(LocatedMatchingWord{
                    value: prefix,
                    positions: position.clone(),
                    is_prefix: true,
                })
            }
        }
    }
}

/// Iterator over terms that match the given token,
/// This allow to lazily evaluate matches.
pub struct MatchesIter<'a, 'b> {
    matching_words: &'a MatchingWords,
    phrases: slice::Iter<'a, LocatedMatchingPhrase>,
    token: &'a OwnedToken<'b>,
}

impl<'a, 'b> Iterator for MatchesIter<'a, 'b> {
    type Item = MatchType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.phrases.next() {
            // Try to match all the phrases first.
            Some(located_phrase) => {
                let phrase = located_phrase.value.iter().map(|x| x.deref()).collect();
                // create a PartialMatch struct to make it compute the first match
                // instead of duplicating the code.
                let ids = &located_phrase.positions;
                // collect the references of words from the interner.
                let partial = PartialMatch { matching_words: phrase, ids, char_len: 0 };

                partial.match_token(self.token).or_else(|| self.next())
            }
            // If no phrases matches, try to match uiques words.
            None => self.matching_words.match_unique_words(self.token),
        }
    }
}

/// Id of a matching term corespounding to a word written by the end user.
pub type WordId = usize;

/// A given token can partially match a query word for several reasons:
/// - split words
/// - multi-word synonyms
/// In these cases we need to match consecutively several tokens to consider that the match is full.
#[derive(Debug, PartialEq)]
pub enum MatchType<'a> {
    Full { char_len: usize, ids: &'a RangeInclusive<WordId> },
    Partial(PartialMatch<'a>),
}

/// Structure helper to match several tokens in a row in order to complete a partial match.
#[derive(Debug, PartialEq)]
pub struct PartialMatch<'a> {
    matching_words: Vec<&'a str>,
    ids: &'a RangeInclusive<WordId>,
    char_len: usize,
}

impl<'a> PartialMatch<'a> {
    /// Returns:
    /// - None if the given token breaks the partial match
    /// - Partial if the given token matches the partial match but doesn't complete it
    /// - Full if the given token completes the partial match
    pub fn match_token(self, token: &OwnedToken<'_>) -> Option<MatchType<'a>> {
        let Self { mut matching_words, ids, .. } = self;

        let is_matching = *matching_words.first()? == token.text;

        let char_len = token.original_lengths(token.text.len()).0;
        // if there are remaining words to match in the phrase and the current token is matching,
        // return a new Partial match allowing the highlighter to continue.
        if is_matching && matching_words.len() > 1 {
            matching_words.remove(0);
            Some(MatchType::Partial(PartialMatch { matching_words, ids, char_len }))
        // if there is no remaining word to match in the phrase and the current token is matching,
        // return a Full match.
        } else if is_matching {
            Some(MatchType::Full { char_len, ids })
        // if the current token doesn't match, return None to break the match sequence.
        } else {
            None
        }
    }

    pub fn char_len(&self) -> usize {
        self.char_len
    }
}

impl fmt::Debug for MatchingWords {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let MatchingWords { phrases, words } = self;

        let phrases: Vec<_> = phrases
            .iter()
            .map(|p| {
                (
                    p.value.join(" "),
                    p.positions.clone(),
                )
            })
            .collect();

        let words: Vec<_> = words
            .iter()
            .map(|w| {
                (&w.value, w.positions.clone(),w.is_prefix)
            })
            .collect();

        f.debug_struct("MatchingWords").field("phrases", &phrases).field("words", &words).finish()
    }
}

// #[cfg(test)]
// pub(crate) mod tests {
//     use std::borrow::Cow;
//
//     use charabia::{TokenKind, TokenizerBuilder};
//
//     use super::*;
//     use crate::index::tests::TempIndex;
//     use crate::search::search::SearchContext;
//
//     pub(crate) fn temp_index_with_documents() -> TempIndex {
//         let temp_index = TempIndex::new();
//         temp_index
//             .add_documents(documents!([
//                 { "id": 1, "name": "split this world westfali westfalia the Ŵôřlḑôle" },
//                 { "id": 2, "name": "Westfália" },
//                 { "id": 3, "name": "Ŵôřlḑôle" },
//             ]))
//             .unwrap();
//         temp_index
//     }
//
//     #[test]
//     fn matching_words() {
//         let temp_index = temp_index_with_documents();
//         let rtxn = temp_index.read_txn().unwrap();
//         let mut ctx = SearchContext::new(&temp_index, &rtxn, Default::default());
//         let mut builder = TokenizerBuilder::default();
//         let tokenizer = builder.build();
//         let tokens = tokenizer.tokenize("split this world");
//
//         let matching_words = MatchingWords::new(ctx, query_terms);
//
//         assert_eq!(
//             matching_words
//                 .match_token(&Token {
//                     kind: TokenKind::Word,
//                     lemma: Cow::Borrowed("split"),
//                     char_end: "split".chars().count(),
//                     byte_end: "split".len(),
//                     ..Default::default()
//                 })
//                 .next(),
//             Some(MatchType::Full { char_len: 5, ids: &(0..=0) })
//         );
//         assert_eq!(
//             matching_words
//                 .match_token(&Token {
//                     kind: TokenKind::Word,
//                     lemma: Cow::Borrowed("nyc"),
//                     char_end: "nyc".chars().count(),
//                     byte_end: "nyc".len(),
//                     ..Default::default()
//                 })
//                 .next(),
//             None
//         );
//         assert_eq!(
//             matching_words
//                 .match_token(&Token {
//                     kind: TokenKind::Word,
//                     lemma: Cow::Borrowed("world"),
//                     char_end: "world".chars().count(),
//                     byte_end: "world".len(),
//                     ..Default::default()
//                 })
//                 .next(),
//             Some(MatchType::Full { char_len: 5, ids: &(2..=2) })
//         );
//         assert_eq!(
//             matching_words
//                 .match_token(&Token {
//                     kind: TokenKind::Word,
//                     lemma: Cow::Borrowed("worlded"),
//                     char_end: "worlded".chars().count(),
//                     byte_end: "worlded".len(),
//                     ..Default::default()
//                 })
//                 .next(),
//             Some(MatchType::Full { char_len: 5, ids: &(2..=2) })
//         );
//         assert_eq!(
//             matching_words
//                 .match_token(&Token {
//                     kind: TokenKind::Word,
//                     lemma: Cow::Borrowed("thisnew"),
//                     char_end: "thisnew".chars().count(),
//                     byte_end: "thisnew".len(),
//                     ..Default::default()
//                 })
//                 .next(),
//             None
//         );
//     }
// }
