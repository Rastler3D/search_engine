use std::ops::RangeInclusive;
use analyzer::token::{SeparatorKind, TokenFlags, TokenKind};
use analyzer::tokenizer::token_stream::TokenStream;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::phrase_builder::PhraseBuilder;
use crate::search::utils::vec_map::VecMap;


pub fn parse_query<'a>(mut token_stream: impl TokenStream<'a>) -> Vec<Term>{
    let mut query_terms = Vec::new();
    let mut phrase: Option<PhraseBuilder> = None;
    let mut negative_term = false;

    let mut position = usize::MAX;
    let mut peekable = token_stream.as_iter().take(100).peekable();
    while let Some(token) = peekable.next() {
        if token.text.is_empty() {
            continue;
        }

        let mut is_negative = if negative_term{
            negative_term = false;
            true
        } else {
            false
        };

        match token.token_kind {
            TokenKind::Word(flags) => {
                position = position.wrapping_add(1);

                if let Some(phrase) = &mut phrase {
                    phrase.push_word(token.text, position)
                } else {
                    let term = if flags.contains(TokenFlags::Prefix) || peekable.peek().is_none(){
                        OriginalTerm::Prefix(token.text)
                    } else {
                        OriginalTerm::Word(token.text)
                    };
                    let term = Term {
                        term_kind: if flags.contains(TokenFlags::Exact) {
                            TermKind::Exact(term)
                        } else { TermKind::Normal(term) },
                        is_negative: is_negative,
                        position: position..=position
                    };

                    query_terms.push(term);
                }
            }
            TokenKind::Separator(separator_kind) => {
                match separator_kind {
                    SeparatorKind::Soft => continue,
                    SeparatorKind::Hard => {
                        position = position.wrapping_add(7);
                        phrase = if let Some(phrase) = phrase.take() {
                            is_negative = phrase.is_negative;

                            if let Some(term) = phrase.build() {
                                query_terms.push(term);
                            }
                            Some(PhraseBuilder::empty(is_negative))
                        } else { None };
                    },
                    SeparatorKind::PhraseQuote => {
                        phrase = if let Some(phrase) = phrase.take() {
                            if let Some(term) = phrase.build() {
                                query_terms.push(term);
                            }
                            None
                        } else {
                            Some(PhraseBuilder::empty(is_negative))
                        }
                    }
                    SeparatorKind::Negative => {
                        negative_term = phrase.is_none();
                    }
                }
            }
        }
    }

    if let Some(phrase) = phrase.take() {
        if let Some(term) = phrase.build() {
            query_terms.push(term);
        }
    }

    query_terms
}

#[derive(Clone, Debug)]
pub struct Term{
    pub term_kind: TermKind,
    pub is_negative: bool,
    pub position: RangeInclusive<usize>,
}

#[derive(Clone, Debug)]
pub enum DerivativeTerm {
    Ngram(String, u8),
    Synonym(Vec<String>),
    PrefixTypo(Vec<String>, u8),
    Typo(Vec<String>, u8),
    SynonymPhrase(Vec<Vec<String>>),
    Split(Vec<(String, String)>)
}

#[derive(Clone,Debug)]
pub enum OriginalTerm{
    Word(String),
    Prefix(String),
    Phrase(Vec<String>),
}

#[derive(Clone,Debug)]
pub enum TermKind{
    Derivative(DerivativeTerm, usize),
    Exact(OriginalTerm),
    Normal(OriginalTerm)
}


#[cfg(test)]
pub mod tests {
    use analyzer::analyzer::{Analyzer};
    use analyzer::char_filter::character_filter_layer::{self, CharacterFilterLayers};
    use analyzer::char_filter::regex_character_filter::RegexCharacterFilter;
    use analyzer::language_detection::whichlang::WhichLangDetector;
    use analyzer::token_filter::lower_case::LowerCaseFilter;
    use analyzer::token_filter::token_filter_layer::{self, TokenFilterLayers};
    use analyzer::tokenizer::whitespace_tokenizer::WhitespaceTokenizer;
    use regex::Regex;
    use analyzer::analyzer::text_analyzer::TextAnalyzer;
    use super::*;

    pub fn build_analyzer() -> impl Analyzer{
        let token_filters = token_filter_layer::BaseLevel.wrap_layer(LowerCaseFilter {});
        let tokenizer = WhitespaceTokenizer {};
        let character_filters = character_filter_layer::BaseLevel.wrap_layer(RegexCharacterFilter {
            pattern: Regex::new("ello").unwrap(),
            replacement: "Hello".to_string()
        });

        let mut analyzer = TextAnalyzer {
            character_filters: character_filters,
            language_detector: WhichLangDetector{},
            tokenizer: tokenizer,
            token_filters: token_filters,
        };

        analyzer
    }
    #[test]
    fn test_parse() {

        let analyzer = build_analyzer();
        let mut stream = analyzer.analyze("Hello WORLD WORLD HELlo");

        let parsed = parse_query(stream);

        println!("{:#?}", parsed);

    }
}