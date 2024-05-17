use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{DerivativeTerm, OriginalTerm, Term, TermKind};
use crate::search::utils::bit_set::BitSet;

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.term_kind {
            TermKind::Derivative(term, orig_idx) => {
                match term {
                    DerivativeTerm::Ngram(text, ngrams) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {text:?}")?;
                        writeln!(f, "\t kind: Derivation")?;
                        writeln!(f, "\t type: Ngram")?;
                        writeln!(f, "\t ngrams: {ngrams}")?;
                        writeln!(f, "\t original_term: {orig_idx}")?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")
                    }
                    DerivativeTerm::Synonym(texts) => {
                        writeln!(f, "Synonyms {{\n")?;
                        for (field, text) in texts.iter().enumerate(){
                            writeln!(f, "\t {field}: {{")?;
                            writeln!(f, "\t\t  label: {text:?}")?;
                            writeln!(f, "\t\t  kind: Derivation")?;
                            writeln!(f, "\t\t  type: Synonym")?;
                            writeln!(f, "\t\t  original_term: {orig_idx}")?;
                            writeln!(f, "\t\t  is_negative: {}", self.is_negative)?;
                            writeln!(f, "\t\t  position: {}..{}", self.position.start(), self.position.end())?;
                            writeln!(f, "\t\t  shape: class")?;
                            writeln!(f, "\t }}")?;
                        }
                        writeln!(f, "}}")

                    }
                    DerivativeTerm::PrefixTypo(texts, typos) => {
                        writeln!(f, "Prefix {typos} typos {{\n")?;
                        for (field, text) in texts.iter().enumerate(){
                            writeln!(f, "\t {field}: {{")?;
                            writeln!(f, "\t\t  label: {text:?}")?;
                            writeln!(f, "\t\t  kind: Derivation")?;
                            writeln!(f, "\t\t  type: Prefix Typo")?;
                            writeln!(f, "\t\t  typos: {typos}")?;
                            writeln!(f, "\t\t  original_term: {orig_idx}")?;
                            writeln!(f, "\t\t  is_negative: {}", self.is_negative)?;
                            writeln!(f, "\t\t  position: {}..{}", self.position.start(), self.position.end())?;
                            writeln!(f, "\t\t  shape: class")?;
                            writeln!(f, "\t }}")?;
                        }
                        writeln!(f, "}}")

                    }
                    DerivativeTerm::Typo(texts, typos) => {
                        writeln!(f, "Word {typos} typos {{\n")?;
                        for (field, text) in texts.iter().enumerate(){
                            writeln!(f, "\t {field}: {{")?;
                            writeln!(f, "\t\t  label: {text:?}")?;
                            writeln!(f, "\t\t  kind: Derivation")?;
                            writeln!(f, "\t\t  type: Word Typo")?;
                            writeln!(f, "\t\t  typos: {typos}")?;
                            writeln!(f, "\t\t  original_term: {orig_idx}")?;
                            writeln!(f, "\t\t  is_negative: {}", self.is_negative)?;
                            writeln!(f, "\t\t  position: {}..{}", self.position.start(), self.position.end())?;
                            writeln!(f, "\t\t  shape: class")?;
                            writeln!(f, "\t }}")?;
                        }
                        writeln!(f, "}}")
                    }
                    DerivativeTerm::SynonymPhrase(phrases) => {
                        writeln!(f, "Synonym phrases{{\n")?;
                        for (field, phrase) in phrases.iter().enumerate(){
                            writeln!(f, "\t {field}: {{")?;
                            writeln!(f, "\t\t  label: {:?}", phrase.join(" "))?;
                            writeln!(f, "\t\t  kind: Derivation")?;
                            writeln!(f, "\t\t  type: Synonym Phrase")?;
                            writeln!(f, "\t\t  original_term: {orig_idx}")?;
                            writeln!(f, "\t\t  is_negative: {}", self.is_negative)?;
                            writeln!(f, "\t\t  position: {}..{}", self.position.start(), self.position.end())?;
                            writeln!(f, "\t\t  shape: class")?;
                            writeln!(f, "\t }}")?;
                        }
                        writeln!(f, "}}")
                    }
                    DerivativeTerm::Split(splits) => {
                        writeln!(f, "Splits {{\n")?;
                        for (field, split) in splits.iter().enumerate(){
                            writeln!(f, "\t {field}: {{")?;
                            writeln!(f, "\t\t  label: {:?}",format!("{} {}", split.0, split.1))?;
                            writeln!(f, "\t\t  kind: Derivation")?;
                            writeln!(f, "\t\t  type: Split")?;
                            writeln!(f, "\t\t  original_term: {orig_idx}")?;
                            writeln!(f, "\t\t  is_negative: {}", self.is_negative)?;
                            writeln!(f, "\t\t  position: {}..{}", self.position.start(), self.position.end())?;
                            writeln!(f, "\t\t  shape: class")?;
                            writeln!(f, "\t }}")?;
                        }
                        writeln!(f, "}}")
                    }
                }
            }
            TermKind::Exact(term) => {
                match term {
                    OriginalTerm::Word(text) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {text:?}")?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Word")?;
                        writeln!(f, "\t is_exact: {}", true)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")

                    }
                    OriginalTerm::Prefix(text) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {text:?}")?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Prefix")?;
                        writeln!(f, "\t is_exact: {}", true)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")
                    }
                    OriginalTerm::Phrase(phrase) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {:?}", phrase.join(" "))?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Phrase")?;
                        writeln!(f, "\t is_exact: {}", true)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")
                    }
                }
            }
            TermKind::Normal(term) => {
                match term {
                    OriginalTerm::Word(text) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {text:?}")?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Word")?;
                        writeln!(f, "\t is_exact: {}", false)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")

                    }
                    OriginalTerm::Prefix(text) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {text:?}")?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Prefix")?;
                        writeln!(f, "\t is_exact: {}", false)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")
                    }
                    OriginalTerm::Phrase(phrase) => {
                        writeln!(f, "{{")?;
                        writeln!(f, "\t label: {:?}", phrase.join(" "))?;
                        writeln!(f, "\t kind: Original")?;
                        writeln!(f, "\t type: Phrase")?;
                        writeln!(f, "\t is_exact: {}", false)?;
                        writeln!(f, "\t is_negative: {}", self.is_negative)?;
                        writeln!(f, "\t position: {}..{}", self.position.start(), self.position.end())?;
                        writeln!(f, "\t shape: class")?;
                        writeln!(f, "}}")
                    }
                }
            }
        }

    }
}

impl Display for NodeData{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeData::Start => {
                writeln!(f, "START")
            }
            NodeData::Term(term) => {
                term.fmt(f)
            }
            NodeData::End => {
                writeln!(f, "END")
            }
        }
    }
}

impl Display for QueryGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "direction: right")?;
        for (node_id, node) in self.nodes.iter().enumerate(){
            writeln!(f, "{node_id}: {}", node.data)?;
            for successor_id in node.successors.iter(){
                writeln!(f, "{node_id} -> {successor_id}")?;
            }
        }

        Ok(())
    }
}
