use std::borrow::Cow;
use std::collections::HashMap;
use fst::Set;
use roaring::RoaringBitmap;
use crate::search::ranking::criteria::Criterion;
use crate::search::search::SearchContext;
use crate::search::split_config::SplitConfig;
use crate::search::typo_config::TypoConfig;
use crate::Result;
use crate::search::query_graph::QueryGraph;
use crate::search::utils::bit_set::BitSet;
use crate::update::split_config::SplitJoinConfig;

pub type Fid = u16;
pub type Position = u16;
pub trait Context {
    fn word_docids(&mut self, word: &str) -> Result<RoaringBitmap>;
    fn prefix_docids(&mut self, word: &str) -> Result<RoaringBitmap>;
    fn synonyms(&self) -> Result<HashMap<Vec<String>, Vec<Vec<String>>>>;
    fn word_documents_count(&mut self, word: &str) -> Result<Option<u64>>;

    fn all_docids(&self) -> Result<RoaringBitmap>;
    fn split_join_config(&self) -> Result<SplitJoinConfig>;
    fn typo_config(&self) -> Result<TypoConfig>;
    fn exact_words(&mut self) -> &fst::Set<Cow<[u8]>>;
    fn word_pair_frequency(
        &mut self,
        left_word: &str,
        right_word: &str,
        proximity: u8,
    ) -> Option<u64>;
    fn word_position_docids(&self, word: &str, position: Position) -> Result<RoaringBitmap>;

    fn word_positions(&mut self, word: &str) -> Result<Vec<Position>>;
    fn prefix_position_docids(&mut self, word: &str, position: Position) -> Result<RoaringBitmap>;

    fn prefix_positions(&mut self, word: &str) -> Result<Vec<Position>>;
    fn word_prefix_pair_proximity_docids(&mut self, word: &str, prefix: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn word_pair_proximity_docids(&mut self, word1: &str, word2: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn ranking_rules(&self) -> Vec<Criterion>;
    fn word_fid_docids(&mut self, word: &str, fid: Fid) -> Result<RoaringBitmap>;
    fn prefix_fid_docids(&mut self, prefix: &str, fid: Fid) -> Result<RoaringBitmap>;
    fn word_fids(&mut self, word: &str) -> Result<Vec<Fid>>;
    fn prefix_fids(&mut self, prefix: &str) -> Result<Vec<Fid>>;
    fn node_docids(&mut self, node_id: usize, graph: &QueryGraph) -> Result<&RoaringBitmap>;
    fn path_docids(&mut self, path: BitSet, graph: &QueryGraph) -> Result<&RoaringBitmap>;
}

impl Context for SearchContext {
    fn word_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
        self.word_docids(word).map(Option::unwrap_or_default)
    }

    fn prefix_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
        self.word_prefix_docids(word).map(Option::unwrap_or_default)
    }

    fn synonyms(&self) -> Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        self.index.synonyms(self.txn).into()
    }

    fn word_documents_count(&mut self, word: &str) -> Result<u64> {
        self.index.word_documents_count(self.txn, word).map(Option::unwrap_or_default).into()
    }

    fn all_docids(&self) -> Result<RoaringBitmap> {
        self.index.documents_ids(self.txn).into()
    }

    fn split_join_config(&self) -> Result<SplitJoinConfig> {
        self.index.split_join_config(self.txn).into()
    }

    fn typo_config(&self) -> Result<TypoConfig> {
        self.index.typo_config(self.txn).into()
    }

    fn exact_words(&mut self) -> Result<Set<Cow<[u8]>>> {
        self.get_words_fst()
    }

    fn word_pair_frequency(&mut self, left_word: &str, right_word: &str, proximity: u8) -> Result<u64> {
        self.get_db_word_pair_proximity_docids_len(left_word,right_word,proximity).map(Option::unwrap_or_default)
    }

    fn word_position_docids(&mut self, word: &str, position: u16) -> Result<RoaringBitmap> {
        self.get_db_word_position_docids(word, position).map(Option::unwrap_or_default)
    }

    fn word_positions(&mut self, word: &str) -> Result<Vec<u16>> {
        self.get_db_word_positions(word)
    }

    fn prefix_position_docids(&mut self, word: &str, position: u16) -> Result<RoaringBitmap> {
        self.get_db_word_prefix_position_docids(word, position).map(Option::unwrap_or_default)
    }

    fn prefix_positions(&mut self, word: &str) -> Result<Vec<u16>> {
        self.get_db_word_prefix_positions(word)
    }

    fn word_prefix_pair_proximity_docids(&mut self, word: &str, prefix: &str, proximity: u8) -> Result<RoaringBitmap> {
        self.get_db_word_prefix_pair_proximity_docids(word, prefix, proximity).map(Option::unwrap_or_default)
    }

    fn word_pair_proximity_docids(&mut self, word1: &str, word2: &str, proximity: u8) -> Result<RoaringBitmap> {
        self.get_db_word_pair_proximity_docids(word1, word2, proximity).map(Option::unwrap_or_default)
    }

    fn ranking_rules(&self) -> Result<Vec<Criterion>> {
        self.index.criteria(self.txn).into()
    }

    fn word_fid_docids(&mut self, word: &str, fid: Fid) -> Result<RoaringBitmap> {
        self.get_db_word_fid_docids(word, fid).map(Option::unwrap_or_default)
    }

    fn prefix_fid_docids(&mut self, prefix: &str, fid: Fid) -> Result<RoaringBitmap> {
        self.get_db_word_prefix_fid_docids(prefix, fid).map(Option::unwrap_or_default)
    }

    fn word_fids(&mut self, word: &str) -> Result<Vec<Fid>> {
        self.get_db_word_fids(word)
    }

    fn prefix_fids(&mut self, prefix: &str) -> Result<Vec<Fid>> {
        self.get_db_word_prefix_fids(prefix)
    }

    fn node_docids(&mut self, node_id: usize, graph: &QueryGraph) -> Result<&RoaringBitmap> {
        self.get_node_docids(node_id, graph)
    }

    fn path_docids(&mut self, path: BitSet, graph: &QueryGraph) -> Result<&RoaringBitmap> {
        self.get_path_docids(path,graph)
    }
}