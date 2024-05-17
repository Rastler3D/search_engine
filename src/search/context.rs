use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use arroy::distances::Angular;
use arroy::{ItemId, Reader};
use fst::Set;
use heed::RoTxn;
use roaring::RoaringBitmap;

use crate::search::search::SearchContext;
use crate::{Criterion, FieldId, FieldsIdsMap, Result, TermsMatchingStrategy, UserError};
use crate::heed_codec::BytesRefCodec;
use crate::heed_codec::facet::FacetGroupKeyCodec;
use crate::search::facet::{ascending_facet_sort, AscendingSortIter, descending_facet_sort, DescendingSortIter};
use crate::search::query_graph::QueryGraph;
use crate::search::utils::bit_set::BitSet;
use crate::update::split_config::SplitJoinConfig;
use crate::update::typo_config::TypoConfig;

pub type Fid = u16;
pub type Position = u16;
pub trait Context<'t> {
    fn word_docids(&mut self, word: &str) -> Result<RoaringBitmap>;
    fn prefix_docids(&mut self, word: &str) -> Result<RoaringBitmap>;
    fn synonyms(&self) -> Result<HashMap<Vec<String>, Vec<Vec<String>>>>;
    fn word_documents_count(&mut self, word: &str) -> Result<u64>;
    fn term_matching_strategy(&self) -> TermsMatchingStrategy;
    fn all_docids(&self) -> Result<RoaringBitmap>;
    fn split_join_config(&self) -> Result<SplitJoinConfig>;
    fn typo_config(&self) -> Result<TypoConfig>;
    fn exact_words(&mut self) -> Result<Set<Cow<[u8]>>>;
    fn searchable_fields_ids(&mut self) -> Result<Option<Vec<FieldId>>>;
    fn word_pair_frequency(
        &mut self,
        left_word: &str,
        right_word: &str,
        proximity: u8,
    ) -> Result<u64>;
    fn word_position_docids(&mut self, word: &str, position: Position) -> Result<RoaringBitmap>;

    fn word_positions(&mut self, word: &str) -> Result<Vec<Position>>;
    fn prefix_position_docids(&mut self, word: &str, position: Position) -> Result<RoaringBitmap>;

    fn prefix_positions(&mut self, word: &str) -> Result<Vec<Position>>;
    fn word_prefix_pair_proximity_docids(&mut self, word: &str, prefix: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn prefix_word_pair_proximity_docids(&mut self, prefix: &str, word: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn word_pair_proximity_docids(&mut self, word1: &str, word2: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn prefix_prefix_pair_proximity_docids(&mut self, prefix1: &str, prefix2: &str, proximity: u8) -> Result<RoaringBitmap>;
    fn ranking_rules(&self) -> Result<Vec<Criterion>>;
    fn field_ids(&self) -> Result<FieldsIdsMap>;
    fn word_fid_docids(&mut self, word: &str, fid: Fid) -> Result<RoaringBitmap>;
    fn prefix_fid_docids(&mut self, prefix: &str, fid: Fid) -> Result<RoaringBitmap>;
    fn word_fids(&mut self, word: &str) -> Result<Vec<Fid>>;
    fn prefix_fids(&mut self, prefix: &str) -> Result<Vec<Fid>>;
    fn node_docids(&mut self, node_id: usize, graph: &QueryGraph) -> Result<&RoaringBitmap>;
    fn path_docids(&mut self, path: BitSet, graph: &QueryGraph) -> Result<&RoaringBitmap>;
    fn phrase_docids(&mut self, path: &[String]) -> Result<&RoaringBitmap>;
    fn split_docids(&mut self, path: &(String, String)) -> Result<&RoaringBitmap>;
    fn embedder_category_id(&self, embedder_name: &str) -> Result<u8>;
    fn vector_reader(&self, index: u16) -> arroy::Result<Reader<Angular>>;
    fn ascending_number_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<AscendingSortIter<'a>>;
    fn ascending_string_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<AscendingSortIter<'a>>;
    fn descending_number_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<DescendingSortIter<'a>>;
    fn txn(&self) -> &'t RoTxn<'t>;
    fn descending_string_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<DescendingSortIter<'a>>;
}

impl<'t> Context<'t> for SearchContext<'t> {
    fn word_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
        self.word_docids(word).map(Option::unwrap_or_default)
    }

    fn prefix_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
        self.word_prefix_docids(word).map(Option::unwrap_or_default)
    }

    fn synonyms(&self) -> Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self.index.synonyms(self.txn)?)
    }

    fn word_documents_count(&mut self, word: &str) -> Result<u64> {
        Ok(self.index.word_documents_count(self.txn, word).map(Option::unwrap_or_default)?)
    }

    fn term_matching_strategy(&self) -> TermsMatchingStrategy {
        self.terms_matching_strategy
    }

    fn all_docids(&self) -> Result<RoaringBitmap> {
        Ok(self.index.documents_ids(self.txn)?)
    }

    fn split_join_config(&self) -> Result<SplitJoinConfig> {
        Ok(self.index.split_join_config(self.txn)?)
    }

    fn typo_config(&self) -> Result<TypoConfig> {
        Ok(self.index.typo_config(self.txn)?)
    }

    fn exact_words(&mut self) -> Result<Set<Cow<[u8]>>> {
        self.get_words_fst()
    }

    fn searchable_fields_ids(&mut self) -> Result<Option<Vec<FieldId>>> {
        self.index.searchable_fields_ids(self.txn)
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

    fn prefix_word_pair_proximity_docids(&mut self, prefix: &str, word: &str, proximity: u8) -> Result<RoaringBitmap> {
        self.get_db_prefix_word_pair_proximity_docids(prefix, word, proximity).map(Option::unwrap_or_default)
    }

    fn word_pair_proximity_docids(&mut self, word1: &str, word2: &str, proximity: u8) -> Result<RoaringBitmap> {
        self.get_db_word_pair_proximity_docids(word1, word2, proximity).map(Option::unwrap_or_default)
    }

    fn prefix_prefix_pair_proximity_docids(&mut self, prefix1: &str, prefix2: &str, proximity: u8) -> Result<RoaringBitmap> {
        self.get_db_prefix_prefix_pair_proximity_docids(prefix1, prefix2, proximity).map(Option::unwrap_or_default)
    }

    fn ranking_rules(&self) -> Result<Vec<Criterion>> {
        Ok(self.index.criteria(self.txn)?)
    }

    fn field_ids(&self) -> Result<FieldsIdsMap> {
        Ok(self.index.fields_ids_map(self.txn)?)
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

    fn phrase_docids(&mut self, path: &[String]) -> Result<&RoaringBitmap> {
        self.get_phrase_docids(path)
    }
    fn split_docids(&mut self, path: &(String,String)) -> Result<&RoaringBitmap> {
        self.get_split_docids(path)
    }
    fn embedder_category_id(&self, embedder_name: &str) -> Result<u8>{
        self.index.embedder_category_id.get(self.txn, embedder_name)?
            .ok_or_else(|| UserError::InvalidEmbedder(embedder_name.to_owned()).into())
    }

    fn vector_reader(&self, index: u16) -> arroy::Result<Reader<Angular>> {
        arroy::Reader::open(self.txn, index, self.index.vector_arroy)
    }


    fn ascending_number_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<AscendingSortIter<'a>> {
        let number_db = self.index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
        Ok(ascending_facet_sort(
            tnx,
            number_db,
            fid,
            candidates,
        )?)
    }

    fn ascending_string_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<AscendingSortIter<'a>> {
        let number_db = self.index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
        Ok(ascending_facet_sort(
            tnx,
            number_db,
            fid,
            candidates,
        )?)
    }

    fn descending_number_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<DescendingSortIter<'a>> {
        let number_db = self.index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
        Ok(descending_facet_sort(
            tnx,
            number_db,
            fid,
            candidates,
        )?)
    }

    fn txn(&self) -> &'t RoTxn<'t> {
        self.txn
    }

    fn descending_string_sort<'a>(&self, tnx: &'a RoTxn<'a>, fid: Fid, candidates: RoaringBitmap) -> Result<DescendingSortIter<'a>> {
        let number_db = self.index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
        Ok(descending_facet_sort(
            tnx,
            number_db,
            fid,
            candidates,
        )?)
    }
}