use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, BTreeSet};
use std::ops::ControlFlow;

use charabia::normalizer::NormalizerOption;
use charabia::Normalize;
use fst::automaton::{Automaton, Str};
use fst::{IntoStreamer, Streamer};
use roaring::RoaringBitmap;
use tracing::error;

use crate::error::UserError;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::{DocumentId, FieldId, OrderBy, Result, Search};
use crate::search::query_graph::{LEVDIST0, LEVDIST1, LEVDIST2};
use crate::update::typo_config;


const DEFAULT_MAX_NUMBER_OF_VALUES_PER_FACET: usize = 100;

pub struct SearchForFacetValues<'a> {
    query: Option<String>,
    facet: String,
    search_query: Search<'a>,
    max_values: usize,
    is_hybrid: bool,
}

impl<'a> SearchForFacetValues<'a> {
    pub fn new(
        facet: String,
        search_query: Search<'a>,
        is_hybrid: bool,
    ) -> SearchForFacetValues<'a> {
        SearchForFacetValues {
            query: None,
            facet,
            search_query,
            max_values: DEFAULT_MAX_NUMBER_OF_VALUES_PER_FACET,
            is_hybrid,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Self {
        self.query = Some(query.into());
        self
    }

    pub fn max_values(&mut self, max: usize) -> &mut Self {
        self.max_values = max;
        self
    }

    fn one_original_value_of(
        &self,
        field_id: FieldId,
        facet_str: &str,
        any_docid: DocumentId,
    ) -> Result<Option<String>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;
        let key: (FieldId, _, &str) = (field_id, any_docid, facet_str);
        Ok(index.field_id_docid_facet_strings.get(rtxn, &key)?.map(|v| v.to_owned()))
    }

    pub fn execute(&self) -> Result<Vec<FacetValueHit>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let filterable_fields = index.filterable_fields(rtxn)?;
        if !filterable_fields.contains(&self.facet) {

            return Err(UserError::InvalidFacetSearchFacetName {
                field: self.facet.clone(),
                valid_fields: BTreeSet::from_iter(filterable_fields),
            }
            .into());
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let fid = match fields_ids_map.id(&self.facet) {
            Some(fid) => fid,
            // we return an empty list of results when the attribute has been
            // set as filterable but no document contains this field (yet).
            None => return Ok(Vec::new()),
        };

        let fst = match self.search_query.index.facet_id_string_fst.get(rtxn, &fid)? {
            Some(fst) => fst,
            None => return Ok(Vec::new()),
        };

        let search_candidates = self.search_query.execute_for_candidates(
            self.is_hybrid
                || self
                    .search_query
                    .semantic
                    .as_ref()
                    .and_then(|semantic| semantic.vector.as_ref())
                    .is_some(),
        )?;

        let mut results = match index.sort_facet_values_by(rtxn)?.get(&self.facet) {
            OrderBy::Lexicographic => ValuesCollection::by_lexicographic(self.max_values),
            OrderBy::Count => ValuesCollection::by_count(self.max_values),
        };

        match self.query.as_ref() {
            Some(query) => {
                let options = NormalizerOption { lossy: true, ..Default::default() };
                let query = query.normalize(&options);
                let query = query.as_ref();
                let typo_config = self.search_query.index.typo_config(self.search_query.rtxn)?;
                let typos_allowed = typo_config.allowed_typos(query);
                let automaton = match typos_allowed {
                    0 => LEVDIST0.build_prefix_dfa(query),
                    1 => LEVDIST1.build_prefix_dfa(query),
                    _ => LEVDIST2.build_prefix_dfa(query)
                };

                let mut stream = fst.search(automaton).into_stream();
                while let Some(facet_value) = stream.next() {
                    let value = std::str::from_utf8(facet_value)?;
                    if self
                        .fetch_original_facets_using_normalized(
                            fid,
                            value,
                            query,
                            &search_candidates,
                            &mut results,
                        )?
                        .is_break()
                    {
                        break;
                    }
                }
            }
            None => {
                let prefix = FacetGroupKey { field_id: fid, level: 0, left_bound: "" };
                for result in index.facet_id_string_docids.prefix_iter(rtxn, &prefix)? {
                    let (FacetGroupKey { left_bound, .. }, FacetGroupValue { bitmap, .. }) =
                        result?;
                    let count = search_candidates.intersection_len(&bitmap);
                    if count != 0 {
                        let value = self
                            .one_original_value_of(fid, left_bound, bitmap.min().unwrap())?
                            .unwrap_or_else(|| left_bound.to_string());
                        if results.insert(FacetValueHit { value, count }).is_break() {
                            break;
                        }
                    }
                }
            }
        }

        Ok(results.into_sorted_vec())
    }

    fn fetch_original_facets_using_normalized(
        &self,
        fid: FieldId,
        value: &str,
        query: &str,
        search_candidates: &RoaringBitmap,
        results: &mut ValuesCollection,
    ) -> Result<ControlFlow<()>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let database = index.facet_id_normalized_string_strings;
        let key = (fid, value);
        let original_strings = match database.get(rtxn, &key)? {
            Some(original_strings) => original_strings,
            None => {
                error!("the facet value is missing from the facet database: {key:?}");
                return Ok(ControlFlow::Continue(()));
            }
        };
        for original in original_strings {
            let key = FacetGroupKey { field_id: fid, level: 0, left_bound: original.as_str() };
            let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                Some(FacetGroupValue { bitmap, .. }) => bitmap,
                None => {
                    error!("the facet value is missing from the facet database: {key:?}");
                    return Ok(ControlFlow::Continue(()));
                }
            };
            let count = search_candidates.intersection_len(&docids);
            if count != 0 {
                let value = self
                    .one_original_value_of(fid, &original, docids.min().unwrap())?
                    .unwrap_or_else(|| query.to_string());
                if results.insert(FacetValueHit { value, count }).is_break() {
                    break;
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct FacetValueHit {
    /// The original facet value
    pub value: String,
    /// The number of documents associated to this facet
    pub count: u64,
}

impl PartialOrd for FacetValueHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FacetValueHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.count.cmp(&other.count).then_with(|| self.value.cmp(&other.value))
    }
}

impl Eq for FacetValueHit {}

/// A wrapper type that collects the best facet values by
/// lexicographic or number of associated values.
enum ValuesCollection {
    /// Keeps the top values according to the lexicographic order.
    Lexicographic { max: usize, content: Vec<FacetValueHit> },
    /// Keeps the top values according to the number of values associated to them.
    ///
    /// Note that it is a max heap and we need to move the smallest counts
    /// at the top to be able to pop them when we reach the max_values limit.
    Count { max: usize, content: BinaryHeap<Reverse<FacetValueHit>> },
}

impl ValuesCollection {
    pub fn by_lexicographic(max: usize) -> Self {
        ValuesCollection::Lexicographic { max, content: Vec::new() }
    }

    pub fn by_count(max: usize) -> Self {
        ValuesCollection::Count { max, content: BinaryHeap::new() }
    }

    pub fn insert(&mut self, value: FacetValueHit) -> ControlFlow<()> {
        match self {
            ValuesCollection::Lexicographic { max, content } => {
                if content.len() < *max {
                    content.push(value);
                    if content.len() < *max {
                        return ControlFlow::Continue(());
                    }
                }
                ControlFlow::Break(())
            }
            ValuesCollection::Count { max, content } => {
                if content.len() == *max {
                    // Peeking gives us the worst value in the list as
                    // this is a max-heap and we reversed it.
                    let Some(mut peek) = content.peek_mut() else { return ControlFlow::Break(()) };
                    if peek.0.count <= value.count {
                        // Replace the current worst value in the heap
                        // with the new one we received that is better.
                        *peek = Reverse(value);
                    }
                } else {
                    content.push(Reverse(value));
                }
                ControlFlow::Continue(())
            }
        }
    }

    /// Returns the list of facet values in descending order of, either,
    /// count or lexicographic order of the value depending on the type.
    pub fn into_sorted_vec(self) -> Vec<FacetValueHit> {
        match self {
            ValuesCollection::Lexicographic { content, .. } => content.into_iter().collect(),
            ValuesCollection::Count { content, .. } => {
                // Convert the heap into a vec of hits by removing the Reverse wrapper.
                // Hits are already in the right order as they were reversed and there
                // are output in ascending order.
                content.into_sorted_vec().into_iter().map(|Reverse(hit)| hit).collect()
            }
        }
    }
}
