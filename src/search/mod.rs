use std::fmt;
use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::{AscDesc, DocumentId, Index};
use crate::score_details::{ScoreDetails};
pub use crate::search::facet::Filter;
use crate::search::search::{execute_search, execute_vector_search, filtered_universe, PartialSearchResult, SearchContext};
use crate::vector::Embedder;
use crate::Result;
use crate::search::matches::MatchingWords;

pub mod utils;
pub mod ranking;
pub mod matches;
pub mod facet;
mod graph_visualize;
mod query_graph;
mod query_parser;
mod context;
mod resolve_query_graph;
mod fst_utils;
mod search;
mod db_cache;
mod query_cache;
mod hybrid;


#[derive(Debug, Clone)]
pub struct SemanticSearch {
    vector: Option<Vec<f32>>,
    embedder_name: String,
    embedder: Arc<Embedder>,
}

pub struct Search<'a> {
    query: Option<String>,
    filter: Option<Filter>,
    offset: u64,
    limit: u64,
    sort_criteria: Option<Vec<AscDesc>>,
    analyzer: Option<String>,
    searchable_attributes: Option<&'a [String]>,
    terms_matching_strategy: TermsMatchingStrategy,
    output_query_graph: bool,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    semantic: Option<SemanticSearch>,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            analyzer: None,
            searchable_attributes: None,
            terms_matching_strategy: TermsMatchingStrategy::default(),
            output_query_graph: false,
            rtxn,
            index,
            semantic: None,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn semantic(
        &mut self,
        embedder_name: String,
        embedder: Arc<Embedder>,
        vector: Option<Vec<f32>>,
    ) -> &mut Search<'a> {
        self.semantic = Some(SemanticSearch { embedder_name, embedder, vector });
        self
    }

    pub fn analyzer(&mut self, analyzer: String) -> &mut Search<'a> {
        self.analyzer = Some(analyzer);
        self
    }

    pub fn offset(&mut self, offset: u64) -> &mut Search<'a> {
        self.offset = offset;
        self
    }

    pub fn limit(&mut self, limit: u64) -> &mut Search<'a> {
        self.limit = limit;
        self
    }

    pub fn sort_criteria(&mut self, criteria: Vec<AscDesc>) -> &mut Search<'a> {
        self.sort_criteria = Some(criteria);
        self
    }

    pub fn searchable_attributes(&mut self, searchable: &'a [String]) -> &mut Search<'a> {
        self.searchable_attributes = Some(searchable);
        self
    }

    pub fn output_query_graph(&mut self, output: bool) -> &mut Search<'a> {
        self.output_query_graph = output;
        self
    }

    pub fn terms_matching_strategy(&mut self, value: TermsMatchingStrategy) -> &mut Search<'a> {
        self.terms_matching_strategy = value;
        self
    }

    pub fn filter(&mut self, condition: Filter) -> &mut Search<'a> {
        self.filter = Some(condition);
        self
    }

    pub fn execute_for_candidates(&self, has_vector_search: bool) -> Result<RoaringBitmap> {
        if has_vector_search {
            let ctx = SearchContext::new(self.index, self.rtxn, self.terms_matching_strategy);
            filtered_universe(&ctx, &self.filter)
        } else {
            Ok(self.execute()?.candidates)
        }
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn, self.terms_matching_strategy);

        if let Some(searchable_attributes) = self.searchable_attributes {
            ctx.searchable_attributes(searchable_attributes)?;
        }

        let universe = filtered_universe(&ctx, &self.filter)?;
        let PartialSearchResult {
            candidates,
            documents_ids,
            document_scores,
            query_graph
        } = match self.semantic.as_ref() {
            Some(SemanticSearch { vector: Some(vector), embedder_name, embedder }) => {
                execute_vector_search(
                    &mut ctx,
                    vector,
                    universe,
                    &self.sort_criteria,
                    self.offset,
                    self.limit,
                    embedder_name,
                    embedder,
                )?
            }
            _ => execute_search(
                &mut ctx,
                &self.query,
                universe,
                &self.sort_criteria,
                &self.analyzer,
                self.offset,
                self.limit,
            )?,
        };

        let query_graph_d2 = self.output_query_graph.then(|| query_graph.to_string());
        let matching_words = MatchingWords::new(ctx, query_graph);

        Ok(SearchResult {
            matching_words,
            candidates,
            document_scores,
            documents_ids,
            query_graph: query_graph_d2,
        })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            sort_criteria,
            searchable_attributes,
            terms_matching_strategy,
            analyzer,
            rtxn: _,
            index: _,
            semantic,
            output_query_graph
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("vector", &"[...]")
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("searchable_attributes", searchable_attributes)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("output_query_graph", output_query_graph)
            .field("analyzer", analyzer)
            .field(
                "semantic.embedder_name",
                &semantic.as_ref().map(|semantic| &semantic.embedder_name),
            )
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,
    pub query_graph: Option<String>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    Last,
    // all words are mandatory
    All,
}

impl Default for TermsMatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}


// #[cfg(test)]
// mod test {
//     #[allow(unused_imports)]
//     use super::*;
//
//     #[cfg(feature = "japanese")]
//     #[test]
//     fn test_kanji_language_detection() {
//         use crate::index::tests::TempIndex;
//
//         let index = TempIndex::new();
//
//         index
//             .add_documents(documents!([
//                 { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
//                 { "id": 1, "title": "東京のお寿司。" },
//                 { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
//             ]))
//             .unwrap();
//
//         let txn = index.write_txn().unwrap();
//         let mut search = Search::new(&txn, &index);
//
//         search.query("東京");
//         let SearchResult { documents_ids, .. } = search.execute().unwrap();
//
//         assert_eq!(documents_ids, vec![1]);
//     }
// }
