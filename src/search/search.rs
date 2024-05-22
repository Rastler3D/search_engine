use std::collections::BTreeSet;
use heed::RoTxn;
use roaring::RoaringBitmap;
use analyzer::analyzer::Analyzer;
use crate::{AscDesc, DocumentId, Index, TermsMatchingStrategy, Result, Member, UserError, FieldId, FieldIdMapMissingEntry, Filter};
use crate::score_details::{ScoreDetails};
use crate::search::context::Context;
use crate::search::db_cache::DatabaseCache;
use crate::search::query_cache::QueryCache;
use crate::search::query_graph::QueryGraph;
use crate::search::query_parser::parse_query;
use crate::search::ranking::bucket_sort::{bucket_sort, BucketSortOutput};
use crate::search::ranking::ranking_rule::{get_ranking_rules_for_query_graph_search, get_ranking_rules_for_vector};
use crate::vector::Embedder;

#[derive(Debug, Clone, Default)]
pub struct RestrictedFids {
    pub fields: Vec<FieldId>,
}

impl RestrictedFids {
    pub fn contains(&self, fid: &FieldId) -> bool {
        self.fields.contains(fid)
    }
}

pub struct SearchContext<'ctx> {
    pub index: &'ctx Index,
    pub txn: &'ctx RoTxn<'ctx>,
    pub db_cache: DatabaseCache<'ctx>,
    pub query_cache: QueryCache,
    pub restricted_fids: Option<RestrictedFids>,
    pub terms_matching_strategy: TermsMatchingStrategy
}

impl<'ctx> SearchContext<'ctx> {
    pub fn new(index: &'ctx Index, txn: &'ctx RoTxn<'ctx>, terms_matching_strategy: TermsMatchingStrategy) -> Self {
        Self {
            index,
            txn,
            terms_matching_strategy,
            db_cache: Default::default(),
            query_cache: Default::default(),
            restricted_fids: None,
        }
    }
    pub fn searchable_attributes(&mut self, searchable_attributes: &'ctx [String]) -> Result<()> {
        let fids_map = self.index.fields_ids_map(self.txn)?;
        let searchable_names = self.index.searchable_fields(self.txn)?;

        let mut restricted_fids = RestrictedFids::default();
        let mut contains_wildcard = false;
        for field_name in searchable_attributes {
            if field_name == "*" {
                contains_wildcard = true;
                continue;
            }
            let searchable_contains_name =
                searchable_names.as_ref().map(|sn| sn.iter().any(|name| name == field_name));
            let fid = match (fids_map.id(field_name), searchable_contains_name) {
                // The Field id exist and the field is searchable
                (Some(fid), Some(true)) | (Some(fid), None) => fid,
                // The field is searchable but the Field id doesn't exist => Internal Error
                (None, Some(true)) => {
                    return Err(FieldIdMapMissingEntry::FieldName {
                        field_name: field_name.to_string(),
                        process: "search",
                    }
                        .into())
                }
                // The field is not searchable, but the searchableAttributes are set to * => ignore field
                (None, None) => continue,
                // The field is not searchable => User error
                (_fid, Some(false)) => {
                    let (valid_fields) = match searchable_names {
                        Some(sn) => BTreeSet::from_iter(sn.into_iter().map(ToOwned::to_owned)),
                        None => BTreeSet::from_iter(fids_map.names().map(ToOwned::to_owned)),
                    };

                    let field = field_name.to_string();
                    return Err(UserError::InvalidSearchableAttribute {
                        field,
                        valid_fields,
                    }
                        .into());
                }
            };

            restricted_fids.fields.push(fid);
        }

        self.restricted_fids = (!contains_wildcard).then_some(restricted_fids);

        Ok(())
    }
}

pub fn execute_vector_search(
    ctx: &mut SearchContext,
    vector: &[f32],
    candidates: RoaringBitmap,
    sort_criteria: &Option<Vec<AscDesc>>,
    skip: u64,
    limit: u64,
    embedder_name: &str,
    embedder: &Embedder,
) -> Result<PartialSearchResult> {
    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    let placeholder_graph = QueryGraph::placeholder(ctx)?;
    let ranking_rules = get_ranking_rules_for_vector(
        ctx,
        sort_criteria,
        &placeholder_graph,
        skip + limit,
        vector,
        embedder_name,
        embedder,
    )?;

    let BucketSortOutput { docids, scores, candidates} = bucket_sort(
        ctx,
        ranking_rules,
        limit,
        skip,
        candidates,
    )?;

    Ok(PartialSearchResult {
        query_graph: placeholder_graph,
        candidates: candidates,
        document_scores: scores,
        documents_ids: docids,
    })
}

pub fn execute_search(
    ctx: &mut SearchContext,
    query: &Option<String>,
    candidates: RoaringBitmap,
    sort_criteria: &Option<Vec<AscDesc>>,
    analyzer: &Option<String>,
    skip: u64,
    limit: u64,
) -> Result<PartialSearchResult> {
    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    let query_graph = if let Some(query) = query {
        let analyzer = ctx.index.analyzer(ctx.txn, analyzer)?;

        let span = tracing::trace_span!(target: "search::tokens", "tokenize");
        let entered = span.enter();
        let tokens = analyzer.analyze(query);
        drop(entered);
        let query = parse_query(tokens);

        QueryGraph::from_query(query, ctx)?
    } else {
        QueryGraph::placeholder(ctx)?
    };

    let ranking_rules = get_ranking_rules_for_query_graph_search(ctx, sort_criteria, &query_graph)?;
    let bucket_sort_output= bucket_sort(ctx, ranking_rules, limit, skip, candidates)?;

    let BucketSortOutput { docids, scores, candidates} = bucket_sort_output;

    Ok(PartialSearchResult {
        query_graph,
        candidates,
        document_scores: scores,
        documents_ids: docids,
    })
}


pub struct PartialSearchResult {
    pub query_graph: QueryGraph,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,

}

fn check_sort_criteria(ctx: &SearchContext, sort_criteria: Option<&Vec<AscDesc>>) -> Result<()> {
    let sort_criteria = if let Some(sort_criteria) = sort_criteria {
        sort_criteria
    } else {
        return Ok(());
    };

    if sort_criteria.is_empty() {
        return Ok(());
    }

    // We check that the sort ranking rule exists and throw an
    // error if we try to use it and that it doesn't.
    let sort_ranking_rule_missing = !ctx.index.criteria(ctx.txn)?.contains(&crate::Criterion::Sort);
    if sort_ranking_rule_missing {
        return Err(UserError::SortRankingRuleMissing.into());
    }

    // We check that we are allowed to use the sort criteria, we check
    // that they are declared in the sortable fields.
    let sortable_fields = ctx.index.sortable_fields(ctx.txn)?;
    for asc_desc in sort_criteria {
        match asc_desc.member() {
            Member::Field(ref field) if !crate::is_faceted(field, &sortable_fields) => {

                return Err(UserError::InvalidSortableAttribute {
                    field: field.to_string(),
                    valid_fields: BTreeSet::from_iter(sortable_fields),
                }
                    .into());
            }
            _ => (),
        }
    }

    Ok(())
}

pub fn filtered_universe(ctx: &SearchContext, filters: &Option<Filter>) -> Result<RoaringBitmap> {
    if let Some(filters) = filters {
        filters.evaluate(ctx.txn, ctx.index)
    } else {
        ctx.all_docids()
    }
}