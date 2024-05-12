use std::collections::BTreeSet;
use heed::RoTxn;
use roaring::RoaringBitmap;
use crate::{AscDesc, DocumentId, Index, TermsMatchingStrategy, Result, Member, UserError, FieldId, FieldIdMapMissingEntry};
use crate::score_details::ScoreDetails;
use crate::search::db_cache::DatabaseCache;
use crate::search::query_cache::QueryCache;

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
    pub(crate) restricted_fids: Option<RestrictedFids>
}

impl<'ctx> SearchContext<'ctx> {
    pub fn searchable_attributes(&mut self, searchable_attributes: &'ctx [String]) -> Result<()> {
        let fids_map = self.index.fields_ids_map(self.txn)?;
        let searchable_names = self.index.searchable_fields(self.txn)?;
        let exact_attributes_ids = self.index.exact_attributes_ids(self.txn)?;

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



pub fn execute_search(
    ctx: &mut SearchContext,
    query: Option<&str>,
    terms_matching_strategy: TermsMatchingStrategy,
    mut universe: RoaringBitmap,
    sort_criteria: &Option<Vec<AscDesc>>,
    skip: usize,
    limit: usize,
) -> Result<PartialSearchResult> {
    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    let query_terms = if let Some(query) = query {
        let tokenizer = ctx.index.tokenizer(ctx.txn)?;

        let span = tracing::trace_span!(target: "search::tokens", "tokenize");
        let entered = span.enter();
        let tokens = tokenizer.tokenize(query);
        drop(entered);

        let ExtractedTokens { query_terms, negative_words, negative_phrases } =
            located_query_terms_from_tokens(ctx, tokens, words_limit)?;
        used_negative_operator = !negative_words.is_empty() || !negative_phrases.is_empty();

        let ignored_documents = resolve_negative_words(ctx, &negative_words)?;
        let ignored_phrases = resolve_negative_phrases(ctx, &negative_phrases)?;

        universe -= ignored_documents;
        universe -= ignored_phrases;

        if query_terms.is_empty() {
            // Do a placeholder search instead
            None
        } else {
            Some(query_terms)
        }
    } else {
        None
    };

    let bucket_sort_output = if let Some(query_terms) = query_terms {
        let (graph, new_located_query_terms) = QueryGraph::from_query(ctx, &query_terms)?;
        located_query_terms = Some(new_located_query_terms);

        let ranking_rules = get_ranking_rules_for_query_graph_search(
            ctx,
            sort_criteria,
            geo_strategy,
            terms_matching_strategy,
        )?;

        universe &=
            resolve_universe(ctx, &universe, &graph, terms_matching_strategy, query_graph_logger)?;

        bucket_sort(
            ctx,
            ranking_rules,
            &graph,
            &universe,
            from,
            length,
            scoring_strategy,
            query_graph_logger,
            time_budget,
        )?
    } else {
        let ranking_rules =
            get_ranking_rules_for_placeholder_search(ctx, sort_criteria, geo_strategy)?;
        bucket_sort(
            ctx,
            ranking_rules,
            &PlaceholderQuery,
            &universe,
            from,
            length,
            scoring_strategy,
            placeholder_search_logger,
            time_budget,
        )?
    };

    let BucketSortOutput { docids, scores, mut all_candidates, degraded } = bucket_sort_output;
    let fields_ids_map = ctx.index.fields_ids_map(ctx.txn)?;

    // The candidates is the universe unless the exhaustive number of hits
    // is requested and a distinct attribute is set.
    if exhaustive_number_hits {
        if let Some(f) = ctx.index.distinct_field(ctx.txn)? {
            if let Some(distinct_fid) = fields_ids_map.id(f) {
                all_candidates = apply_distinct_rule(ctx, distinct_fid, &all_candidates)?.remaining;
            }
        }
    }

    Ok(PartialSearchResult {
        candidates: all_candidates,
        document_scores: scores,
        documents_ids: docids,
        located_query_terms,
        degraded,
        used_negative_operator,
    })
}


pub struct PartialSearchResult {
    pub located_query_terms: Option<Vec<LocatedQueryTerm>>,
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