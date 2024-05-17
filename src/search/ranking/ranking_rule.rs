use std::collections::{HashMap, HashSet};
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::utils::bit_set::BitSet;
use crate::{AscDesc, Criterion, Member, Result, TermsMatchingStrategy};
use crate::score_details::ScoreDetails;
use crate::search::ranking::attribute::AttributeRule;
use crate::search::ranking::exactness::ExactnessRule;
use crate::search::ranking::proximity::ProximityRule;
use crate::search::ranking::sort::SortRule;
use crate::search::ranking::typos::TypoRule;
use crate::search::ranking::vector_sort::VectorSort;
use crate::search::ranking::words::WordsRule;
use crate::vector::Embedder;

pub trait RankingRule{
    fn start_iteration(&mut self, ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()>;
    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>>;
}

#[derive(Debug)]
pub struct RankingRuleOutput{
    pub score: ScoreDetails,
    pub allowed_path: Option<HashSet<BitSet>>,
    pub candidates: RoaringBitmap
}

pub fn get_ranking_rules_for_query_graph_search<'ctx: 'graph, 'graph>(
    ctx: &mut impl Context<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    query_graph: &'graph QueryGraph,
) -> Result<Vec<Box<dyn RankingRule + 'graph>>> {

    let mut ranking_rules: Vec<Box<dyn RankingRule + 'graph>> = vec![];

    let rules = ctx.ranking_rules()?;
    for rule in rules {
        match rule {
            Criterion::Attribute => {
                ranking_rules.push(Box::new(AttributeRule::new(ctx, query_graph)?))
            }
            Criterion::Proximity => {
                ranking_rules.push(Box::new(ProximityRule::new(ctx, query_graph)?))
            },
            Criterion::Typo => {
                ranking_rules.push(Box::new(TypoRule::new(ctx, query_graph)?))
            },
            Criterion::Exactness => {
                ranking_rules.push(Box::new(ExactnessRule::new(ctx, query_graph)?))
            }
            Criterion::Words => {
                ranking_rules.push(Box::new(WordsRule::new(ctx, query_graph)?))
            }
            Criterion::Sort => {
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                )?;
            }
            Criterion::Asc(field_name) => {
                ranking_rules.push(Box::new(SortRule::new(ctx, field_name, true)?));
            }
            Criterion::Desc(field_name) => {
                ranking_rules.push(Box::new(SortRule::new(ctx, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}


pub fn get_ranking_rules_for_vector<'ctx: 'graph, 'graph>(
    ctx: &mut impl Context<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    query_graph: &'graph QueryGraph,
    limit_plus_offset: u64,
    target: &[f32],
    embedder_name: &str,
    embedder: &Embedder,
) -> Result<Vec<Box<dyn RankingRule + 'graph>>> {
    let mut ranking_rules: Vec<Box<dyn RankingRule + 'graph>> = vec![];

    let vector_candidates = ctx.all_docids()?;
    let vector_sort = VectorSort::new(
        ctx,
        target.to_vec(),
        vector_candidates,
        limit_plus_offset as usize,
        embedder_name,
        embedder,
    )?;

    ranking_rules.push(Box::new(vector_sort));


    let rules = ctx.ranking_rules()?;
    for rule in rules {
        match rule {
            Criterion::Attribute => {
                ranking_rules.push(Box::new(AttributeRule::new(ctx, query_graph)?))
            }
            Criterion::Proximity => {
                ranking_rules.push(Box::new(ProximityRule::new(ctx, query_graph)?))
            },
            Criterion::Typo => {
                ranking_rules.push(Box::new(TypoRule::new(ctx, query_graph)?))
            },
            Criterion::Exactness => {
                ranking_rules.push(Box::new(ExactnessRule::new(ctx, query_graph)?))
            }
            Criterion::Words => {
                ranking_rules.push(Box::new(WordsRule::new(ctx, query_graph)?))
            }
            Criterion::Sort => {
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                )?;
            }
            Criterion::Asc(field_name) => {
                ranking_rules.push(Box::new(SortRule::new(ctx, field_name, true)?));
            }
            Criterion::Desc(field_name) => {
                ranking_rules.push(Box::new(SortRule::new(ctx, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}

fn resolve_sort_criteria<'ctx:'graph, 'graph>(
    sort_criteria: &Option<Vec<AscDesc>>,
    ctx: &impl Context<'ctx>,
    ranking_rules: &mut Vec<Box<dyn RankingRule + 'graph>>,
) -> Result<()> {
    if let Some(sort_criteria) = sort_criteria.clone(){
        ranking_rules.reserve(sort_criteria.len());

        for criterion in sort_criteria {
            match criterion {
                AscDesc::Asc(Member::Field(field_name)) => {
                    ranking_rules.push(Box::new(SortRule::new(ctx, field_name, true)?));
                }
                AscDesc::Desc(Member::Field(field_name)) => {
                    ranking_rules.push(Box::new(SortRule::new(ctx, field_name, false)?));
                }
            };
        }
    }

    Ok(())
}