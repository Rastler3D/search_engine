use std::ops::{BitAnd, BitXor};
use std::time::Instant;
use itertools::Itertools;
use roaring::RoaringBitmap;
use crate::Criterion;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::ranking::proximity::ProximityRule;
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::Result;
use crate::score_details::ScoreDetails;
use crate::search::ranking::attribute::AttributeRule;
use crate::search::ranking::exactness::ExactnessRule;
use crate::search::ranking::typos::TypoRule;
use crate::search::ranking::words::WordsRule;

pub fn bucket_sort(ctx: &mut impl Context, mut ranking_rules: Vec<Box<dyn RankingRule + '_>>, limit: u64, skip: u64, candidates: RoaringBitmap) -> Result<BucketSortOutput>{
    if ranking_rules.is_empty(){
        return Ok(BucketSortOutput{
            docids: candidates.iter().collect(),
            scores: vec![Vec::new(); candidates.len() as usize],
            candidates
        })
    }
    let time = Instant::now();

    let mut limit = limit;
    let mut skip = skip;
    let mut visited = RoaringBitmap::new();
    let mut score = Vec::new();
    let mut output = Vec::new();

    ranking_rules[0].start_iteration(ctx, candidates, None)?;
    recursive_sort(0, ctx, &mut score, &mut ranking_rules, &mut limit, &mut skip, &mut output, &mut visited)?;
    println!("Sorted {:?} Docs {}", time.elapsed(), output.len());

    let (docids, scores) = output.into_iter().unzip();

    let candidates = RoaringBitmap::from_iter(&docids);

    Ok(BucketSortOutput{ candidates, docids, scores, })

}

pub fn recursive_sort(mut current_rule: usize, ctx: &mut dyn Context, mut score: &mut Vec<ScoreDetails>, ranking_rule: &mut Vec<Box<dyn RankingRule + '_>>, mut limit: &mut u64, mut skip:&mut u64, output: &mut Vec<(u32, Vec<ScoreDetails>)>, mut visited: &mut RoaringBitmap) -> Result<()>{

    while let Some(mut bucket) = ranking_rule[current_rule].next_bucket(ctx)?{
        println!("{:?}", bucket);
        if *limit == 0 {
            return Ok(());
        }
        let mut docids = &mut bucket.candidates;
        *docids -= &*visited;
        let len = docids.len();
        if len == 0{
            continue
        }
        if *skip > 0 && len < *skip{
            *visited |= &*docids;
            *skip -= len;

            continue
        }

        score.push(bucket.score);
        if current_rule == ranking_rule.len() - 1{
            *visited |= &*docids;
            let docids_count = docids.len();
            for docids in docids.iter().skip(*skip as usize).take(*limit as usize){
                output.push((docids, score.clone()));
            }
            *limit = limit.saturating_sub(if docids_count < *skip { 0 } else {  docids_count - *skip });
            *skip = if docids_count < *skip { *skip - docids_count } else { 0 };

            score.pop();
        } else {
            ranking_rule[current_rule + 1].start_iteration(ctx, bucket.candidates, bucket.allowed_path)?;
            recursive_sort(current_rule + 1, ctx, score, ranking_rule, limit, skip, output,visited)?;
        }
    }
    score.pop();

    Ok(())
}

#[derive(Debug)]
pub struct BucketSortOutput {
    pub candidates: RoaringBitmap,
    pub docids: Vec<u32>,
    pub scores: Vec<Vec<ScoreDetails>>,
}


#[cfg(test)]
mod tests {
    use std::time::Instant;
    use crate::search::query_graph::tests::TestContext;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use analyzer::analyzer::Analyzer;
    use crate::search::ranking::ranking_rule::get_ranking_rules_for_query_graph_search;
    use super::*;

    #[test]
    fn bucket_sort_test() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world");
        let parsed_query = parse_query(stream);
        let time = Instant::now();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph build {:?}", elapsed);
        let time = Instant::now();
        let candidates = context.all_docids().unwrap();
        let ranking_rules = get_ranking_rules_for_query_graph_search(&mut context, &None, &query_graph).unwrap();
        let mut costs = bucket_sort(&mut context, ranking_rules,  10000, 600, candidates).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        //println!("{:#?}", costs);
    }
}