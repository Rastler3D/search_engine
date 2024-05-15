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

pub fn bucket_sort(graph: &QueryGraph, ctx: &mut impl Context, limit: u64, skip: u64, candidates: RoaringBitmap) -> Result<BucketSortOutput>{
    let mut ranking_rules = resolve_ranking_rules(ctx, graph)?;
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

    Ok(BucketSortOutput{ docids, scores, })

}

pub fn recursive_sort(mut current_rule: usize, ctx: &mut dyn Context, mut score: &mut Vec<ScoreDetails>, ranking_rule: &mut Vec<Box<dyn RankingRule + '_>>, mut limit: &mut u64, mut skip:&mut u64, output: &mut Vec<(u32, Vec<ScoreDetails>)>, mut visited: &mut RoaringBitmap) -> Result<()>{

    while let Some(mut bucket) = ranking_rule[current_rule].next_bucket(ctx)?{
        println!("{:?}", bucket);
        score.push(bucket.score);
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
        if current_rule == ranking_rule.len() - 1{
            *visited |= &*docids;
            let docids_count = docids.len();
            for docids in docids.iter().skip(*skip as usize).take(*limit as usize){
                output.push((docids, score.clone()));
            }
            *limit -= if docids_count < *skip { 0 } else {  docids_count - *skip };
            *skip = if docids_count < *skip { *skip - docids_count } else { 0 };
        } else {
            ranking_rule[current_rule + 1].start_iteration(ctx, bucket.candidates, Some(bucket.allowed_path))?;
            recursive_sort(current_rule + 1, ctx, score, ranking_rule, limit, skip, output,visited)?;
        }

    }

    score.pop();

    Ok(())
}

fn resolve_ranking_rules<'graph>(context: &mut impl Context, graph: &'graph QueryGraph) -> Result<Vec<Box<dyn RankingRule + 'graph>>>{
    let rules = context.ranking_rules()?;
    let mut ranking_rules: Vec<Box<dyn RankingRule>> = Vec::with_capacity(rules.len());

    for rule in rules{
        match rule {
            // Criterion::Typo => {
            //     ranking_rules.push(Box::new(TypoRule::new(context, graph)?))
            // }
            Criterion::Proximity => {
                ranking_rules.push(Box::new(ProximityRule::new(context, graph)?))
            }
            _ => unimplemented!()
        }
    }

    Ok(ranking_rules)
}



#[derive(Debug)]
pub struct BucketSortOutput {
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
        let mut costs = bucket_sort(&query_graph, &mut context, 10000, 600, candidates).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        //println!("{:#?}", costs);
    }
}