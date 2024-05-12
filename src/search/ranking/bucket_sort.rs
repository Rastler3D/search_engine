use std::ops::{BitAnd, BitXor};
use std::time::Instant;
use itertools::Itertools;
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::ranking::criteria::Criterion;
use crate::search::ranking::proximity::ProximityRule;
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::ranking::score::ScoreDetails;
use crate::search::ranking::typos::TypoRule;

pub fn bucket_sort(graph: &QueryGraph, context: &mut impl Context, limit: u64, skip: u64) -> heed::Result<BucketSortOutput>{
    let ranking_rules = resolve_ranking_rules(context, graph)?;
    let time = Instant::now();
    let mut rule = &ranking_rules[0];
    let mut buckets = rule.buckets(None);
    let mut limit = limit;
    let mut skip = skip;
    let mut buf = RoaringBitmap::new();
    let mut visited = RoaringBitmap::new();
    let mut score = Vec::new();
    let mut output = Vec::new();
    
    recursive_sort(buckets, &mut score, &ranking_rules, 0, &mut limit, &mut skip, &mut output, &mut visited, &mut buf );
    println!("Sorted {:?} Docs {}", time.elapsed(), output.len());
    let (docids, scores) = output.into_iter().unzip();
    Ok(BucketSortOutput{
        docids: docids,
        scores: scores,
    })

}

pub fn recursive_sort(mut buckets: Box<dyn Iterator<Item = RankingRuleOutput> + '_>, mut score: &mut Vec<ScoreDetails>, ranking_rule: &Vec<Box<dyn RankingRule>>, current_rule: usize, mut limit: &mut u64, mut skip:&mut u64, output: &mut Vec<(u32, Vec<ScoreDetails>)>, mut visited: &mut RoaringBitmap, buf: &mut RoaringBitmap){
    while let Some(bucket) = buckets.next(){
        score.push(bucket.score);
        println!("{:?}", bucket);
        if *limit == 0 {
            return;
        }
        buf.clear();
        let mut docids =bucket.candidates.iter().fold(&mut *buf, |mut buf: &mut RoaringBitmap, (_, docids)| {*buf |= docids; buf});
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
            let buckets = ranking_rule[current_rule + 1].buckets(Some(bucket.candidates));
            recursive_sort(buckets, score, ranking_rule, current_rule + 1, limit, skip, output,visited,buf)
        }

    }
    score.pop();
}

fn resolve_ranking_rules(context: &mut impl Context, graph: &QueryGraph) -> heed::Result<Vec<Box<dyn RankingRule>>>{
    let rules = context.ranking_rules();
    let mut ranking_rules: Vec<Box<dyn RankingRule>> = Vec::with_capacity(rules.len());

    for rule in rules{
        match rule {
            Criterion::Typo => {
                ranking_rules.push(Box::new(TypoRule::new(context, graph)?))
            }
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
        let query_graph = QueryGraph::from_query(parsed_query, &context).unwrap();
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world");
        let parsed_query = parse_query(stream);
        let time = Instant::now();
        let query_graph = QueryGraph::from_query(parsed_query, &context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph build {:?}", elapsed);
        let time = Instant::now();
        let mut costs = bucket_sort(&query_graph, &mut context, 10000, 600).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        //println!("{:#?}", costs);
    }
}