use std::collections::{HashMap, HashSet};
use std::ops::{ControlFlow, RangeInclusive};
use std::time::Instant;
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::ranking::paths_cost::{Cost, paths_cost};
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use crate::Result;
use crate::score_details::{ExactWords, ScoreDetails};
use crate::search::query_parser::{DerivativeTerm, Term, TermKind};
use crate::search::ranking::path_visitor::Edge;

pub struct ExactnessRule<'graph>{
    costs: VecMap<HashSet<BitSet>>,
    candidates: RoaringBitmap,
    allowed_paths: Option<HashSet<BitSet>>,
    graph: &'graph QueryGraph,
    cur_cost: RangeInclusive<usize>,
    max_cost: usize
}

impl<'graph> ExactnessRule<'graph> {
    pub fn new(context: &mut impl Context, graph: &'graph QueryGraph) -> Result<Self>{
        let paths = paths_cost::<ExactnessCost>(graph, context);
        let max_cost = Self::max_cost(context, graph)?;
        let mut costs = VecMap::with_capacity(max_cost as usize);
        for (path, cost) in paths{
            costs.get_or_insert_with(cost, || HashSet::new()).insert(path);
        }

        Ok(ExactnessRule{ costs, candidates: RoaringBitmap::new(), allowed_paths: None, graph, cur_cost: (1..=0), max_cost })
    }
    pub fn max_cost(_: &impl Context, graph: &QueryGraph) -> Result<usize>{
        Ok(graph.query_words)
    }
}


impl RankingRule for ExactnessRule<'_> {

    fn start_iteration(&mut self, _ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()> {
        self.candidates = candidates;
        self.allowed_paths = allowed_paths;
        self.cur_cost = 0..=self.max_cost;

        Ok(())
    }

    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>> {
        self.cur_cost.next_back().map(|cost| -> Result<RankingRuleOutput> {
            let mut bucket = RoaringBitmap::new();
            let mut good_paths = HashSet::new();
            let mut buf = RoaringBitmap::new();
            if let Some(paths) = self.costs.get(cost){
                good_paths = if let Some(allowed_paths) = &self.allowed_paths{
                    paths.intersection(&allowed_paths).copied().collect()
                } else { paths.clone() };

                for path in &good_paths{
                    let path_docids = ctx.path_docids(*path, self.graph)?;
                    buf |= path_docids;
                    buf &= &self.candidates;
                    bucket |= &buf;
                    buf.clear();
                    self.candidates -= path_docids;
                }
            };

            Ok(RankingRuleOutput{
                score: ScoreDetails::Exactness(ExactWords{
                    exact_words: cost as u32,
                    max_exact_words: self.max_cost as u32,
                }),
                allowed_path: Some(good_paths),
                candidates: bucket
            })

        }).transpose()
    }
}


pub struct ExactnessCost;

impl Cost for ExactnessCost {
    fn cost(node: &GraphNode, _: &impl Context) -> usize {
        match &node.data {
            NodeData::Term(Term{ term_kind: TermKind::Normal(_) | TermKind::Exact(_), position, .. }) => position.clone().count(),
            _ => 0
        }
    }
}