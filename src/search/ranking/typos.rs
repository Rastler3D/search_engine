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
use crate::score_details::{ ScoreDetails, Typo};
use crate::search::query_parser::{DerivativeTerm, Term, TermKind};
use crate::search::ranking::path_visitor::Edge;

pub struct TypoRule<'graph>{
    costs: VecMap<HashSet<BitSet>>,
    candidates: RoaringBitmap,
    allowed_paths: Option<HashSet<BitSet>>,
    graph: &'graph QueryGraph,
    cur_cost: RangeInclusive<usize>,
    max_cost: usize
}

impl<'graph> TypoRule<'graph> {
    pub fn new(context: &mut impl Context, graph: &'graph QueryGraph) -> Result<Self>{
        let paths = paths_cost::<TypoCost>(graph, context);
        let max_cost = Self::max_cost(context, graph)?;
        let mut costs = VecMap::with_capacity(max_cost);
        for (path, cost) in paths{
            costs.get_or_insert_with(cost, || HashSet::new()).insert(path);
        }

        Ok(TypoRule{ costs, candidates: RoaringBitmap::new(), allowed_paths: None, graph, cur_cost: (max_cost+1..=max_cost), max_cost })
    }
    pub fn max_cost(_: &impl Context, graph: &QueryGraph) -> Result<usize>{
        Ok(graph.query_max_typos)
    }
}


impl RankingRule for TypoRule<'_> {

    fn start_iteration(&mut self, _ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()> {
        self.candidates = candidates;
        self.allowed_paths = allowed_paths;
        self.cur_cost = 0..=self.max_cost;

        Ok(())
    }

    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>> {
        self.cur_cost.next().map(|cost| -> Result<RankingRuleOutput> {
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
                score: ScoreDetails::Typo(Typo{
                    typo_count: cost as u32,
                    max_typo_count: self.max_cost as u32,
                }),
                allowed_path: Some(good_paths),
                candidates: bucket
            })

        }).transpose()
    }
}

pub struct TypoCost;

impl Cost for TypoCost {
    fn cost(node: &GraphNode, search_context: &impl Context) -> usize {
        match node.data {
            NodeData::Term(Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(_), ..), .. }) => 1,
            NodeData::Term(Term{ term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(_, typos) | DerivativeTerm::Typo(_, typos), ..), .. }) => typos as usize,
            _ => 0
        }
    }
}