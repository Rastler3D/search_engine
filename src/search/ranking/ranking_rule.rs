use std::collections::{HashMap, HashSet};
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::utils::bit_set::BitSet;
use crate::Result;
use crate::score_details::ScoreDetails;

pub trait RankingRule{

    fn start_iteration(&mut self, ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()>;

    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>>;
}

#[derive(Debug)]
pub struct RankingRuleOutput{
    pub score: ScoreDetails,
    pub allowed_path: HashSet<BitSet>,
    pub candidates: RoaringBitmap
}