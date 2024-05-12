use std::collections::HashMap;
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::ranking::score::ScoreDetails;
use crate::search::utils::bit_set::BitSet;

pub trait RankingRule{

    fn buckets(&self, candidates: Option<Vec<(BitSet, RoaringBitmap)>>) -> Box<dyn Iterator<Item = RankingRuleOutput> + '_>;
}

#[derive(Debug)]
pub struct RankingRuleOutput{
    pub(crate) score: ScoreDetails,
    pub(crate) candidates: Vec<(BitSet, RoaringBitmap)>
}