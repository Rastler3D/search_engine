use std::collections::HashMap;
use roaring::RoaringBitmap;
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::ranking::paths_cost::{paths_cost, TypoCost};
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::ranking::score::{Rank, ScoreDetails, Typo};
use crate::search::resolve_query_graph::resolve_path_docids;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;

pub struct TypoRule{
    costs: VecMap<HashMap<BitSet, RoaringBitmap>>,
    max_cost: u32
}

impl TypoRule {
    pub fn new(context: &mut impl Context, graph: &QueryGraph) -> heed::Result<Self>{
        let paths = paths_cost::<TypoCost>(graph, context);
        println!("Paths {:?}", paths);
        let max_cost = Self::max_cost(context, graph)?;
        let mut costs = VecMap::with_capacity(max_cost as usize);
        for (path, cost) in paths{
            let docids = resolve_path_docids(path, graph, context)?;
            costs.get_or_insert_with(cost, || HashMap::new()).insert(path, docids);
        }
        println!("Costs {:?}", costs);
        Ok(TypoRule{ costs, max_cost })
    }
    pub fn max_cost(_: &impl Context, graph: &QueryGraph) -> heed::Result<u32>{
        Ok((graph.query_word * 2) as u32)
    }
}


impl RankingRule for TypoRule {

    fn buckets(&self, candidates: Option<Vec<(BitSet, RoaringBitmap)>>) -> Box<dyn Iterator<Item=RankingRuleOutput> + '_> {
        if let Some(candidates) = candidates{
            Box::new(self.costs.key_value().map(move |(cost, paths)| {
                let mut new_candidates = Vec::new();
                for (path, docids) in &candidates{
                    if let Some(path_docids) = paths.get(&path){
                        new_candidates.push((*path, path_docids & docids));
                    }
                }
                RankingRuleOutput{
                    score: ScoreDetails::Typo(Typo{
                        typo_count: cost as u32,
                        max_typo_count: self.max_cost,
                    }),
                    candidates: new_candidates
                }
            }))
        } else {
            Box::new(self.costs.key_value().map(|(cost, path)| RankingRuleOutput{
                score: ScoreDetails::Typo(Typo{
                    typo_count: cost as u32,
                    max_typo_count: self.max_cost,
                }),
                candidates: path.iter().map(|(key, value)| (*key, value.clone())).collect()
            }))
        }
    }
}