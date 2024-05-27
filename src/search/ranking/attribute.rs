use std::collections::{HashMap, HashSet};
use std::ops::{ControlFlow, RangeInclusive};
use std::time::Instant;
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use crate::search::context::{Context, Fid};
use crate::search::query_graph::{NodeData, QueryGraph};
use crate::search::ranking::path_visitor::{Edge, EdgeToCost, PathVisitor};
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::resolve_query_graph::{resolve_docids, resolve_docids_proximity, resolve_fid_docids};
use crate::search::utils::bit_set::BitSet;
use crate::{FieldId, Result};
use crate::score_details::{Attribute, ScoreDetails};
use crate::search::ranking::attribute_cost::{MAX_ATTRIBUTE, paths_cost};
use crate::search::ranking::dead_ends_cache::DeadEndsCache;
use crate::search::utils::vec_map::VecMap;

pub struct AttributeRule<'graph> {
    edge_docids: HashMap<Edge, RoaringBitmap>,
    path_visitor: PathVisitor<'graph, AttributeCost, SmallVec<[BitSet; MAX_ATTRIBUTE]>>,
    cost_field_id_mapping: VecMap<FieldId>,
    cur_cost: RangeInclusive<usize>,
    candidates: RoaringBitmap,
    max_cost: usize
}

impl<'graph> AttributeRule<'graph> {
    pub fn new(context: &mut impl Context, graph: &'graph QueryGraph) -> Result<Self>{
        let costs = paths_cost(graph, context)?;
        let max_cost = Self::max_cost(context, graph)?;
        let path_visitor = PathVisitor::new(graph, costs, None, DeadEndsCache::new(100));
        let cost_field_id_mapping = if let Some(searchable_field) = context.searchable_fields_ids()?{
            (0..).zip(searchable_field).collect::<VecMap<_>>()
        } else {
            (0..).zip(0..=max_cost as FieldId).collect::<VecMap<_>>()
        };
        Ok(AttributeRule{ edge_docids: HashMap::new(), cur_cost: max_cost+1..=max_cost, candidates: RoaringBitmap::new(), max_cost, path_visitor, cost_field_id_mapping })
    }
    pub fn max_cost(context: &mut impl Context, graph: &QueryGraph) -> Result<usize>{
        let max_cost: usize = {
            if let Some(attributes) = context
                .searchable_fields_ids()?
                .map(|x| x.len())
            {
                attributes.saturating_sub(1)
            } else {
                context.field_ids()?.ids().max().unwrap_or_default() as usize
            }
        };

        Ok((graph.query_words) * max_cost)
    }
}


impl<'graph> RankingRule for AttributeRule<'graph> {
    fn start_iteration(&mut self, _ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()> {
        self.candidates = candidates;
        self.path_visitor.set_allowed_paths(allowed_paths);
        self.cur_cost = 0..=self.max_cost;

        Ok(())
    }

    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>> {
        self.cur_cost.next().map(|cost| -> Result<RankingRuleOutput> {
            let mut subpaths_docids: Vec<(Edge, RoaringBitmap)> = vec![];
            let mut good_paths = HashSet::new();
            let mut bucket = RoaringBitmap::new();
            let query_graph = self.path_visitor.query_graph();
            let mut time = Instant::now();
            self.path_visitor.visit_paths(cost, |path, dead_ends_cache|{
                println!("Path found {:?}", time.elapsed());
                if self.candidates.is_empty() {
                    return Ok(ControlFlow::Break(()));
                }

                let idx_of_first_different_condition = {
                    let mut idx = 0;
                    for (&last_c, cur_c) in path.iter().zip(subpaths_docids.iter().map(|x| x.0)) {
                        if last_c == cur_c {
                            idx += 1;
                        } else {
                            break;
                        }
                    }
                    subpaths_docids.truncate(idx);
                    idx
                };

                for latest_edge in path[idx_of_first_different_condition..].iter().copied() {
                    let success = visit_path_edge(
                        ctx,
                        &query_graph,
                        &self.candidates,
                        dead_ends_cache,
                        &mut self.edge_docids,
                        &mut subpaths_docids,
                        &self.cost_field_id_mapping,
                        latest_edge
                    )?;
                    if !success {
                        return Ok(ControlFlow::Continue(()));
                    }
                }

                let path_docids =
                    subpaths_docids.pop().map(|x| x.1).unwrap_or_else(|| self.candidates.clone()) & &self.candidates;

                if path_docids.is_empty(){
                    return Ok(ControlFlow::Continue(()));
                }

                let mut path_bitset = BitSet::new();
                for edge in path{
                    path_bitset.insert(edge.from);
                }
                path.last().map(|last| path_bitset.insert(last.to));
                good_paths.insert(path_bitset);

                bucket |= &path_docids;

                self.candidates -= &path_docids;
                // for (_, docids) in subpaths_docids.iter_mut() {
                //     *docids -= &path_docids;
                // }

                if self.candidates.is_empty() {
                    time = Instant::now();
                    Ok(ControlFlow::Break(()))
                } else {
                    time = Instant::now();
                    Ok(ControlFlow::Continue(()))
                }

            })?;

            Ok(RankingRuleOutput{
                score: ScoreDetails::Attribute(Attribute{
                    attribute: cost as u32,
                    max_attribute: self.max_cost as u32
                }),
                allowed_path: Some(good_paths),
                candidates: bucket
            })

        }).transpose()

    }
}


fn visit_path_edge(
    ctx: &mut (impl Context + ?Sized),
    graph: &QueryGraph,
    candidates: &RoaringBitmap,
    dead_ends_cache: &mut DeadEndsCache,
    edge_docids: &mut HashMap<Edge, RoaringBitmap>,
    subpath: &mut Vec<(Edge, RoaringBitmap)>,
    mapping: &VecMap<Fid>,
    latest_edge: Edge,
) -> Result<bool> {
    let edge_docids = get_edge_docids(edge_docids, ctx, latest_edge, graph, mapping)?;
    if edge_docids.is_empty() {
        //dead_ends_cache.forbid_condition(latest_edge);
        return Ok(false);
    }

    let mut latest_path_docids = if let Some((_, prev_docids)) = subpath.last() {
        prev_docids & edge_docids
    } else {
        edge_docids.clone()
    };
    if !latest_path_docids.is_empty() {
        subpath.push((latest_edge, latest_path_docids));
        return Ok(true);
    }

    //dead_ends_cache.forbid_condition_after_prefix(subpath.iter().map(|x| x.0), latest_edge);
    if subpath.len() <= 1 {
        return Ok(false);
    }

    //let mut subprefix = vec![];
    // Deadend if the intersection between this edge and any
    // previous prefix is disjoint with the universe
    // We already know that the intersection with the last one
    // is empty,
    // for (past_condition, sp_docids) in subpath[..subpath.len() - 1].iter() {
    //     subprefix.push(*past_condition);
    //     if edge_docids.is_disjoint(sp_docids) {
    //         dead_ends_cache
    //             .forbid_condition_after_prefix(subprefix.iter().copied(), latest_edge);
    //     }
    // }

    Ok(false)
}

fn resolve_edge(ctx: &mut (impl Context + ?Sized), edge: Edge, graph: &QueryGraph, mapping: &VecMap<Fid>) -> Result<RoaringBitmap>{
    let right = &graph.nodes[edge.to].data;
    return match right {
        NodeData::Term(term) => {
            let docids = resolve_fid_docids(term, ctx, mapping[edge.cost])?;
            Ok(docids)
        }
        (NodeData::Start | NodeData::End) => {
            let docids = ctx.all_docids()?;
            Ok(docids)
        }
    };
}

pub fn get_edge_docids<'s>(
    cache: &'s mut HashMap<Edge, RoaringBitmap>,
    ctx: &mut (impl Context + ?Sized),
    edge: Edge,
    graph: &QueryGraph,
    mapping: &VecMap<Fid>
) -> Result<&'s RoaringBitmap> {
    if cache.contains_key(&edge) {
        let docids = cache.get_mut(&edge).unwrap();
        return Ok(docids);
    }
    let edge_docids = resolve_edge(ctx, edge, graph, mapping)?;

    let _ = cache.insert(edge, edge_docids);
    let edge_docids = &cache[&edge];
    Ok(edge_docids)
}

struct AttributeCost;
impl EdgeToCost for AttributeCost {
    #[inline(always)]
    fn to_cost(edge: usize, node_id: usize, graph: &QueryGraph) -> usize {
        match &graph.nodes[node_id].data{
            NodeData::Start | NodeData::End => edge,
            NodeData::Term(term) => edge * term.position.clone().count()
        }
    }
}