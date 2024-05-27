use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::time::Instant;
use roaring::MultiOps;
use crate::proximity::MAX_DISTANCE;
use crate::search::query_graph::{NodeData, QueryGraph};
use crate::Result;
use crate::search::query_parser::{Term, TermKind};
use crate::search::ranking::dead_ends_cache::DeadEndsCache;
use crate::search::ranking::proximity::ProximityCost;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    pub cost: usize
}

pub type Depth = usize;

struct VisitorContext<'a, T: AsRef<[BitSet]>> {
    graph: &'a QueryGraph,
    all_costs_from_node: VecMap<VecMap<T>>,
    allowed_paths: Option<HashSet<BitSet>>,
}

struct VisitorState<'a> {
    remaining_cost: usize,
    path: &'a mut Vec<Edge>,
    visited_nodes: BitSet,
    allowed_paths: Option<&'a mut Vec<BitSet>>,
    allowed_paths_bitset: &'a mut BitSet<Vec<u64>>,
    forbidden_conditions: &'a mut HashSet<Edge>,
    visited_conditions: &'a mut HashSet<Edge>,
    dead_ends_cache: &'a mut DeadEndsCache,
    restricted_paths: &'a mut Vec<usize>
}

pub struct PathVisitor<'a, EC: EdgeToCost = ProximityCost, T: AsRef<[BitSet]> = [BitSet; MAX_DISTANCE as usize]> {
    ctx: VisitorContext<'a, T>,
    allowed_paths_buffer: Vec<BitSet>,
    dead_ends_cache: DeadEndsCache,
    allowed_paths_bitset_buffer: BitSet<Vec<u64>>,
    forbidden_conditions: HashSet<Edge>,
    visited_conditions: HashSet<Edge>,
    path_buffer: Vec<Edge>,
    restricted_paths_buffer: Vec<usize>,
    _phantom: PhantomData<EC>
}
impl<'a, EC: EdgeToCost, T: AsRef<[BitSet]>> PathVisitor<'a, EC, T> {
    pub fn new(
        graph: &'a QueryGraph,
        all_costs_from_node: VecMap<VecMap<T>>,
        allowed_paths: Option<HashSet<BitSet>>,
        dead_ends_cache: DeadEndsCache
    ) -> Self {
        Self{
            ctx: VisitorContext { graph, all_costs_from_node, allowed_paths },
            allowed_paths_buffer: Vec::new(),
            allowed_paths_bitset_buffer: BitSet::new_vec(),
            forbidden_conditions: HashSet::new(),
            dead_ends_cache,
            visited_conditions: HashSet::new(),
            path_buffer: Vec::new(),
            restricted_paths_buffer: Vec::new(),
            _phantom: Default::default(),
        }
    }
    #[inline(always)]
    pub fn set_allowed_paths(&mut self, allowed_paths: Option<HashSet<BitSet>>){
        self.ctx.allowed_paths = allowed_paths;
    }
    #[inline(always)]
    pub fn query_graph(&mut self) -> &'a QueryGraph{
        self.ctx.graph
    }
    #[inline(always)]
    pub fn visit_paths(&mut self, cost: usize, mut visit: impl (FnMut(&[Edge], &mut DeadEndsCache) -> Result<ControlFlow<()>>)) -> Result<()> {
        self.path_buffer.clear();
        self.restricted_paths_buffer.clear();
        self.allowed_paths_buffer.clear();
        self.allowed_paths_bitset_buffer.clear();
        self.forbidden_conditions.clear();
        self.allowed_paths_bitset_buffer.extend(0..self.ctx.allowed_paths.as_ref().map_or(0,|paths| paths.len()));
        let mut state = VisitorState {
            remaining_cost: cost,
            path: &mut self.path_buffer,
            allowed_paths: self.ctx.allowed_paths.as_ref().map(|paths| {
                self.allowed_paths_buffer.extend(paths);
                &mut self.allowed_paths_buffer
            }),
            allowed_paths_bitset: &mut self.allowed_paths_bitset_buffer,
            forbidden_conditions: &mut self.forbidden_conditions,
            visited_conditions: &mut self.visited_conditions,
            dead_ends_cache: &mut self.dead_ends_cache,
            restricted_paths: &mut self.restricted_paths_buffer,
            visited_nodes: BitSet::new(),
        };
        let _ = state.visit_node::<EC>(self.ctx.graph.root, &mut visit, &mut self.ctx)?;

        Ok(())
    }
}

pub trait EdgeToCost{
    #[inline(always)]
    fn to_cost(edge: usize, node_id: usize, graph: &QueryGraph) -> usize;
}

impl<'a> VisitorState<'a> {
    #[inline(always)]
    fn visit_node<EC: EdgeToCost>(
        &mut self,
        from_node: usize,
        visit: &mut impl (FnMut(&[Edge], &mut DeadEndsCache) -> Result<ControlFlow<()>>),
        ctx: &VisitorContext<impl AsRef<[BitSet]>>,
    ) -> Result<ControlFlow<(), bool>> {
        if from_node == ctx.graph.end {
            if let Some(allowed_paths) = &self.allowed_paths{
                for path in self.allowed_paths_bitset.iter(){
                    let mut path = allowed_paths[path];
                    let path = path.difference(&self.visited_nodes);
                    let is_allowed = if path.len() > 1 {
                        false
                    } else {
                        path.contains(from_node)
                    };
                    if is_allowed{
                        let control_flow = visit(self.path, self.dead_ends_cache)?;
                        return match control_flow {
                            ControlFlow::Continue(_) => Ok(ControlFlow::Continue(true)),
                            ControlFlow::Break(_) => Ok(ControlFlow::Break(())),
                        }
                    }
                }
                return Ok(ControlFlow::Continue(false))
            }

            let control_flow = visit(self.path, self.dead_ends_cache)?;
            return match control_flow {
                ControlFlow::Continue(_) => Ok(ControlFlow::Continue(true)),
                ControlFlow::Break(_) => Ok(ControlFlow::Break(())),
            }
        }

        let mut any_valid = false;
        let restricted_paths_len = self.restricted_paths.len();

        if let Some(allowed_paths) = &mut self.allowed_paths {
            for path_idx in self.allowed_paths_bitset.iter(){
                let mut path = allowed_paths[path_idx];

                let is_allowed = path.difference(&self.visited_nodes).contains(from_node);
                if !is_allowed{
                    self.restricted_paths.push(path_idx);
                }
            }
            let _ = self.restricted_paths[restricted_paths_len..].iter().map(|&x| {
                self.allowed_paths_bitset.remove(x);
            });


        }

        if self.allowed_paths.is_none() || self.allowed_paths_bitset.len() > 0 {
            self.visited_nodes.insert(from_node);
            let node = if let NodeData::Term(Term{ term_kind: TermKind::Derivative(_, orig), .. })   = &ctx.graph.nodes[from_node].data{
                *orig
            } else {
                from_node
            };
            let costs = &ctx.all_costs_from_node[node];
            if let Some(paths) = costs.get(self.remaining_cost) {
                'outer: for (edge, next_nodes) in paths.as_ref().iter().enumerate() {
                    for to_node in next_nodes.iter() {
                        let cost = EC::to_cost(edge, to_node, ctx.graph);
                        let edge = Edge {
                            from: from_node,
                            to: to_node,
                            cost: edge
                        };

                        if self.forbidden_conditions.contains(&edge)
                        {
                            return Ok(ControlFlow::Continue(false));
                        }

                        if self.remaining_cost < cost{
                            continue
                        }
                        self.remaining_cost -= cost;

                        self.path.push(edge);
                        self.visited_conditions.insert(edge);

                        let old_forb_cond = self.forbidden_conditions.clone();
                        if let Some(next_forbidden) =
                            self.dead_ends_cache.forbidden_conditions_after_prefix(self.path.iter().copied())
                        {
                            self.forbidden_conditions.extend(next_forbidden.iter().copied());
                        }

                        let cf = self.visit_node::<EC>(to_node, visit, ctx)?;

                        *self.forbidden_conditions = old_forb_cond;
                        self.visited_conditions.remove(&edge);
                        self.path.pop();

                        self.remaining_cost += cost;

                        let ControlFlow::Continue(next_any_valid) = cf else {
                            return Ok(ControlFlow::Break(()));
                        };
                        any_valid |= next_any_valid;
                        if next_any_valid {
                            // backtrack as much as possible if a valid path was found and the dead_ends_cache
                            // was updated such that the current prefix is now invalid
                            *self.forbidden_conditions = self
                                .dead_ends_cache
                                .forbidden_conditions_for_all_prefixes_up_to(self.path.iter().copied());
                            if !self.visited_conditions.is_disjoint(&self.forbidden_conditions) {
                                break 'outer;
                            }
                        }

                    }
                }
            }
            self.visited_nodes.remove(from_node);
        }

        if let Some(_) = &mut self.allowed_paths {
            self.allowed_paths_bitset.extend(self.restricted_paths.drain(restricted_paths_len..));
        }

        Ok(ControlFlow::Continue(any_valid))
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;
    use crate::search::query_graph::tests::TestContext;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use crate::search::ranking::proximity_cost::paths_cost;
    use analyzer::analyzer::Analyzer;
    use super::*;

    #[test]
    fn visit() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world World ");
        let parsed_query = parse_query(stream);
        let time = Instant::now();
        let mut query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph build {:?}", elapsed);
        let time = Instant::now();
        let mut costs = paths_cost(&query_graph, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        let mut path: PathVisitor = PathVisitor::new(&mut query_graph, costs, None, DeadEndsCache::new(10));

        let time = Instant::now();
        path.visit_paths(9, |x, dead| {
           println!("{x:?}");

            Ok(ControlFlow::Continue(()))
        }).unwrap();
        let elapsed = time.elapsed();
        println!("Graph visit {:?}", elapsed);

    }
}

