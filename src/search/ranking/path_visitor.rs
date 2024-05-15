use std::collections::HashSet;
use std::ops::ControlFlow;
use crate::proximity::MAX_DISTANCE;
use crate::search::query_graph::{NodeData, QueryGraph};
use crate::Result;
use crate::search::query_parser::{Term, TermKind};
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    pub cost: usize
}

pub type Depth = usize;

struct VisitorContext<'a> {
    graph: &'a QueryGraph,
    all_costs_from_node: VecMap<VecMap<[BitSet; MAX_DISTANCE as usize]>>,
    allowed_paths: Option<HashSet<BitSet>>,
}

struct VisitorState<'a> {
    remaining_cost: usize,
    path: &'a mut Vec<Edge>,
    visited_nodes: BitSet,
    allowed_paths: Option<&'a mut Vec<BitSet>>,
    allowed_paths_bitset: BitSet,
    restricted_paths: &'a mut Vec<usize>
}

pub struct PathVisitor<'a> {
    ctx: VisitorContext<'a>,
    allowed_paths_buffer: Vec<BitSet>,
    path_buffer: Vec<Edge>,
    restricted_paths_buffer: Vec<usize>
}
impl<'a> PathVisitor<'a> {
    pub fn new(
        graph: &'a QueryGraph,
        all_costs_from_node: VecMap<VecMap<[BitSet; MAX_DISTANCE as usize]>>,
        allowed_paths: Option<HashSet<BitSet>>
    ) -> Self {
        Self{
            ctx: VisitorContext { graph, all_costs_from_node, allowed_paths },
            allowed_paths_buffer: Vec::new(),
            path_buffer: Vec::new(),
            restricted_paths_buffer: Vec::new(),
        }
    }

    pub fn set_allowed_paths(&mut self, allowed_paths: Option<HashSet<BitSet>>){
        self.ctx.allowed_paths = allowed_paths;
    }

    pub fn query_graph(&mut self) -> &'a QueryGraph{
        self.ctx.graph
    }

    pub fn visit_paths(&mut self, cost: usize, mut visit: impl (FnMut(&[Edge]) -> Result<ControlFlow<()>>)) -> Result<()> {
        self.path_buffer.clear();
        self.restricted_paths_buffer.clear();
        self.allowed_paths_buffer.clear();

        let mut state = VisitorState {
            remaining_cost: cost,
            path: &mut self.path_buffer,
            allowed_paths: self.ctx.allowed_paths.as_ref().map(|paths| {
                self.allowed_paths_buffer.extend(paths);
                &mut self.allowed_paths_buffer
            }),
            allowed_paths_bitset: BitSet::<[_;2]>::init(true, self.ctx.allowed_paths.as_ref().map_or(0,|paths| paths.len())),
            restricted_paths: &mut self.restricted_paths_buffer,
            visited_nodes: BitSet::new(),
        };
        let _ = state.visit_node(self.ctx.graph.root, &mut visit, &mut self.ctx)?;

        Ok(())
    }
}

impl<'a> VisitorState<'a> {
    fn visit_node(
        &mut self,
        from_node: usize,
        visit: &mut impl (FnMut(&[Edge]) -> Result<ControlFlow<()>>),
        ctx: &mut VisitorContext,
    ) -> Result<ControlFlow<(), bool>> {
        if from_node == ctx.graph.end {
            if let Some(allowed_paths) = &self.allowed_paths{
                for path in self.allowed_paths_bitset.iter(){
                    let mut path = allowed_paths[path];
                    //let mut path = *path;
                    let path = path.difference(&self.visited_nodes);
                    let is_allowed = if path.len() > 1 {
                        false
                    } else {
                        path.contains(from_node)
                    };
                    if is_allowed{
                        let control_flow = visit(self.path)?;
                        return match control_flow {
                            ControlFlow::Continue(_) => Ok(ControlFlow::Continue(true)),
                            ControlFlow::Break(_) => Ok(ControlFlow::Break(())),
                        }
                    }
                }
                return Ok(ControlFlow::Continue(false))
            }

            let control_flow = visit(self.path)?;
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
            let restricted = BitSet::from_iter(self.restricted_paths[restricted_paths_len..].iter().copied());
            self.allowed_paths_bitset.difference(&restricted);

        }

        if self.allowed_paths.is_none() || self.allowed_paths_bitset.len() > 0 {
            self.visited_nodes.insert(from_node);
            let node = if let NodeData::Term(Term{ term_kind: TermKind::Derivative(_, orig), .. })   = &ctx.graph.nodes[from_node].data{
                *orig
            } else {
                from_node
            };
            let costs = &ctx.all_costs_from_node[node];
            if let Some(&paths) = costs.get(self.remaining_cost) {
                for (cost, next_nodes) in paths.iter().enumerate() {
                    if self.remaining_cost < cost{
                        continue
                    }
                    self.remaining_cost -= cost;
                    for to_node in next_nodes.iter() {
                        self.path.push(Edge {
                            from: from_node,
                            to: to_node,
                            cost
                        });
                        let cf = self.visit_node(to_node, visit, ctx)?;
                        self.path.pop();

                        let ControlFlow::Continue(next_any_valid) = cf else {
                            return Ok(ControlFlow::Break(()));
                        };
                        any_valid |= next_any_valid;
                    }
                    self.remaining_cost += cost;
                }
            }
            self.visited_nodes.remove(from_node);
        }

        if let Some(allowed_paths) = &mut self.allowed_paths {
            self.allowed_paths_bitset.extend(self.restricted_paths.drain(restricted_paths_len..));
        }

        Ok(ControlFlow::Continue(any_valid))
    }
}


#[cfg(test)]
mod tests {
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
        let stream = analyzer.analyze("Hello world");
        let parsed_query = parse_query(stream);
        let time = Instant::now();
        let mut query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph build {:?}", elapsed);
        let time = Instant::now();
        let mut costs = paths_cost(&query_graph, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        let time = Instant::now();
        let mut path = PathVisitor::new(&mut query_graph, costs, Some(HashSet::from([BitSet::from_iter([0,1,2,3]),BitSet::from_iter([0,1,7,3]),BitSet::from_iter([0,5,2,3]),BitSet::from_iter([0,6,7,3])])));

        path.visit_paths(3, |x| { println!("{x:?}"); Ok(ControlFlow::Continue(())) }).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);

    }
}

