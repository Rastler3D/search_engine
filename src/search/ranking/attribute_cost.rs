use crate::search::context::{Context, Fid};
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{Term, TermKind};
use crate::search::ranking::paths_cost::Cost;
//use crate::search::resolve_query_graph::resolve_positions;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use itertools::Itertools;
use polonius_the_crab::{polonius, polonius_return};
use roaring::RoaringBitmap;
use std::iter::empty;
use std::time::Instant;
use crate::Result;

pub fn paths_cost(
    graph: &QueryGraph,
    search_context: &impl Context,
) -> Result<VecMap<Vec<(BitSet, RoaringBitmap)>>> {
    let mut cost = VecMap::with_capacity(graph.nodes.len());
    let mut visited = VecMap::with_capacity(graph.nodes.len());
    println!("{:?}", graph);
    let time = Instant::now();
    let res = graph_traverse(graph.root, graph, search_context, &mut cost, &mut visited).cloned();
    println!("{:?}", time.elapsed());
    res
}

fn graph_traverse<'search, 'cost, 'visited>(
    node_id: usize,
    graph: &QueryGraph,
    search_context: &'search impl Context,
    mut cost: &'cost mut VecMap<VecMap<RoaringBitmap>>,
    mut visited: &'visited mut VecMap<VecMap<Vec<(BitSet, RoaringBitmap)>>>,
) -> Result<&'visited VecMap<Vec<(BitSet, RoaringBitmap)>>> {

    let node = &graph.nodes[node_id];
    if !cost.contains_key(node_id) {
        cost.insert(node_id, attribute_cost(node, search_context)?);
    }
    let time = Instant::now();
    match &node.data {
        NodeData::Term(Term {
            term_kind: TermKind::Derivative(_, original_term_node),
            ..
        }) => {
            let result = graph_traverse(*original_term_node, graph, search_context, cost, visited);
            return result;
        }
        NodeData::End if !visited.contains_key(node_id) =>{
            let mut vec_map = VecMap::new();
            vec_map.insert(0, vec![(BitSet::new(), search_context.all_docids()?)]);
            return Ok(visited.get_or_insert(node_id, vec_map))
        }
        _ => {
            polonius!(
                |visited| -> Result<&'polonius VecMap<Vec<(BitSet, RoaringBitmap)>>> {
                    if let Some(paths) = visited.get(node_id) {
                        polonius_return!(Ok(paths));
                    }
                }
            )
        }
    }

    let mut paths = VecMap::new();
    for successor_id in node.successors.iter() {
        let prev_paths = graph_traverse(successor_id, graph, search_context, cost, visited)?;
        let cost = &cost[successor_id];
        for (paths_cost, paths_docids) in prev_paths.key_value() {

            for (cost, docids) in cost.key_value() {
                paths
                    .get_or_insert_with(paths_cost + cost, || Vec::new())
                    .extend(paths_docids.iter().filter_map(|(x, y)| {
                        let mut x = *x;
                        x.insert(successor_id);
                        let docids = y & docids;
                        if docids.is_empty(){
                            None
                        } else {
                            Some((x, docids))
                        }

                    }))
            }
        }
    }
    let paths = visited.get_or_insert(node_id, paths);
    Ok(paths)
}

pub fn attribute_cost(
    node: &GraphNode,
    context: &impl Context,
) -> Result<VecMap<RoaringBitmap>> {
    match &node.data {
        NodeData::Start | NodeData::End => {
            let mut vec_map = VecMap::new();
            vec_map.insert(0, context.all_docids()?);
            Ok(vec_map)
        }
        NodeData::Term(term) => {
            let mut vec_map = VecMap::new();
            let positions = resolve_positions(term, context)?;
            for ((fid, _), bitmap) in positions {
                let docids = vec_map.get_or_insert_with(fid as usize, || RoaringBitmap::new());
                *docids |= bitmap;
            }
            Ok(vec_map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::query_graph::tests::TestContext;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use crate::search::ranking::paths_cost::TypoCost;
    use analyzer::analyzer::Analyzer;
    use std::time::Instant;

    #[test]
    fn typos_paths_cost() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        //let query_graph = QueryGraph::from_query(parsed_query, &context).unwrap();
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world");
        let parsed_query = parse_query(stream);
        let time = Instant::now();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph build {:?}", elapsed);
        let time = Instant::now();
        let mut costs = paths_cost(&query_graph, &mut context).unwrap();
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        println!("{:#?}", costs);
    }
}
