use std::collections::HashMap;
use crate::search::context::{Context, Fid};
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{Term, TermKind};
use crate::search::ranking::paths_cost::Cost;
use crate::search::resolve_query_graph::{resolve_docids};
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use itertools::Itertools;
use polonius_the_crab::{polonius, polonius_return};
use roaring::RoaringBitmap;
use std::iter::{empty, successors};
use std::time::Instant;
use crate::proximity::MAX_DISTANCE;
use crate::Result;

pub fn paths_cost(
    graph: &QueryGraph,
    search_context: &mut impl Context,
) -> Result<VecMap<VecMap<[BitSet; MAX_DISTANCE as usize]>>> {
    let mut visited = VecMap::with_capacity(graph.nodes.len());
    let time = Instant::now();
    let res = graph_traverse(graph.root, graph, search_context, &mut visited)?;

    Ok(visited)
}

fn graph_traverse<'search, 'cost, 'visited>(
    node_id: usize,
    graph: &QueryGraph,
    search_context: &'search mut impl Context,
    //mut cost: &'cost mut HashMap<(usize, usize), VecMap<RoaringBitmap>>,
    mut visited: &'visited mut VecMap<VecMap<[BitSet; MAX_DISTANCE as usize]>>,
) -> Result<&'visited VecMap<[BitSet; MAX_DISTANCE as usize]>> {
    let node = &graph.nodes[node_id];


    let time = Instant::now();
    match &node.data {
        NodeData::Term(Term {
           term_kind: TermKind::Derivative(_, original_term_node), ..
        }) => {
            let result = graph_traverse(*original_term_node, graph, search_context, visited);
            return result;
        }
        _ => {
            polonius!(
                |visited| -> Result<&'polonius VecMap<[BitSet; MAX_DISTANCE as usize]>> {
                    if let Some(paths) = visited.get(node_id) {
                        polonius_return!(Ok(paths));
                    }
                }
            )
        }
    }

    let mut paths = VecMap::new();
    for successor_id in node.successors.iter() {
        if successor_id == graph.end{
            paths
                .get_or_insert_with(0, || [BitSet::new(); MAX_DISTANCE as usize])[0]
                .insert(successor_id);
            continue
        }
        let prev_paths = graph_traverse(successor_id, graph, search_context, visited)?;
        for (path_cost, _) in prev_paths.key_value() {
            for edge_cost in 0..MAX_DISTANCE as usize{
                paths
                    .get_or_insert_with(path_cost + edge_cost, || [BitSet::new(); MAX_DISTANCE as usize])[edge_cost]
                    .insert(successor_id);
            }
        }
    }
    let paths = visited.get_or_insert(node_id, paths);
    Ok(paths)
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::query_graph::tests::TestContext;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use analyzer::analyzer::Analyzer;
    use std::time::Instant;

    #[test]
    fn typos_paths_cost() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context).unwrap();
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
