use std::cmp::max;
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
use std::ops::RangeInclusive;
use std::time::Instant;
use smallvec::{SmallVec, smallvec};
use crate::proximity::MAX_DISTANCE;
use crate::{FieldId, Result};

pub const MAX_ATTRIBUTE: usize = 8;

pub fn paths_cost(
    graph: &QueryGraph,
    context: &mut impl Context,
) -> Result<VecMap<VecMap<SmallVec<[BitSet; MAX_ATTRIBUTE]>>>> {
    let mut visited = VecMap::with_capacity(graph.nodes.len());
    let time = Instant::now();
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

    let res = graph_traverse(graph.root, graph, context, max_cost, &mut visited)?;


    println!("{:?}", time.elapsed());
    Ok(visited)
}

fn graph_traverse<'search, 'cost, 'visited>(
    node_id: usize,
    graph: &QueryGraph,
    search_context: &'search mut impl Context,
    max_cost: usize,
    mut visited: &'visited mut VecMap<VecMap<SmallVec<[BitSet; MAX_ATTRIBUTE]>>>,
) -> Result<&'visited VecMap<SmallVec<[BitSet; MAX_ATTRIBUTE]>>> {
    let node = &graph.nodes[node_id];

    let time = Instant::now();
    match &node.data {
        NodeData::Term(Term {
            term_kind: TermKind::Derivative(_, original_term_node), ..
        }) => {
            let result = graph_traverse(*original_term_node, graph, search_context, max_cost, visited);
            return result;
        }
        _ => {
            polonius!(
                |visited| -> Result<&'polonius VecMap<SmallVec<[BitSet; MAX_ATTRIBUTE]>>> {
                    if let Some(paths) = visited.get(node_id) {
                        polonius_return!(Ok(paths));
                    }
                }
            )
        }
    }

    let mut paths: VecMap<SmallVec<[BitSet; MAX_ATTRIBUTE]>> = VecMap::new();
    for successor_id in node.successors.iter() {
        if successor_id == graph.end{
            paths
                .get_or_insert_with(0, || smallvec![BitSet::new(); max_cost + 1])[0]
                .insert(successor_id);
            continue
        }
        let prev_paths = graph_traverse(successor_id, graph, search_context, max_cost, visited)?;
        for (path_cost, _) in prev_paths.key_value() {
            for attribute in 0..=max_cost{
                let edge_cost = cost(successor_id, graph, attribute);
                paths
                    .get_or_insert_with(path_cost + edge_cost, || smallvec![BitSet::new(); max_cost + 1])[attribute]
                    .insert(successor_id);
            }
        }
    }
    let paths = visited.get_or_insert(node_id, paths);
    Ok(paths)
}

pub fn cost(node_id: usize, graph: &QueryGraph, attribute: usize) -> usize{

    match &graph.nodes[node_id].data {
        NodeData::Start | NodeData::End => 0,
        NodeData::Term(term) => term.position.clone().count() * attribute

    }
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
