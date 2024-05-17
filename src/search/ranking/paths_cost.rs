use polonius_the_crab::{polonius, polonius_return};
use crate::search::context::Context;
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{DerivativeTerm, Term, TermKind};
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;



pub trait Cost {

    fn cost(node: &GraphNode, search_context: &impl Context) -> usize;
}

pub fn paths_cost<C: Cost>(graph: &QueryGraph, search_context: &impl Context) -> Vec<(BitSet, usize)>{
    let mut context = TraverseContext{
        cost: VecMap::with_capacity(graph.nodes.len()),
        visited: VecMap::with_capacity(graph.nodes.len()),
    };

    let (prev_paths, cost) = graph_traverse::<C>(graph.root, graph, &mut context, search_context);
    prev_paths.iter().map(|&(mut bitset, prev_cost)| {
        bitset.insert(graph.root);
        (bitset, cost + prev_cost)
    }).collect()

}

struct TraverseContext{
    cost: VecMap<usize>,
    visited: VecMap<Vec<(BitSet, usize)>>,
}



fn graph_traverse<'a, 'b, C: Cost>(node_id: usize, graph: &QueryGraph, mut traverse_context: &'a mut TraverseContext, search_context: &'a impl Context<'_>) -> (&'a [(BitSet, usize)], usize){
    let node = &graph.nodes[node_id];

    let cost = *traverse_context.cost.get_or_insert_with(node_id, || C::cost(node, search_context));
    match &node.data {
        NodeData::Term(Term{ term_kind: TermKind::Derivative(_, original_term_node), .. }) => {
            let (paths, _) = graph_traverse::<C>(*original_term_node, graph, traverse_context, search_context);

            return (paths, cost);
        },
        NodeData::End => {
            const EMPTY: (&'static [(BitSet, usize)], usize) = (&[(BitSet::<[_;2]>::new(),0)], 0);
            return EMPTY
        },
        _ => {
            polonius!(|traverse_context| -> (&'polonius [(BitSet,usize)], usize){
                if let Some(paths) = traverse_context.visited.get(node_id){
                    polonius_return!((paths, cost));
                }
            })
        },
    }

    let mut paths = Vec::new();
    for successor_id in node.successors.iter() {
        let (prev_paths, cost) = graph_traverse::<C>(successor_id, graph, traverse_context, search_context);
        prev_paths.iter().map(|&(mut bitset, prev_cost)| {
            bitset.insert(successor_id);
            (bitset, cost + prev_cost)
        }).collect_into(&mut paths);
    };
    let paths = traverse_context.visited.get_or_insert(node_id, paths);

    (paths, cost)

}


#[cfg(test)]
mod tests {
    use std::time::Instant;
    use crate::search::query_graph::tests::TestContext;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use analyzer::analyzer::Analyzer;
    use crate::search::ranking::typos::TypoCost;
    use super::*;

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
        println!("{:?}", query_graph);
        let mut costs = paths_cost::<TypoCost>(&query_graph, &mut context);
        costs.sort_unstable_by_key(|x| x.1);
        let elapsed = time.elapsed();
        println!("Graph cost {:?}", elapsed);
        println!("{:#?}", costs);
    }
}