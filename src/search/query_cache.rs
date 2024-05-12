use fxhash::FxHashMap;
use roaring::RoaringBitmap;
use crate::search::query_graph::QueryGraph;
use crate::search::resolve_query_graph::{resolve_node_docids, resolve_path_docids};
use crate::search::search::SearchContext;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use crate::Result;

#[derive(Default)]
pub struct QueryCache {
    pub path_cache: FxHashMap<BitSet, RoaringBitmap>,
    pub node_cache: VecMap<RoaringBitmap>
}
impl<'ctx> SearchContext<'ctx> {
    pub fn get_path_docids(&mut self, path: BitSet, graph: &QueryGraph) -> Result<&RoaringBitmap>{
        if self.query_cache.path_cache.contains_key(&path) {
            return Ok(&self.query_cache.path_cache[&path]);
        };
        let docids = resolve_path_docids(path, graph, self)?;
        let _ = self.query_cache.path_cache.insert(path, docids);
        let docids = &self.query_cache.path_cache[&path];
        Ok(docids)
    }

    pub fn get_node_docids(&mut self, node_id: usize, graph: &QueryGraph) -> Result<&RoaringBitmap>{
        if let Some(key) = self.query_cache.node_cache.get(node_id) {
            return Ok(key)
        };
        let docids = resolve_node_docids(&graph.nodes[node_id], self)?;
        let _ = self.query_cache.node_cache.insert(node_id, docids);
        let docids = &self.query_cache.path_cache[&node_id];
        Ok(docids)
    }
}