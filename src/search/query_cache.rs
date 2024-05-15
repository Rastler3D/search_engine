use fxhash::FxHashMap;
use roaring::RoaringBitmap;
use crate::search::query_graph::QueryGraph;
use crate::search::resolve_query_graph::{phrase_resolve, resolve_node_docids, resolve_path_docids, split_resolve};
use crate::search::search::SearchContext;
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use crate::Result;

#[derive(Default)]
pub struct QueryCache {
    pub path_cache: FxHashMap<BitSet, RoaringBitmap>,
    pub node_cache: VecMap<RoaringBitmap>,
    pub phrase_cache: FxHashMap<Vec<String>, RoaringBitmap>,
    pub split_cache: FxHashMap<(String,String), RoaringBitmap>
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
        if self.query_cache.node_cache.contains_key(node_id) {
            return Ok(&self.query_cache.node_cache[node_id])
        };
        let docids = resolve_node_docids(&graph.nodes[node_id], self)?;
        let _ = self.query_cache.node_cache.insert(node_id, docids);
        let docids = &self.query_cache.node_cache[&node_id];
        Ok(docids)
    }

    pub fn get_phrase_docids(&mut self, phrase: &[String]) -> Result<&RoaringBitmap> {
        if self.query_cache.phrase_cache.contains_key(phrase) {
            return Ok(&self.query_cache.phrase_cache[phrase]);
        };
        let docids = phrase_resolve(phrase, self)?;
        let _ = self.query_cache.phrase_cache.insert(phrase.to_vec(), docids);
        let docids = &self.query_cache.phrase_cache[phrase];
        Ok(docids)
    }

    pub fn get_split_docids(&mut self, split: &(String,String)) -> Result<&RoaringBitmap> {
        if self.query_cache.split_cache.contains_key(split) {
            return Ok(&self.query_cache.split_cache[split]);
        };
        let docids = split_resolve(split, self)?;
        let _ = self.query_cache.split_cache.insert(split.clone(), docids);
        let docids = &self.query_cache.split_cache[split];
        Ok(docids)
    }
}