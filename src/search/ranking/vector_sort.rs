use std::collections::HashSet;
use std::iter::FromIterator;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::score_details::{self, ScoreDetails};
use crate::vector::{DistributionShift, Embedder};
use crate::{DocumentId, Result};
use crate::search::context::Context;
use crate::search::query_graph::QueryGraph;
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::utils::bit_set::BitSet;

pub struct VectorSort {
    allowed_paths: Option<HashSet<BitSet>>,
    target: Vec<f32>,
    candidates: RoaringBitmap,
    vector_candidates: RoaringBitmap,
    cached_sorted_docids: std::vec::IntoIter<(DocumentId, f32)>,
    limit: usize,
    distribution_shift: Option<DistributionShift>,
    embedder_index: u8,
}

impl VectorSort {
    pub fn new(
        ctx: &mut impl Context,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
        limit: usize,
        embedder_name: &str,
        embedder: &Embedder,
    ) -> Result<Self> {
        let embedder_index = ctx
            .embedder_category_id(embedder_name)?;

        Ok(Self {
            allowed_paths: None,
            target,
            candidates: RoaringBitmap::new(),
            vector_candidates,
            cached_sorted_docids: Default::default(),
            limit,
            distribution_shift: embedder.distribution(),
            embedder_index,
        })
    }

    fn fill_buffer(
        &mut self,
        ctx: &mut (impl Context + ?Sized),
        vector_candidates: &RoaringBitmap,
    ) -> Result<()> {
        let writer_index = (self.embedder_index as u16) << 8;
        let readers: std::result::Result<Vec<_>, _> = (0..=u8::MAX)
            .map_while(|k| {
                ctx.vector_reader(writer_index | (k as u16))
                    .map(Some)
                    .or_else(|e| match e {
                        arroy::Error::MissingMetadata => Ok(None),
                        e => Err(e),
                    })
                    .transpose()
            })
            .collect();

        let readers = readers?;

        let target = &self.target;
        let mut results = Vec::new();

        for reader in readers.iter() {
            let nns_by_vector =
                reader.nns_by_vector(ctx.txn(), target, self.limit, None, Some(vector_candidates))?;
            results.extend(nns_by_vector.into_iter());
        }
        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));
        self.cached_sorted_docids = results.into_iter();

        Ok(())
    }
}

impl<'ctx> RankingRule for VectorSort {

    fn start_iteration(&mut self, ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()>{
        self.allowed_paths = allowed_paths;
        self.candidates = candidates;
        let vector_candidates = &self.vector_candidates & &self.candidates;
        self.fill_buffer(ctx, &vector_candidates)?;
        Ok(())
    }

    fn next_bucket(&mut self, ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>>{
        if self.candidates.is_empty(){
            return Ok(None)
        }

        let vector_candidates = &self.vector_candidates & &self.candidates;

        if vector_candidates.is_empty() {
            self.candidates = RoaringBitmap::new();
            return Ok(Some(RankingRuleOutput {
                candidates: self.candidates.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
                allowed_path: self.allowed_paths.clone(),
            }));
        }

        for (docid, distance) in self.cached_sorted_docids.by_ref() {
            if vector_candidates.contains(docid) {
                let score = 1.0 - distance;
                let score = self
                    .distribution_shift
                    .map(|distribution| distribution.shift(score))
                    .unwrap_or(score);
                self.candidates.remove(docid);
                return Ok(Some(RankingRuleOutput {
                    allowed_path: self.allowed_paths.clone(),
                    candidates: RoaringBitmap::from_iter([docid]),
                    score: ScoreDetails::Vector(score_details::Vector { similarity: Some(score) }),
                }));
            }
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        self.fill_buffer(ctx, &vector_candidates)?;

        // we tried filling the buffer, but it remained empty 😢
        // it means we don't actually have any document remaining in the universe with a vector.
        // => exit
        if self.cached_sorted_docids.len() == 0 {
            self.candidates = RoaringBitmap::new();
            return Ok(Some(RankingRuleOutput {
                allowed_path: self.allowed_paths.clone(),
                candidates: self.candidates.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        self.next_bucket(ctx)
    }
}
