use std::collections::HashSet;
use heed::{BytesDecode, RoTxn};
use roaring::RoaringBitmap;

use crate::heed_codec::facet::{FacetGroupKeyCodec, OrderedF64Codec};
use crate::heed_codec::{BytesRefCodec, StrRefCodec};
use crate::score_details::{self, ScoreDetails, Sort};
use crate::search::facet::{ascending_facet_sort, descending_facet_sort};
use crate::{FieldId, Index, Result};
use crate::search::context::Context;
use crate::search::ranking::ranking_rule::{RankingRule, RankingRuleOutput};
use crate::search::utils::bit_set::BitSet;

pub trait RankingRuleOutputIter<'ctx> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput>>;
}

pub struct RankingRuleOutputIterWrapper<'ctx> {
    iter: Box<dyn Iterator<Item = Result<RankingRuleOutput>> + 'ctx>,
}
impl<'ctx> RankingRuleOutputIterWrapper<'ctx> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<RankingRuleOutput>> + 'ctx>) -> Self {
        Self { iter }
    }
}
impl<'ctx> RankingRuleOutputIter<'ctx> for RankingRuleOutputIterWrapper<'ctx> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput>> {
        match self.iter.next() {
            Some(x) => x.map(Some),
            None => Ok(None),
        }
    }
}

pub struct SortRule<'ctx> {
    txn: &'ctx RoTxn<'ctx>,
    allowed_paths: Option<HashSet<BitSet>>,
    candidates: RoaringBitmap,
    field_name: String,
    field_id: Option<FieldId>,
    is_ascending: bool,
    iter: Option<RankingRuleOutputIterWrapper<'ctx>>,
}
impl<'ctx> SortRule<'ctx> {
    pub fn new(
        ctx: &impl Context<'ctx>,
        field_name: String,
        is_ascending: bool,
    ) -> Result<Self> {
        let fields_ids_map = ctx.field_ids()?;
        let field_id = fields_ids_map.id(&field_name);

        Ok(Self {
            txn: ctx.txn(),
            allowed_paths: None,
            candidates: RoaringBitmap::new(),
            field_name,
            field_id,
            is_ascending,
            iter: None,
        })
    }
}

impl<'ctx> RankingRule for SortRule<'ctx> {
    fn start_iteration(&mut self, ctx: &mut dyn Context, candidates: RoaringBitmap, allowed_paths: Option<HashSet<BitSet>>) -> Result<()>{
        let iter: RankingRuleOutputIterWrapper = match self.field_id {
            Some(field_id) => {
                let (number_iter, string_iter) = if self.is_ascending {
                    let number_iter = ctx.ascending_number_sort(self.txn, field_id, candidates.clone())?;
                    let string_iter = ctx.ascending_string_sort(self.txn, field_id, candidates.clone())?;

                    (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
                } else {
                    let number_iter = ctx.descending_number_sort(self.txn, field_id, candidates.clone())?;
                    let string_iter = ctx.descending_string_sort(self.txn, field_id, candidates.clone())?;

                    (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
                };
                let number_iter = number_iter.map(|r| -> Result<_> {
                    let (docids, bytes) = r?;
                    Ok((
                        docids,
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(
                                OrderedF64Codec::bytes_decode(bytes).expect("some number"),
                            )
                            .expect("too big float"),
                        ),
                    ))
                });
                let string_iter = string_iter.map(|r| -> Result<_> {
                    let (docids, bytes) = r?;
                    Ok((
                        docids,
                        serde_json::Value::String(
                            StrRefCodec::bytes_decode(bytes).expect("some string").to_owned(),
                        ),
                    ))
                });
                let allowed_path = allowed_paths.clone();
                let ascending = self.is_ascending;
                let field_name = self.field_name.clone();
                RankingRuleOutputIterWrapper::new(Box::new(number_iter.chain(string_iter).map(
                    move |r| {
                        let (docids, value) = r?;
                        Ok(RankingRuleOutput {
                            allowed_path: allowed_path.clone(),
                            candidates: docids,
                            score: ScoreDetails::Sort(Sort {
                                field_name: field_name.clone(),
                                ascending,
                                value,
                            }),
                        })
                    },
                )))
            }
            None => RankingRuleOutputIterWrapper::new(Box::new(std::iter::empty())),
        };
        self.candidates = candidates;
        self.allowed_paths = allowed_paths;
        self.iter = Some(iter);
        Ok(())
    }

    fn next_bucket(&mut self, _ctx: &mut dyn Context) -> Result<Option<RankingRuleOutput>> {
        if self.candidates.is_empty() && self.iter.is_none(){
            return Ok(None)
        };

        let iter = self.iter.as_mut().unwrap();
        if let Some(mut bucket) = iter.next_bucket()? {
            bucket.candidates &= &self.candidates;
            self.candidates -= &bucket.candidates;

            Ok(Some(bucket))
        } else {
            Ok(Some(RankingRuleOutput {
                allowed_path: self.allowed_paths.clone(),
                candidates: self.candidates.clone(),
                score: ScoreDetails::Sort(Sort {
                    field_name: self.field_name.clone(),
                    ascending: self.is_ascending,
                    value: serde_json::Value::Null,
                }),
            }))
        }
    }
}
