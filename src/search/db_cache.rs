use std::borrow::{Borrow, Cow};
use std::collections::hash_map::Entry;
use std::hash::Hash;
use fxhash::FxHashMap;
use heed::{BytesEncode, Database, RoTxn};
use heed::types::Bytes;
use roaring::RoaringBitmap;
use crate::heed_codec::{BytesDecodeOwned, StrBEU16Codec};
use crate::update::{merge_cbo_roaring_bitmaps, MergeFn};
use crate::{CboRoaringBitmapCodec, CboRoaringBitmapLenCodec, Result, U8StrStrCodec};
use crate::proximity::ProximityPrecision;
use crate::search::search::SearchContext;


#[derive(Default)]
pub struct DatabaseCache<'ctx> {
    pub word_pair_proximity_docids:
    FxHashMap<(u8, Cow<'static,str>, Cow<'static, str>), Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_pair_proximity_docids:
    FxHashMap<(u8, Cow<'static,str>, Cow<'static, str>), Option<RoaringBitmap>>,
    pub prefix_word_pair_proximity_docids:
    FxHashMap<(u8, Cow<'static,str>, Cow<'static, str>), Option<Cow<'ctx, [u8]>>>,
    pub word_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub exact_word_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub exact_word_prefix_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,

    pub words_fst: Option<fst::Set<Cow<'ctx, [u8]>>>,
    pub word_position_docids: FxHashMap<(Cow<'static, str>, u16), Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_position_docids: FxHashMap<(Cow<'static, str>, u16), Option<Cow<'ctx, [u8]>>>,
    pub word_positions: FxHashMap<String, Vec<u16>>,
    pub word_prefix_positions: FxHashMap<String, Vec<u16>>,

    pub word_fid_docids: FxHashMap<(Cow<'static, str>, u16), Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_fid_docids: FxHashMap<(Cow<'static, str>, u16), Option<Cow<'ctx, [u8]>>>,
    pub word_fids: FxHashMap<String, Vec<u16>>,
    pub word_prefix_fids: FxHashMap<String, Vec<u16>>,
}
impl<'ctx> DatabaseCache<'ctx> {
    fn get_value<'v, K1, KC, DC, KB>(
        txn: &'ctx RoTxn,
        cache_key: &KB,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<K1, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
    ) -> Result<Option<DC::DItem>>
        where
            KB: Eq + Hash + ToOwned<Owned = K1> + ?Sized,
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
            K1: Borrow<KB> + Eq + Hash
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr = db.get(txn, db_key)?.map(Cow::Borrowed);
            cache.insert(cache_key.to_owned(), bitmap_ptr);
        }

        match cache.get(cache_key).unwrap() {
            Some(Cow::Borrowed(bytes)) => DC::bytes_decode_owned(bytes)
                .map(Some)
                .map_err(heed::Error::Decoding)
                .map_err(Into::into),
            Some(Cow::Owned(bytes)) => DC::bytes_decode_owned(bytes)
                .map(Some)
                .map_err(heed::Error::Decoding)
                .map_err(Into::into),
            None => Ok(None),
        }
    }

    fn get_value_from_keys<'v, K1, KC, DC, KB>(
        txn: &'ctx RoTxn,
        cache_key: &KB,
        db_keys: &'v [KC::EItem],
        cache: &mut FxHashMap<K1, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
        merger: MergeFn,
    ) -> Result<Option<DC::DItem>>
        where
            KB: Eq + Hash + ToOwned<Owned = K1> + ?Sized,
            K1: Borrow<KB> +  Eq + Hash,
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
            KC::EItem: Sized,
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr: Option<Cow<'ctx, [u8]>> = match db_keys {
                [] => None,
                [key] => db.get(txn, key)?.map(Cow::Borrowed),
                keys => {
                    let bitmaps = keys
                        .iter()
                        .filter_map(|key| db.get(txn, key).transpose())
                        .map(|v| v.map(Cow::Borrowed))
                        .collect::<std::result::Result<Vec<Cow<[u8]>>, _>>()?;

                    if bitmaps.is_empty() {
                        None
                    } else {
                        Some(merger(&[], &bitmaps[..])?)
                    }
                }
            };

            cache.insert(cache_key.to_owned(), bitmap_ptr);
        }

        match cache.get(cache_key).unwrap() {
            Some(Cow::Borrowed(bytes)) => DC::bytes_decode_owned(bytes)
                .map(Some)
                .map_err(heed::Error::Decoding)
                .map_err(Into::into),
            Some(Cow::Owned(bytes)) => DC::bytes_decode_owned(bytes)
                .map(Some)
                .map_err(heed::Error::Decoding)
                .map_err(Into::into),
            None => Ok(None),
        }
    }
}

impl<'ctx> SearchContext<'ctx> {
    pub fn get_words_fst(&mut self) -> Result<fst::Set<Cow<'ctx, [u8]>>> {
        if let Some(fst) = self.db_cache.words_fst.clone() {
            Ok(fst)
        } else {
            let fst = self.index.words_fst(self.txn)?;
            self.db_cache.words_fst = Some(fst.clone());
            Ok(fst)
        }
    }

    pub fn word_docids(&mut self, word: &str) -> Result<Option<RoaringBitmap>> {
        self.get_db_word_docids(word)
    }

    /// Retrieve or insert the given value in the `word_docids` database.
    fn get_db_word_docids(&mut self, word: &str) -> Result<Option<RoaringBitmap>> {
        match &self.restricted_fids {
            Some(restricted_fids) => {
                let keys: Vec<_> =
                    restricted_fids.fields.iter().map(|fid| (word, *fid)).collect();

                DatabaseCache::get_value_from_keys::<_, _, CboRoaringBitmapCodec, _>(
                    self.txn,
                    word,
                    &keys[..],
                    &mut self.db_cache.word_docids,
                    self.index.word_fid_docids.remap_data_type::<Bytes>(),
                    merge_cbo_roaring_bitmaps,
                )
            }
            None => DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
                self.txn,
                word,
                word,
                &mut self.db_cache.word_docids,
                self.index.word_docids.remap_data_type::<Bytes>(),
            ),
        }
    }

    pub fn word_prefix_docids(&mut self, prefix: &str) -> Result<Option<RoaringBitmap>> {
        self.get_db_word_prefix_docids(prefix)
    }

    /// Retrieve or insert the given value in the `word_prefix_docids` database.
    fn get_db_word_prefix_docids(
        &mut self,
        prefix: &str,
    ) -> Result<Option<RoaringBitmap>> {
        match &self.restricted_fids {
            Some(restricted_fids) => {
                let keys: Vec<_> =
                    restricted_fids.fields.iter().map(|fid| (prefix, *fid)).collect();
                DatabaseCache::get_value_from_keys::<_, _, CboRoaringBitmapCodec, _>(
                    self.txn,
                    prefix,
                    &keys[..],
                    &mut self.db_cache.word_prefix_docids,
                    self.index.word_prefix_fid_docids.remap_data_type::<Bytes>(),
                    merge_cbo_roaring_bitmaps,
                )
            }

            None => DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
                self.txn,
                prefix,
                prefix,
                &mut self.db_cache.word_prefix_docids,
                self.index.word_prefix_docids.remap_data_type::<Bytes>(),
            )
        }
    }

    pub fn get_db_word_pair_proximity_docids(
        &mut self,
        word1: &str,
        word2: &str,
        proximity: u8,
    ) -> Result<Option<RoaringBitmap>> {
        match self.index.proximity_precision(self.txn)?.unwrap_or_default() {
            ProximityPrecision::ByAttribute => {
                // Force proximity to 0 because:
                // in ByAttribute, there are only 2 possible distances:
                // 1. words in same attribute: in that the DB contains (0, word1, word2)
                // 2. words in different attributes: no DB entry for these two words.
                let proximity = 0;
                let docids = if let Some(docids) =
                    self.db_cache.word_pair_proximity_docids.get(&(proximity, word1.into(), word2.into()))
                {
                    docids
                        .as_ref()
                        .map(|d| CboRoaringBitmapCodec::bytes_decode_owned(d))
                        .transpose()
                        .map_err(heed::Error::Decoding)?
                } else {
                    // Compute the distance at the attribute level and store it in the cache.
                    let fids = if let Some(fids) = self.index.searchable_fields_ids(self.txn)? {
                        fids
                    } else {
                        self.index.fields_ids_map(self.txn)?.ids().collect()
                    };
                    let mut docids = RoaringBitmap::new();
                    for fid in fids {
                        // for each field, intersect left word bitmap and right word bitmap,
                        // then merge the result in a global bitmap before storing it in the cache.
                        let word1_docids = self.get_db_word_fid_docids(word1, fid)?;
                        let word2_docids = self.get_db_word_fid_docids(word2, fid)?;
                        if let (Some(word1_docids), Some(word2_docids)) =
                            (word1_docids, word2_docids)
                        {
                            docids |= word1_docids & word2_docids;
                        }
                    }
                    let encoded = CboRoaringBitmapCodec::bytes_encode(&docids)
                        .map(Cow::into_owned)
                        .map(Cow::Owned)
                        .map(Some)
                        .map_err(heed::Error::Decoding)?;
                    self.db_cache
                        .word_pair_proximity_docids
                        .insert((proximity, word1.to_string().into(), word2.to_string().into()), encoded);
                    Some(docids)
                };

                Ok(docids)
            }
            ProximityPrecision::ByWord => DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec,_>(
                self.txn,
                &(proximity, word1.into(), word2.into()),
                &(
                    proximity,
                    word1,
                    word2,
                ),
                &mut self.db_cache.word_pair_proximity_docids,
                self.index.word_pair_proximity_docids.remap_data_type::<Bytes>(),
            ),
        }
    }

    pub fn get_db_word_pair_proximity_docids_len(
        &mut self,
        word1: &str,
        word2: &str,
        proximity: u8,
    ) -> Result<Option<u64>> {
        match self.index.proximity_precision(self.txn)?.unwrap_or_default() {
            ProximityPrecision::ByAttribute => Ok(self
                .get_db_word_pair_proximity_docids(word1, word2, proximity)?
                .map(|d| d.len())),
            ProximityPrecision::ByWord => {
                DatabaseCache::get_value::<_, _, CboRoaringBitmapLenCodec,_>(
                    self.txn,
                    &(proximity, word1.into(), word2.into()),
                    &(proximity, word1, word2),
                    &mut self.db_cache.word_pair_proximity_docids,
                    self.index.word_pair_proximity_docids.remap_data_type::<Bytes>(),
                )
            }
        }
    }

    pub fn get_db_word_prefix_pair_proximity_docids(
        &mut self,
        word1: &str,
        prefix2: &str,
        mut proximity: u8,
    ) -> Result<Option<RoaringBitmap>> {
        let proximity_precision = self.index.proximity_precision(self.txn)?.unwrap_or_default();
        if proximity_precision == ProximityPrecision::ByAttribute {
            // Force proximity to 0 because:
            // in ByAttribute, there are only 2 possible distances:
            // 1. words in same attribute: in that the DB contains (0, word1, word2)
            // 2. words in different attributes: no DB entry for these two words.
            proximity = 0;
        }

        let docids = if let Some(docids) =
            self.db_cache.word_prefix_pair_proximity_docids.get(&(proximity, word1.into(), prefix2.into()))
        {
            docids.clone()
        } else {
            let prefix_docids = match proximity_precision {
                ProximityPrecision::ByAttribute => {
                    // Compute the distance at the attribute level and store it in the cache.
                    let fids = if let Some(fids) = self.index.searchable_fields_ids(self.txn)? {
                        fids
                    } else {
                        self.index.fields_ids_map(self.txn)?.ids().collect()
                    };
                    let mut prefix_docids = RoaringBitmap::new();
                    // for each field, intersect left word bitmap and right word bitmap,
                    // then merge the result in a global bitmap before storing it in the cache.
                    for fid in fids {
                        let word1_docids = self.get_db_word_fid_docids(word1, fid)?;
                        let prefix2_docids = self.get_db_word_prefix_fid_docids(prefix2, fid)?;
                        if let (Some(word1_docids), Some(prefix2_docids)) =
                            (word1_docids, prefix2_docids)
                        {
                            prefix_docids |= word1_docids & prefix2_docids;
                        }
                    }
                    prefix_docids
                }
                ProximityPrecision::ByWord => {
                    // compute docids using prefix iter and store the result in the cache.
                    let key = U8StrStrCodec::bytes_encode(&(
                        proximity,
                        word1,
                        prefix2,
                    ))
                        .unwrap()
                        .into_owned();
                    let mut prefix_docids = RoaringBitmap::new();
                    let remap_key_type = self
                        .index
                        .word_pair_proximity_docids
                        .remap_key_type::<Bytes>()
                        .prefix_iter(self.txn, &key)?;
                    for result in remap_key_type {
                        let (_, docids) = result?;

                        prefix_docids |= docids;
                    }
                    prefix_docids
                }
            };
            self.db_cache
                .word_prefix_pair_proximity_docids
                .insert((proximity, word1.into(), prefix2.into()), Some(prefix_docids.clone()));
            Some(prefix_docids)
        };
        Ok(docids)
    }

    pub fn get_db_prefix_word_pair_proximity_docids(
        &mut self,
        left_prefix: &str,
        right: &str,
        proximity: u8,
    ) -> Result<Option<RoaringBitmap>> {
        // only accept exact matches on reverted positions
        self.get_db_word_pair_proximity_docids(left_prefix, right, proximity)
    }

    pub fn get_db_word_fid_docids(
        &mut self,
        word: &str,
        fid: u16,
    ) -> Result<Option<RoaringBitmap>> {
        // if the requested fid isn't in the restricted list, return None.
        if self.restricted_fids.as_ref().map_or(false, |fids| !fids.contains(&fid)) {
            return Ok(None);
        }

        DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
            self.txn,
            &(word.into(), fid),
            &(word,fid),
            &mut self.db_cache.word_fid_docids,
            self.index.word_fid_docids.remap_data_type::<Bytes>(),
        )
    }

    pub fn get_db_word_prefix_fid_docids(
        &mut self,
        word_prefix: &str,
        fid: u16,
    ) -> Result<Option<RoaringBitmap>> {
        // if the requested fid isn't in the restricted list, return None.
        if self.restricted_fids.as_ref().map_or(false, |fids| !fids.contains(&fid)) {
            return Ok(None);
        }

        DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
            self.txn,
            &(word_prefix.into(), fid),
            &(word_prefix, fid),
            &mut self.db_cache.word_prefix_fid_docids,
            self.index.word_prefix_fid_docids.remap_data_type::<Bytes>(),
        )
    }

    pub fn get_db_word_fids(&mut self, word: &str) -> Result<Vec<u16>> {
        let fids = match self.db_cache.word_fids.get(word) {
            Some(fids) => fids.clone(),
            None => {
                let mut key = word.as_bytes().to_owned();
                key.push(0);
                let mut fids = vec![];
                let remap_key_type = self
                    .index
                    .word_fid_docids
                    .remap_types::<Bytes, Bytes>()
                    .prefix_iter(self.txn, &key)?
                    .remap_key_type::<StrBEU16Codec>();
                for result in remap_key_type {
                    let ((_, fid), value) = result?;
                    // filling other caches to avoid searching for them again
                    self.db_cache.word_fid_docids.insert((word.into(), fid), Some(Cow::Borrowed(value)));
                    fids.push(fid);
                }
                self.db_cache.word_fids.insert(word.to_string(), fids.clone());
                fids
            }
        };
        Ok(fids)
    }

    pub fn get_db_word_prefix_fids(&mut self, word_prefix: &str) -> Result<Vec<u16>> {
        let fids = match self.db_cache.word_prefix_fids.get(word_prefix) {
            Some(fids) => fids.clone(),
            None => {
                let mut key = word_prefix.as_bytes().to_owned();
                key.push(0);
                let mut fids = vec![];
                let remap_key_type = self
                    .index
                    .word_prefix_fid_docids
                    .remap_types::<Bytes, Bytes>()
                    .prefix_iter(self.txn, &key)?
                    .remap_key_type::<StrBEU16Codec>();
                for result in remap_key_type {
                    let ((_, fid), value) = result?;
                    // filling other caches to avoid searching for them again
                    self.db_cache.word_prefix_fid_docids.insert((word_prefix.into(), fid), Some(Cow::Borrowed(value)));
                    fids.push(fid);
                }
                self.db_cache.word_prefix_fids.insert(word_prefix.to_string(), fids.clone());
                fids
            }
        };
        Ok(fids)
    }

    pub fn get_db_word_position_docids(
        &mut self,
        word: &str,
        position: u16,
    ) -> Result<Option<RoaringBitmap>> {
        DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
            self.txn,
            &(word.into(), position),
            &(word, position),
            &mut self.db_cache.word_position_docids,
            self.index.word_position_docids.remap_data_type::<Bytes>(),
        )
    }

    pub fn get_db_word_prefix_position_docids(
        &mut self,
        word_prefix: &str,
        position: u16,
    ) -> Result<Option<RoaringBitmap>> {
        DatabaseCache::get_value::<_, _, CboRoaringBitmapCodec, _>(
            self.txn,
            &(word_prefix.into(), position),
            &(word_prefix, position),
            &mut self.db_cache.word_prefix_position_docids,
            self.index.word_prefix_position_docids.remap_data_type::<Bytes>(),
        )
    }

    pub fn get_db_word_positions(&mut self, word: &str) -> Result<Vec<u16>> {
        let positions = match self.db_cache.word_positions.get(word) {
            Some(positions) => positions.clone(),
            None => {
                let mut key = word.as_bytes().to_owned();
                key.push(0);
                let mut positions = vec![];
                let remap_key_type = self
                    .index
                    .word_position_docids
                    .remap_types::<Bytes, Bytes>()
                    .prefix_iter(self.txn, &key)?
                    .remap_key_type::<StrBEU16Codec>();
                for result in remap_key_type {
                    let ((_, position), value) = result?;
                    // filling other caches to avoid searching for them again
                    self.db_cache
                        .word_position_docids
                        .insert((word.into(), position), Some(Cow::Borrowed(value)));
                    positions.push(position);
                }
                self.db_cache.word_positions.insert(word.to_string(), positions.clone());
                positions
            }
        };
        Ok(positions)
    }

    pub fn get_db_word_prefix_positions(
        &mut self,
        word_prefix: &str,
    ) -> Result<Vec<u16>> {
        let positions = match self.db_cache.word_prefix_positions.get(word_prefix) {
            Some(positions) => positions.clone(),
            None => {
                let mut key = word_prefix.as_bytes().to_owned();
                key.push(0);
                let mut positions = vec![];
                let remap_key_type = self
                    .index
                    .word_prefix_position_docids
                    .remap_types::<Bytes, Bytes>()
                    .prefix_iter(self.txn, &key)?
                    .remap_key_type::<StrBEU16Codec>();
                for result in remap_key_type {
                    let ((_, position), value) = result?;
                    // filling other caches to avoid searching for them again
                    self.db_cache
                        .word_prefix_position_docids
                        .insert((word_prefix.into(), position), Some(Cow::Borrowed(value)));
                    positions.push(position);
                }
                self.db_cache.word_prefix_positions.insert(word_prefix.to_string(), positions.clone());
                positions
            }
        };
        Ok(positions)
    }
}