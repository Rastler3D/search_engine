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
use crate::search::context::{Fid, Position};
use crate::search::search::SearchContext;


#[derive(Default)]
pub struct DatabaseCache<'ctx> {
    pub word_pair_proximity_docids:
    FxHashMap<PairProximity<'static>, Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_pair_proximity_docids:
    FxHashMap<PairProximity<'static>, Option<RoaringBitmap>>,
    pub prefix_word_pair_proximity_docids:
    FxHashMap<PairProximity<'static>, Option<Cow<'ctx, [u8]>>>,
    pub prefix_prefix_pair_proximity_docids:
    FxHashMap<PairProximity<'static>, Option<RoaringBitmap>>,
    pub word_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub exact_word_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,
    pub exact_word_prefix_docids: FxHashMap<String, Option<Cow<'ctx, [u8]>>>,

    pub words_fst: Option<fst::Set<Cow<'ctx, [u8]>>>,
    pub word_position_docids: FxHashMap<WordPosition<'static>, Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_position_docids: FxHashMap<WordPosition<'static>, Option<Cow<'ctx, [u8]>>>,
    pub word_positions: FxHashMap<String, Vec<u16>>,
    pub word_prefix_positions: FxHashMap<String, Vec<u16>>,

    pub word_fid_docids: FxHashMap<WordFid<'static>, Option<Cow<'ctx, [u8]>>>,
    pub word_prefix_fid_docids: FxHashMap<WordFid<'static>, Option<Cow<'ctx, [u8]>>>,
    pub word_fids: FxHashMap<String, Vec<u16>>,
    pub word_prefix_fids: FxHashMap<String, Vec<u16>>,
}
impl<'ctx> DatabaseCache<'ctx> {
    fn get_value<'v, K1, KC, DC, KB>(
        txn: &'ctx RoTxn,
        cache_key: &'v KB,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<K1, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
    ) -> Result<Option<DC::DItem>>
        where
            KB: Eq + Hash + ?Sized,
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
            K1: Borrow<KB> + Eq + Hash + From<&'v KB>,
            &'v KB: Into<K1>
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr = db.get(txn, db_key)?.map(Cow::Borrowed);
            cache.insert(<K1 as From<&'v KB>>::from(cache_key), bitmap_ptr);
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

    fn get_proximity_value<'v, KC, DC>(
        txn: &'ctx RoTxn,
        cache_key: &'v PairProximity<'v>,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<PairProximity<'static>, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
    ) -> Result<Option<DC::DItem>>
        where
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr = db.get(txn, db_key)?.map(Cow::Borrowed);
            cache.insert(<&PairProximity<'v> as Into<PairProximity<'static>>>::into(cache_key), bitmap_ptr);
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

    fn get_fid_value<'v, KC, DC>(
        txn: &'ctx RoTxn,
        cache_key: &'v WordFid<'v>,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<WordFid<'static>, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
    ) -> Result<Option<DC::DItem>>
        where
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr = db.get(txn, db_key)?.map(Cow::Borrowed);
            cache.insert(<&WordFid<'v> as Into<WordFid<'static>>>::into(cache_key), bitmap_ptr);
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

    fn get_position_value<'v, KC, DC>(
        txn: &'ctx RoTxn,
        cache_key: &'v WordPosition<'v>,
        db_key: &'v KC::EItem,
        cache: &mut FxHashMap<WordPosition<'static>, Option<Cow<'ctx, [u8]>>>,
        db: Database<KC, Bytes>,
    ) -> Result<Option<DC::DItem>>
        where
            KC: BytesEncode<'v>,
            DC: BytesDecodeOwned,
    {
        if !cache.contains_key(cache_key) {
            let bitmap_ptr = db.get(txn, db_key)?.map(Cow::Borrowed);
            cache.insert(<&WordPosition<'v> as Into<WordPosition<'static>>>::into(cache_key), bitmap_ptr);
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

    pub fn get_db_word_pair_proximity_docids<'a>(
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
                    self.db_cache.word_pair_proximity_docids.get(&PairProximity(proximity, word1.into(), word2.into()))
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
                        .insert(PairProximity(proximity, word1.to_string().into(), word2.to_string().into()).into(), encoded);
                    Some(docids)
                };

                Ok(docids)
            }
            ProximityPrecision::ByWord => DatabaseCache::get_proximity_value::<_, CboRoaringBitmapCodec>(
                self.txn,
                &PairProximity(proximity, Cow::Borrowed(word1), Cow::Borrowed(word2)),
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
                DatabaseCache::get_proximity_value::<_, CboRoaringBitmapLenCodec>(
                    self.txn,
                    &PairProximity(proximity, word1.into(), word2.into()),
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
            self.db_cache.word_prefix_pair_proximity_docids.get(&PairProximity(proximity, word1.into(), prefix2.into()))
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
                .insert((&PairProximity(proximity, word1.into(), prefix2.into())).into(), Some(prefix_docids.clone()));
            Some(prefix_docids)
        };
        Ok(docids)
    }

    pub fn get_db_prefix_prefix_pair_proximity_docids(
        &mut self,
        prefix1: &str,
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
            self.db_cache.prefix_prefix_pair_proximity_docids.get(&PairProximity(proximity, prefix1.into(), prefix2.into()))
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
                        let prefix1_docids = self.get_db_word_prefix_fid_docids(prefix1, fid)?;
                        let prefix2_docids = self.get_db_word_prefix_fid_docids(prefix2, fid)?;
                        if let (Some(prefix1_docids), Some(prefix2_docids)) =
                            (prefix1_docids, prefix2_docids)
                        {
                            prefix_docids |= prefix1_docids & prefix2_docids;
                        }
                    }
                    prefix_docids
                }
                ProximityPrecision::ByWord => {
                    let mut key = Vec::with_capacity(prefix1.len() + 1);
                    key.push(proximity);
                    key.extend_from_slice(prefix1.as_bytes());
                    let mut prefix_docids = RoaringBitmap::new();
                    let remap_key_type = self
                        .index
                        .word_pair_proximity_docids
                        .remap_key_type::<Bytes>()
                        .prefix_iter(self.txn, &key)?
                        .remap_key_type::<U8StrStrCodec>()
                        .lazily_decode_data();
                    for result in remap_key_type {
                        let ((_,_,word2), docids) = result?;
                        if word2.starts_with(prefix2){
                            prefix_docids |= docids.decode().map_err(|err| heed::Error::Decoding(err))?;
                        }
                    }
                    prefix_docids
                }
            };
            self.db_cache
                .prefix_prefix_pair_proximity_docids
                .insert((&PairProximity(proximity, prefix1.into(), prefix2.into())).into(), Some(prefix_docids.clone()));
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

        DatabaseCache::get_fid_value::<_, CboRoaringBitmapCodec>(
            self.txn,
            &WordFid(word.into(), fid),
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

        DatabaseCache::get_fid_value::<_, CboRoaringBitmapCodec>(
            self.txn,
            &WordFid(word_prefix.into(), fid),
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
                    self.db_cache.word_fid_docids.insert(WordFid(Cow::Owned(word.to_string()), fid).into(), Some(Cow::Borrowed(value)));
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
                    self.db_cache.word_prefix_fid_docids.insert(WordFid(Cow::Owned(word_prefix.to_string()), fid).into(), Some(Cow::Borrowed(value)));
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
        DatabaseCache::get_position_value::<_, CboRoaringBitmapCodec>(
            self.txn,
            &WordPosition(word.into(), position),
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
        DatabaseCache::get_position_value::<_, CboRoaringBitmapCodec>(
            self.txn,
            &WordPosition(word_prefix.into(), position),
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
                        .insert(WordPosition(Cow::Owned(word.to_string()), position).into(), Some(Cow::Borrowed(value)));
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
                        .insert(WordPosition(Cow::Owned(word_prefix.to_string()), position).into(), Some(Cow::Borrowed(value)));
                    positions.push(position);
                }
                self.db_cache.word_prefix_positions.insert(word_prefix.to_string(), positions.clone());
                positions
            }
        };
        Ok(positions)
    }
}
#[derive(Eq, PartialEq, Hash)]
struct PairProximity<'str>(u8, Cow<'str, str>, Cow<'str, str>);
impl<'str> From<&PairProximity<'str>> for PairProximity<'static> {
    fn from(value: &PairProximity<'str>) -> Self {
        PairProximity(value.0,Cow::Owned(value.1.to_string()), Cow::Owned(value.2.to_string()))
    }
}

#[derive(Eq, PartialEq, Hash)]
struct WordPosition<'str>(Cow<'str, str>, Position);
impl<'str> From<&WordPosition<'str>> for WordPosition<'static> {
    fn from(value: &WordPosition<'str>) -> Self {
        WordPosition(Cow::Owned(value.0.to_string()), value.1)
    }
}
#[derive(Eq, PartialEq, Hash)]
struct WordFid<'str>(Cow<'str, str>, Fid);
impl<'str> From<&WordFid<'str>> for WordFid<'static> {
    fn from(value: &WordFid<'str>) -> Self {
        WordFid(Cow::Owned(value.0.to_string()), value.1)
    }
}