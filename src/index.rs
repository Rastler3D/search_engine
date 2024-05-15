use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::path::Path;

use charabia::{Language, Script};
use heed::types::*;
use heed::{CompactionOption, Database, RoTxn, RwTxn, Unspecified};
use roaring::RoaringBitmap;
use rstar::RTree;
use time::OffsetDateTime;

use crate::documents::PrimaryKey;
use crate::error::{InternalError, UserError};
use crate::fields_ids_map::FieldsIdsMap;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    FieldIdCodec, OrderedF64Codec,
};
use crate::heed_codec::{
    BEU16StrCodec, FstSetCodec, ScriptLanguageCodec, StrBEU16Codec, StrRefCodec,
};
use crate::order_by_map::OrderByMap;
use crate::proximity::ProximityPrecision;
use crate::vector::EmbeddingConfig;
use crate::{
    default_criteria, CboRoaringBitmapCodec, Criterion, DocumentId, ExternalDocumentsIds,
     FieldDistribution, FieldId, FieldIdWordCountCodec, ObkvCodec,
    Result, RoaringBitmapCodec, RoaringBitmapLenCodec, Search, U8StrStrCodec, BEU16, BEU32, BEU64,
};
use crate::search::facet::FacetDistribution;
use crate::update::analyzer_settings::{AnalyzerConfig, AnalyzerConfigs};
use crate::update::split_config::SplitJoinConfig;
use crate::update::typo_config::TypoConfig;

pub const DEFAULT_MIN_WORD_LEN_ONE_TYPO: u8 = 5;
pub const DEFAULT_MIN_WORD_LEN_TWO_TYPOS: u8 = 9;

pub mod main_key {
    pub const CRITERIA_KEY: &str = "criteria";
    pub const DISTINCT_FIELD_KEY: &str = "distinct-field-key";
    pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
    pub const HIDDEN_FACETED_FIELDS_KEY: &str = "hidden-faceted-fields";
    pub const FILTERABLE_FIELDS_KEY: &str = "filterable-fields";
    pub const SORTABLE_FIELDS_KEY: &str = "sortable-fields";
    pub const FIELD_DISTRIBUTION_KEY: &str = "fields-distribution";
    pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
    pub const GEO_FACETED_DOCUMENTS_IDS_KEY: &str = "geo-faceted-documents-ids";
    pub const GEO_RTREE_KEY: &str = "geo-rtree";
    pub const PRIMARY_KEY_KEY: &str = "primary-key";
    pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
    pub const USER_DEFINED_SEARCHABLE_FIELDS_KEY: &str = "user-defined-searchable-fields";
    pub const SYNONYMS_KEY: &str = "synonyms";
    pub const USER_DEFINED_SYNONYMS_KEY: &str = "user-defined-synonyms";
    pub const WORDS_FST_KEY: &str = "words-fst";
    pub const WORDS_PREFIXES_FST_KEY: &str = "words-prefixes-fst";
    pub const CREATED_AT_KEY: &str = "created-at";
    pub const UPDATED_AT_KEY: &str = "updated-at";
    pub const SPLIT_JOIN_CONFIG: &str = "split-join-config";
    pub const TYPO_CONFIG: &str = "typo-config";
    pub const MAX_VALUES_PER_FACET: &str = "max-values-per-facet";
    pub const SORT_FACET_VALUES_BY: &str = "sort-facet-values-by";
    pub const PAGINATION_MAX_TOTAL_HITS: &str = "pagination-max-total-hits";
    pub const PROXIMITY_PRECISION: &str = "proximity-precision";
    pub const EMBEDDING_CONFIGS: &str = "embedding_configs";
    pub const ANALYZER_CONFIGS: &str = "analyzer_configs";
    pub const SEARCH_CUTOFF: &str = "search_cutoff";
}

pub mod db_name {
    pub const MAIN: &str = "main";
    pub const WORD_DOCIDS: &str = "word-docids";
    pub const EXACT_WORD_DOCIDS: &str = "exact-word-docids";
    pub const WORD_PREFIX_DOCIDS: &str = "word-prefix-docids";
    pub const EXACT_WORD_PREFIX_DOCIDS: &str = "exact-word-prefix-docids";
    pub const EXTERNAL_DOCUMENTS_IDS: &str = "external-documents-ids";
    pub const DOCID_WORD_POSITIONS: &str = "docid-word-positions";
    pub const WORD_PAIR_PROXIMITY_DOCIDS: &str = "word-pair-proximity-docids";
    pub const WORD_POSITION_DOCIDS: &str = "word-position-docids";
    pub const WORD_FIELD_ID_DOCIDS: &str = "word-field-id-docids";
    pub const WORD_PREFIX_POSITION_DOCIDS: &str = "word-prefix-position-docids";
    pub const WORD_PREFIX_FIELD_ID_DOCIDS: &str = "word-prefix-field-id-docids";
    pub const FIELD_ID_WORD_COUNT_DOCIDS: &str = "field-id-word-count-docids";
    pub const FACET_ID_F64_DOCIDS: &str = "facet-id-f64-docids";
    pub const FACET_ID_EXISTS_DOCIDS: &str = "facet-id-exists-docids";
    pub const FACET_ID_IS_NULL_DOCIDS: &str = "facet-id-is-null-docids";
    pub const FACET_ID_IS_EMPTY_DOCIDS: &str = "facet-id-is-empty-docids";
    pub const FACET_ID_STRING_DOCIDS: &str = "facet-id-string-docids";
    pub const FACET_ID_NORMALIZED_STRING_STRINGS: &str = "facet-id-normalized-string-strings";
    pub const FACET_ID_STRING_FST: &str = "facet-id-string-fst";
    pub const FIELD_ID_DOCID_FACET_F64S: &str = "field-id-docid-facet-f64s";
    pub const FIELD_ID_DOCID_FACET_STRINGS: &str = "field-id-docid-facet-strings";
    pub const VECTOR_EMBEDDER_CATEGORY_ID: &str = "vector-embedder-category-id";
    pub const VECTOR_ARROY: &str = "vector-arroy";
    pub const DOCUMENTS: &str = "documents";
    pub const SCRIPT_LANGUAGE_DOCIDS: &str = "script_language_docids";
}

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub(crate) env: heed::Env,

    /// Contains many different types (e.g. the fields ids map).
    pub(crate) main: Database<Unspecified, Unspecified>,

    /// Maps the external documents ids with the internal document id.
    pub external_documents_ids: Database<Str, BEU32>,

    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix.
    pub word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,

    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,

    /// Maps the word and the position with the docids that corresponds to it.
    pub word_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word and the field id with the docids that corresponds to it.
    pub word_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the field id and the word count with the docids that corresponds to it.
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a position with all the docids where the prefix appears at the position.
    pub word_prefix_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a field id with all the docids where the prefix appears inside the field
    pub word_prefix_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and the docids for which this field exists
    pub facet_id_exists_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is set as null
    pub facet_id_is_null_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is considered empty
    pub facet_id_is_empty_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and ranges of numbers with the docids that corresponds to them.
    pub facet_id_f64_docids: Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
    /// Maps the facet field id and ranges of strings with the docids that corresponds to them.
    pub facet_id_string_docids: Database<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>,
    /// Maps the facet field id of the normalized-for-search string facets with their original versions.
    pub facet_id_normalized_string_strings: Database<BEU16StrCodec, SerdeJson<BTreeSet<String>>>,
    /// Maps the facet field id of the string facets with an FST containing all the facets values.
    pub facet_id_string_fst: Database<BEU16, FstSetCodec>,

    /// Maps the document id, the facet field id and the numbers.
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    /// Maps the document id, the facet field id and the strings.
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Str>,

    /// Maps an embedder name to its id in the arroy store.
    pub embedder_category_id: Database<Str, U8>,
    /// Vector store based on arroy™.
    pub vector_arroy: arroy::Database<arroy::distances::Angular>,

    /// Maps the document id to the document as an obkv store.
    pub(crate) documents: Database<BEU32, ObkvCodec>,
}

impl Index {
    pub fn new_with_creation_dates<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions,
        path: P,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> Result<Index> {
        use db_name::*;

        options.max_dbs(25);

        let env = unsafe{ options.open(path)? };
        let mut wtxn = env.write_txn()?;
        let main = env.database_options().name(MAIN).create(&mut wtxn)?;
        let word_docids = env.create_database(&mut wtxn, Some(WORD_DOCIDS))?;
        let external_documents_ids =
            env.create_database(&mut wtxn, Some(EXTERNAL_DOCUMENTS_IDS))?;
        let word_prefix_docids = env.create_database(&mut wtxn, Some(WORD_PREFIX_DOCIDS))?;
        let word_pair_proximity_docids =
            env.create_database(&mut wtxn, Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_position_docids = env.create_database(&mut wtxn, Some(WORD_POSITION_DOCIDS))?;
        let word_fid_docids = env.create_database(&mut wtxn, Some(WORD_FIELD_ID_DOCIDS))?;
        let field_id_word_count_docids =
            env.create_database(&mut wtxn, Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
        let word_prefix_position_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_POSITION_DOCIDS))?;
        let word_prefix_fid_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_FIELD_ID_DOCIDS))?;
        let facet_id_f64_docids = env.create_database(&mut wtxn, Some(FACET_ID_F64_DOCIDS))?;
        let facet_id_string_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_STRING_DOCIDS))?;
        let facet_id_normalized_string_strings =
            env.create_database(&mut wtxn, Some(FACET_ID_NORMALIZED_STRING_STRINGS))?;
        let facet_id_string_fst = env.create_database(&mut wtxn, Some(FACET_ID_STRING_FST))?;
        let facet_id_exists_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_EXISTS_DOCIDS))?;
        let facet_id_is_null_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_NULL_DOCIDS))?;
        let facet_id_is_empty_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_EMPTY_DOCIDS))?;
        let field_id_docid_facet_f64s =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_F64S))?;
        let field_id_docid_facet_strings =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_STRINGS))?;
        // vector stuff
        let embedder_category_id =
            env.create_database(&mut wtxn, Some(VECTOR_EMBEDDER_CATEGORY_ID))?;
        let vector_arroy = env.create_database(&mut wtxn, Some(VECTOR_ARROY))?;

        let documents = env.create_database(&mut wtxn, Some(DOCUMENTS))?;
        wtxn.commit()?;

        Index::set_creation_dates(&env, main, created_at, updated_at)?;

        Ok(Index {
            env,
            main,
            external_documents_ids,
            word_docids,
            word_prefix_docids,
            word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_arroy,
            embedder_category_id,
            documents,
        })
    }

    pub fn new<P: AsRef<Path>>(options: heed::EnvOpenOptions, path: P) -> Result<Index> {
        let now = OffsetDateTime::now_utc();
        Self::new_with_creation_dates(options, path, now, now)
    }

    fn set_creation_dates(
        env: &heed::Env,
        main: Database<Unspecified, Unspecified>,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> heed::Result<()> {
        let mut txn = env.write_txn()?;
        // The db was just created, we update its metadata with the relevant information.
        let main = main.remap_types::<Str, SerdeJson<OffsetDateTime>>();
        if main.get(&txn, main_key::CREATED_AT_KEY)?.is_none() {
            main.put(&mut txn, main_key::UPDATED_AT_KEY, &updated_at)?;
            main.put(&mut txn, main_key::CREATED_AT_KEY, &created_at)?;
            txn.commit()?;
        }
        Ok(())
    }

    /// Create a write transaction to be able to write into the index.
    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    /// Create a read transaction to be able to read the index.
    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    /// Returns the canonicalized path where the heed `Env` of this `Index` lives.
    pub fn path(&self) -> &Path {
        self.env.path()
    }

    /// Returns the size used by the index without the cached pages.
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    /// Returns the real size used by the index.
    pub fn on_disk_size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Returns the map size the underlying environment was opened with, in bytes.
    ///
    /// This value does not represent the current on-disk size of the index.
    ///
    /// This value is the maximum between the map size passed during the opening of the index
    /// and the on-disk size of the index at the time of opening.
    pub fn map_size(&self) -> usize {
        self.env.info().map_size
    }

    pub fn copy_to_file<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        self.env.copy_to_file(path, option).map_err(Into::into)
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event,
    /// multiple threads can wait on this event.
    ///
    /// Make sure that you drop all the copies of `Index`es you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(self) -> heed::EnvClosingEvent {
        self.env.prepare_for_closing()
    }

    /* documents ids */

    /// Writes the documents ids that corresponds to the user-ids-documents-ids FST.
    pub(crate) fn put_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringBitmapCodec>().put(
            wtxn,
            main_key::DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?
            .unwrap_or_default())
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> Result<u64> {
        let count = self
            .main
            .remap_types::<Str, RoaringBitmapLenCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?;
        Ok(count.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub(crate) fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: &str) -> heed::Result<()> {
        self.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::PRIMARY_KEY_KEY, primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub(crate) fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids(&self) -> ExternalDocumentsIds {
        ExternalDocumentsIds::new(self.external_documents_ids)
    }

    /* fields ids map */

    /// Writes the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub(crate) fn put_fields_ids_map(
        &self,
        wtxn: &mut RwTxn,
        map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldsIdsMap>>().put(
            wtxn,
            main_key::FIELDS_IDS_MAP_KEY,
            map,
        )
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn) -> heed::Result<FieldsIdsMap> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldsIdsMap>>()
            .get(rtxn, main_key::FIELDS_IDS_MAP_KEY)?
            .unwrap_or_default())
    }

    /* field distribution */

    /// Writes the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub(crate) fn put_field_distribution(
        &self,
        wtxn: &mut RwTxn,
        distribution: &FieldDistribution,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldDistribution>>().put(
            wtxn,
            main_key::FIELD_DISTRIBUTION_KEY,
            distribution,
        )
    }

    /// Returns the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn field_distribution(&self, rtxn: &RoTxn) -> heed::Result<FieldDistribution> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldDistribution>>()
            .get(rtxn, main_key::FIELD_DISTRIBUTION_KEY)?
            .unwrap_or_default())
    }


    /* searchable fields */

    /// Write the user defined searchable fields and generate the real searchable fields from the specified fields ids map.
    pub(crate) fn put_all_searchable_fields_from_fields_ids_map(
        &self,
        wtxn: &mut RwTxn,
        user_fields: &[&str],
        fields_ids_map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        // We can write the user defined searchable fields as-is.
        self.put_user_defined_searchable_fields(wtxn, user_fields)?;

        // Now we generate the real searchable fields:
        // 1. Take the user defined searchable fields as-is to keep the priority defined by the attributes criterion.
        // 2. Iterate over the user defined searchable fields.
        // 3. If a user defined field is a subset of a field defined in the fields_ids_map
        // (ie doggo.name is a subset of doggo) then we push it at the end of the fields.
        let mut real_fields = user_fields.to_vec();

        for field_from_map in fields_ids_map.names() {
            for user_field in user_fields {
                if crate::is_faceted_by(field_from_map, user_field)
                    && !user_fields.contains(&field_from_map)
                {
                    real_fields.push(field_from_map);
                }
            }
        }

        self.put_searchable_fields(wtxn, &real_fields)
    }

    pub(crate) fn delete_all_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        let did_delete_searchable = self.delete_searchable_fields(wtxn)?;
        let did_delete_user_defined = self.delete_user_defined_searchable_fields(wtxn)?;
        Ok(did_delete_searchable || did_delete_user_defined)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    fn put_searchable_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            wtxn,
            main_key::SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    /// if the searchable fields aren't there it means that **all** the fields are indexed.
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<&'t str>>>()
            .get(rtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids(&self, rtxn: &RoTxn) -> Result<Option<Vec<FieldId>>> {
        match self.searchable_fields(rtxn)? {
            Some(fields) => {
                let fields_ids_map = self.fields_ids_map(rtxn)?;
                let mut fields_ids = Vec::new();
                for name in fields {
                    if let Some(field_id) = fields_ids_map.id(name) {
                        fields_ids.push(field_id);
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub(crate) fn put_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub(crate) fn delete_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the user defined searchable fields.
    pub fn user_defined_searchable_fields<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<_>>>()
            .get(rtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /* filterable fields */

    /// Writes the filterable fields names in the database.
    pub(crate) fn put_filterable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::FILTERABLE_FIELDS_KEY,
            fields,
        )
    }

    /// Deletes the filterable fields ids in the database.
    pub(crate) fn delete_filterable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FILTERABLE_FIELDS_KEY)
    }

    /// Returns the filterable fields names.
    pub fn filterable_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FILTERABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `filterable_fields`, but returns ids instead.
    pub fn filterable_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.filterable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* sortable fields */

    /// Writes the sortable fields names in the database.
    pub(crate) fn put_sortable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::SORTABLE_FIELDS_KEY,
            fields,
        )
    }

    /// Deletes the sortable fields ids in the database.
    pub(crate) fn delete_sortable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SORTABLE_FIELDS_KEY)
    }

    /// Returns the sortable fields names.
    pub fn sortable_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::SORTABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `sortable_fields`, but returns ids instead.
    pub fn sortable_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.sortable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        Ok(fields.into_iter().filter_map(|name| fields_ids_map.id(&name)).collect())
    }

    /* faceted fields */

    /// Writes the faceted fields in the database.
    pub(crate) fn put_faceted_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::HIDDEN_FACETED_FIELDS_KEY,
            fields,
        )
    }

    /// Returns the faceted fields names.
    pub fn faceted_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::HIDDEN_FACETED_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Returns the user defined faceted fields names.
    ///
    /// The user faceted fields are the union of all the filterable, sortable, distinct, and Asc/Desc fields.
    pub fn user_defined_faceted_fields(&self, rtxn: &RoTxn) -> Result<HashSet<String>> {
        let filterable_fields = self.filterable_fields(rtxn)?;
        let sortable_fields = self.sortable_fields(rtxn)?;
        let distinct_field = self.distinct_field(rtxn)?;
        let asc_desc_fields =
            self.criteria(rtxn)?.into_iter().filter_map(|criterion| match criterion {
                Criterion::Asc(field) | Criterion::Desc(field) => Some(field),
                _otherwise => None,
            });

        let mut faceted_fields = filterable_fields;
        faceted_fields.extend(sortable_fields);
        faceted_fields.extend(asc_desc_fields);
        if let Some(field) = distinct_field {
            faceted_fields.insert(field.to_owned());
        }

        Ok(faceted_fields)
    }

    /// Identical to `user_defined_faceted_fields`, but returns ids instead.
    pub fn user_defined_faceted_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields.into_iter() {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Retrieve all the documents which contain this field id set as null
    pub fn null_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_null_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id and that is considered empty
    pub fn empty_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_empty_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id
    pub fn exists_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_exists_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* distinct field */

    pub(crate) fn put_distinct_field(
        &self,
        wtxn: &mut RwTxn,
        distinct_field: &str,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::DISTINCT_FIELD_KEY, distinct_field)
    }

    pub fn distinct_field<'a>(&self, rtxn: &'a RoTxn) -> heed::Result<Option<&'a str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::DISTINCT_FIELD_KEY)
    }

    pub(crate) fn delete_distinct_field(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DISTINCT_FIELD_KEY)
    }

    /* criteria */

    pub(crate) fn put_criteria(
        &self,
        wtxn: &mut RwTxn,
        criteria: &[Criterion],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<&[Criterion]>>().put(
            wtxn,
            main_key::CRITERIA_KEY,
            &criteria,
        )
    }

    pub(crate) fn delete_criteria(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn) -> heed::Result<Vec<Criterion>> {
        match self
            .main
            .remap_types::<Str, SerdeJson<Vec<Criterion>>>()
            .get(rtxn, main_key::CRITERIA_KEY)?
        {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
    }

    /* words fst */

    /// Writes the FST which is the words dictionary of the engine.
    pub(crate) fn put_words_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    /// Returns the FST which is the words dictionary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* synonyms */

    pub(crate) fn put_synonyms(
        &self,
        wtxn: &mut RwTxn,
        synonyms: &HashMap<Vec<String>, Vec<Vec<String>>>,
        user_defined_synonyms: &BTreeMap<String, Vec<String>>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::SYNONYMS_KEY,
            synonyms,
        )?;
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SYNONYMS_KEY,
            user_defined_synonyms,
        )
    }

    pub(crate) fn delete_synonyms(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SYNONYMS_KEY)?;
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SYNONYMS_KEY)
    }

    pub fn user_defined_synonyms(
        &self,
        rtxn: &RoTxn,
    ) -> heed::Result<BTreeMap<String, Vec<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::USER_DEFINED_SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn synonyms(&self, rtxn: &RoTxn) -> heed::Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn words_synonyms<S: AsRef<str>>(
        &self,
        rtxn: &RoTxn,
        words: &[S],
    ) -> heed::Result<Option<Vec<Vec<String>>>> {
        let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
        Ok(self.synonyms(rtxn)?.remove(&words))
    }

    /* words prefixes fst */

    /// Writes the FST which is the words prefixes dictionary of the engine.
    pub(crate) fn put_words_prefixes_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_PREFIXES_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    /// Returns the FST which is the words prefixes dictionary of the engine.
    pub fn words_prefixes_fst<'t>(&self, rtxn: &'t RoTxn) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_PREFIXES_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* word documents count */

    /// Returns the number of documents ids associated with the given word,
    /// it is much faster than deserializing the bitmap and getting the length of it.
    pub fn word_documents_count(&self, rtxn: &RoTxn, word: &str) -> heed::Result<Option<u64>> {
        self.word_docids.remap_data_type::<RoaringBitmapLenCodec>().get(rtxn, word)
    }

    /* documents */

    /// Returns an iterator over the requested documents. The next item will be an error if a document is missing.
    pub fn iter_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, obkv::KvReaderU16<'t>)>> + 'a> {
        Ok(ids.into_iter().map(move |id| {
            let kv = self
                .documents
                .get(rtxn, &id)?
                .ok_or(UserError::UnknownInternalDocumentId { document_id: id })?;
            Ok((id, kv))
        }))
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<'t>(
        &self,
        rtxn: &'t RoTxn,
        ids: impl IntoIterator<Item = DocumentId>,
    ) -> Result<Vec<(DocumentId, obkv::KvReaderU16<'t>)>> {
        self.iter_documents(rtxn, ids)?.collect()
    }

    /// Returns an iterator over all the documents in the index.
    pub fn all_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, obkv::KvReaderU16<'t>)>> + 'a> {
        self.iter_documents(rtxn, self.documents_ids(rtxn)?)
    }

    pub fn external_id_of<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl IntoIterator<Item = Result<String>> + 'a> {
        let fields = self.fields_ids_map(rtxn)?;

        // uses precondition "never called on an empty index"
        let primary_key = self.primary_key(rtxn)?.ok_or(InternalError::DatabaseMissingEntry {
            db_name: db_name::MAIN,
            key: Some(main_key::PRIMARY_KEY_KEY),
        })?;
        let primary_key = PrimaryKey::new(primary_key, &fields).ok_or_else(|| {
            InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldName {
                field_name: primary_key.to_owned(),
                process: "external_id_of",
            })
        })?;
        Ok(self.iter_documents(rtxn, ids)?.map(move |entry| -> Result<_> {
            let (_docid, obkv) = entry?;
            match primary_key.document_id(&obkv, &fields)? {
                Ok(document_id) => Ok(document_id),
                Err(_) => Err(InternalError::DocumentsError(
                    crate::documents::Error::InvalidDocumentFormat,
                )
                .into()),
            }
        }))
    }

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn) -> Result<OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::CREATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::CREATED_AT_KEY),
            })?)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn) -> Result<OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::UPDATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::UPDATED_AT_KEY),
            })?)
    }

    pub(crate) fn set_updated_at(
        &self,
        wtxn: &mut RwTxn,
        time: &OffsetDateTime,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<OffsetDateTime>>().put(
            wtxn,
            main_key::UPDATED_AT_KEY,
            time,
        )
    }

    pub fn typo_config(&self, txn: &RoTxn) -> heed::Result<TypoConfig> {

        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(txn, main_key::TYPO_CONFIG)?
            .unwrap_or(TypoConfig::default()))
    }

    pub(crate) fn put_typo_config(&self, txn: &mut RwTxn, val: TypoConfig) -> heed::Result<()> {

        self.main.remap_types::<Str, SerdeBincode<_>>().put(txn, main_key::TYPO_CONFIG, &val)?;
        Ok(())
    }

    pub fn split_join_config(&self, txn: &RoTxn) -> heed::Result<SplitJoinConfig> {

        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(txn, main_key::SPLIT_JOIN_CONFIG)?
            .unwrap_or(SplitJoinConfig::default()))
    }

    pub(crate) fn put_split_join_config(&self, txn: &mut RwTxn, val: SplitJoinConfig) -> heed::Result<()> {

        self.main.remap_types::<Str, SerdeBincode<_>>().put(txn, main_key::SPLIT_JOIN_CONFIG, &val)?;
        Ok(())
    }

    pub fn max_values_per_facet(&self, txn: &RoTxn) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub(crate) fn put_max_values_per_facet(&self, txn: &mut RwTxn, val: u64) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::MAX_VALUES_PER_FACET, &val)
    }

    pub(crate) fn delete_max_values_per_facet(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub fn sort_facet_values_by(&self, txn: &RoTxn) -> heed::Result<OrderByMap> {
        let orders = self
            .main
            .remap_types::<Str, SerdeJson<OrderByMap>>()
            .get(txn, main_key::SORT_FACET_VALUES_BY)?
            .unwrap_or_default();
        Ok(orders)
    }

    pub(crate) fn put_sort_facet_values_by(
        &self,
        txn: &mut RwTxn,
        val: &OrderByMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(txn, main_key::SORT_FACET_VALUES_BY, &val)
    }

    pub(crate) fn delete_sort_facet_values_by(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::SORT_FACET_VALUES_BY)
    }

    pub fn pagination_max_total_hits(&self, txn: &RoTxn) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub(crate) fn put_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn,
        val: u64,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::PAGINATION_MAX_TOTAL_HITS, &val)
    }

    pub(crate) fn delete_pagination_max_total_hits(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub fn proximity_precision(&self, txn: &RoTxn) -> heed::Result<Option<ProximityPrecision>> {
        self.main
            .remap_types::<Str, SerdeBincode<ProximityPrecision>>()
            .get(txn, main_key::PROXIMITY_PRECISION)
    }

    pub(crate) fn put_proximity_precision(
        &self,
        txn: &mut RwTxn,
        val: ProximityPrecision,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<ProximityPrecision>>().put(
            txn,
            main_key::PROXIMITY_PRECISION,
            &val,
        )
    }

    pub(crate) fn delete_proximity_precision(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PROXIMITY_PRECISION)
    }

    pub fn embedding_configs(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> Result<Vec<(String, EmbeddingConfig)>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<Vec<(String, EmbeddingConfig)>>>()
            .get(rtxn, main_key::EMBEDDING_CONFIGS)?
            .unwrap_or_default())
    }

    pub(crate) fn put_embedding_configs(
        &self,
        wtxn: &mut RwTxn<'_>,
        configs: Vec<(String, EmbeddingConfig)>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<(String, EmbeddingConfig)>>>().put(
            wtxn,
            main_key::EMBEDDING_CONFIGS,
            &configs,
        )
    }

    pub(crate) fn delete_embedding_configs(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::EMBEDDING_CONFIGS)
    }

    pub fn analyzers(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> Result<AnalyzerConfigs> {
        self.analyzer_configs(rtxn).map(AnalyzerConfigs::new)
    }

    pub fn analyzer_configs(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> Result<Vec<(String, AnalyzerConfig)>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<Vec<(String, AnalyzerConfig)>>>()
            .get(rtxn, main_key::ANALYZER_CONFIGS)?
            .unwrap_or_default())
    }

    pub(crate) fn put_analyzer_configs(
        &self,
        wtxn: &mut RwTxn<'_>,
        configs: Vec<(String, AnalyzerConfig)>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<(String, AnalyzerConfig)>>>().put(
            wtxn,
            main_key::ANALYZER_CONFIGS,
            &configs,
        )
    }

    pub(crate) fn delete_analyzer_configs(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::ANALYZER_CONFIGS)
    }

    pub(crate) fn put_search_cutoff(&self, wtxn: &mut RwTxn<'_>, cutoff: u64) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(wtxn, main_key::SEARCH_CUTOFF, &cutoff)
    }

    pub fn search_cutoff(&self, rtxn: &RoTxn<'_>) -> Result<Option<u64>> {
        Ok(self.main.remap_types::<Str, BEU64>().get(rtxn, main_key::SEARCH_CUTOFF)?)
    }

    pub(crate) fn delete_search_cutoff(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCH_CUTOFF)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::ops::Deref;

    use big_s::S;
    use heed::{EnvOpenOptions, RwTxn};
    use maplit::hashset;
    use tempfile::TempDir;

    use crate::documents::DocumentsBatchReader;
    use crate::error::{Error, InternalError};
    use crate::index::{DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS};
    use crate::update::{
        self, IndexDocuments, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Settings,
    };
    use crate::{ obkv_to_json, Index, Search, SearchResult};
    use crate::search::facet::Filter;

    pub(crate) struct TempIndex {
        pub inner: Index,
        pub indexer_config: IndexerConfig,
        pub index_documents_config: IndexDocumentsConfig,
        _tempdir: TempDir,
    }

    impl Deref for TempIndex {
        type Target = Index;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl TempIndex {
        /// Creates a temporary index
        pub fn new_with_map_size(size: usize) -> Self {
            let mut options = EnvOpenOptions::new();
            options.map_size(size);
            let _tempdir = TempDir::new_in(".").unwrap();
            let inner = Index::new(options, _tempdir.path()).unwrap();
            let indexer_config = IndexerConfig::default();
            let index_documents_config = IndexDocumentsConfig::default();
            Self { inner, indexer_config, index_documents_config, _tempdir }
        }
        /// Creates a temporary index, with a default `4096 * 2000` size. This should be enough for
        /// most tests.
        pub fn new() -> Self {
            Self::new_with_map_size(4096 * 2000)
        }
        pub fn add_documents_using_wtxn<'t, R>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            documents: DocumentsBatchReader<R>,
        ) -> Result<(), crate::error::Error>
        where
            R: std::io::Read + std::io::Seek,
        {
            let builder = IndexDocuments::new(
                wtxn,
                self,
                &self.indexer_config,
                self.index_documents_config.clone(),
                |_| (),
                || false,
            )
            .unwrap();
            let (builder, user_error) = builder.add_documents(documents).unwrap();
            user_error?;
            builder.execute()?;
            Ok(())
        }
        pub fn add_documents<R>(
            &self,
            documents: DocumentsBatchReader<R>,
        ) -> Result<(), crate::error::Error>
        where
            R: std::io::Read + std::io::Seek,
        {
            let mut wtxn = self.write_txn().unwrap();
            self.add_documents_using_wtxn(&mut wtxn, documents)?;
            wtxn.commit().unwrap();
            Ok(())
        }

        pub fn update_settings(
            &self,
            update: impl Fn(&mut Settings),
        ) -> Result<(), crate::error::Error> {
            let mut wtxn = self.write_txn().unwrap();
            self.update_settings_using_wtxn(&mut wtxn, update)?;
            wtxn.commit().unwrap();
            Ok(())
        }
        pub fn update_settings_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            update: impl Fn(&mut Settings),
        ) -> Result<(), crate::error::Error> {
            let mut builder = update::Settings::new(wtxn, &self.inner, &self.indexer_config);
            update(&mut builder);
            builder.execute(drop, || false)?;
            Ok(())
        }

        pub fn delete_documents_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            external_document_ids: Vec<String>,
        ) {
            let builder = IndexDocuments::new(
                wtxn,
                self,
                &self.indexer_config,
                self.index_documents_config.clone(),
                |_| (),
                || false,
            )
            .unwrap();
            let (builder, user_error) = builder.remove_documents(external_document_ids).unwrap();
            user_error.unwrap();
            builder.execute().unwrap();
        }

        pub fn delete_documents(&self, external_document_ids: Vec<String>) {
            let mut wtxn = self.write_txn().unwrap();

            self.delete_documents_using_wtxn(&mut wtxn, external_document_ids);

            wtxn.commit().unwrap();
        }

        pub fn delete_document(&self, external_document_id: &str) {
            self.delete_documents(vec![external_document_id.to_string()])
        }
    }

    #[test]
    fn aborting_indexation() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering::Relaxed;

        let index = TempIndex::new();
        let mut wtxn = index.inner.write_txn().unwrap();

        let should_abort = AtomicBool::new(false);
        let builder = IndexDocuments::new(
            &mut wtxn,
            &index.inner,
            &index.indexer_config,
            index.index_documents_config.clone(),
            |_| (),
            || should_abort.load(Relaxed),
        )
        .unwrap();

        let (builder, user_error) = builder
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();
        user_error.unwrap();

        should_abort.store(true, Relaxed);
        let err = builder.execute().unwrap_err();

        assert!(matches!(err, Error::InternalError(InternalError::AbortedIndexation)));
    }

    #[test]
    fn initial_field_distribution() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        // db_snap!(index, field_distribution, 1);
        //
        // db_snap!(index, word_docids,
        //     @r###"
        // 1                [0, ]
        // 2                [1, ]
        // 20               [1, ]
        // bob              [1, ]
        // kevin            [0, ]
        // "###
        // );
        //
        // db_snap!(index, field_distribution);
        //
        // db_snap!(index, field_distribution,
        //     @r###"
        // age              1      |
        // id               2      |
        // name             2      |
        // "###
        // );

        // snapshot_index!(&index, "1", include: "^field_distribution$");

        // we add all the documents a second time. we are supposed to get the same
        // field_distribution in the end
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        // db_snap!(index, field_distribution,
        //     @r###"
        // age              1      |
        // id               2      |
        // name             2      |
        // "###
        // );

        // then we update a document by removing one field and another by adding one field
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin", "has_dog": true },
                { "id": 2, "name": "bob" }
            ]))
            .unwrap();

        // db_snap!(index, field_distribution,
        //     @r###"
        // has_dog          1      |
        // id               2      |
        // name             2      |
        // "###
        // );
    }


    #[test]
    fn add_documents_and_set_searchable_fields() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();
        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }

    #[test]
    fn set_searchable_fields_and_add_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name"]);
        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);

        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }


    #[test]
    fn replace_documents_external_ids_and_soft_deletion_check() {
        use big_s::S;
        use maplit::hashset;

        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("doggo") });
            })
            .unwrap();

        let mut docs = vec![];
        for i in 0..4 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();


        let mut docs = vec![];
        for i in 0..3 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i + 1 }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();



        index
            .add_documents(documents!([{ "id": 3, "doggo": 4 }, { "id": 3, "doggo": 5 },{ "id": 3, "doggo": 4 }]))
            .unwrap();



        index
            .update_settings(|settings| {
                settings.set_distinct_field("id".to_owned());
            })
            .unwrap();


    }


    #[test]
    fn simple_delete() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;
        index
            .add_documents(documents!([
                { "id": 30 },
                { "id": 34 }
            ]))
            .unwrap();


        index.delete_document("34");


    }

}
