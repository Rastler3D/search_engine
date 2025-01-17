use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::io::BufReader;
use std::{io, mem, str};

use obkv::{KvReader, KvWriterU16};
use roaring::RoaringBitmap;
use serde_json::Value;
use analyzer::analyzer::{Analyzer, BoxAnalyzer};
use analyzer::token::{OwnedToken, SeparatorKind, TokenKind};
use analyzer::tokenizer::token_stream::TokenStream;

use super::helpers::{create_sorter, keep_latest_obkv, sorter_into_reader, GrenadParameters};
use crate::error::{InternalError, SerializationError};
use crate::update::del_add::{del_add_from_two_obkvs, DelAdd, KvReaderDelAdd};
use crate::{FieldId, Result, MAX_POSITION_PER_ATTRIBUTE, MAX_WORD_LENGTH};


/// Extracts the word and positions where this word appear and
/// prefixes it by the document id.
///
/// Returns the generated internal documents ids and a grenad reader
/// with the list of extracted words from the given chunk of documents.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_docid_word_positions<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    searchable_fields: &Option<HashSet<FieldId>>,
    analyzer: &BoxAnalyzer,
    max_positions_per_attributes: Option<u32>,
) -> Result<grenad::Reader<BufReader<File>>> {
    puffin::profile_function!();

    let max_positions_per_attributes = max_positions_per_attributes
        .map_or(MAX_POSITION_PER_ATTRIBUTE, |max| max.min(MAX_POSITION_PER_ATTRIBUTE));
    let max_memory = indexer.max_memory_by_thread();

    // initialize destination values.
    let mut documents_ids = RoaringBitmap::new();
    let mut docid_word_positions_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        keep_latest_obkv,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    // initialize buffers.
    let mut del_buffers = Buffers::default();
    let mut add_buffers = Buffers::default();
    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();


    // iterate over documents.
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::new(value);

        // if the searchable fields didn't change, skip the searchable indexing for this document.
        if !searchable_fields_changed(&KvReader::<FieldId>::new(value), searchable_fields) {
            continue;
        }

        documents_ids.push(document_id);

        // Update key buffer prefix.
        key_buffer.clear();
        key_buffer.extend_from_slice(&document_id.to_be_bytes());

        // Tokenize deletions and additions in 2 diffferent threads.
        let (del, add): (Result<_>, Result<_>) = rayon::join(
            || {
                // deletions
                tokens_from_document(
                    &obkv,
                    searchable_fields,
                    analyzer,
                    max_positions_per_attributes,
                    DelAdd::Deletion,
                    &mut del_buffers,
                )
            },
            || {
                // additions
                tokens_from_document(
                    &obkv,
                    searchable_fields,
                    analyzer,
                    max_positions_per_attributes,
                    DelAdd::Addition,
                    &mut add_buffers,
                )
            },
        );

        let del_obkv = del?;
        let add_obkv = add?;

        // merge deletions and additions.
        // transforming two KV<FieldId, KV<u16, String>> into one KV<FieldId, KV<DelAdd, KV<u16, String>>>
        value_buffer.clear();
        del_add_from_two_obkvs(
            KvReader::<FieldId>::new(del_obkv),
            KvReader::<FieldId>::new(add_obkv),
            &mut value_buffer,
        )?;

        // write each KV<DelAdd, KV<u16, String>> into the sorter, field by field.
        let obkv = KvReader::<FieldId>::new(&value_buffer);
        for (field_id, value) in obkv.iter() {
            key_buffer.truncate(mem::size_of::<u32>());
            key_buffer.extend_from_slice(&field_id.to_be_bytes());
            docid_word_positions_sorter.insert(&key_buffer, value)?;
        }

    }

    // the returned sorter is serialized as: key: (DocId, FieldId), value: KV<DelAdd, KV<u16, String>>.
    sorter_into_reader(docid_word_positions_sorter, indexer)
}

/// Check if any searchable fields of a document changed.
fn searchable_fields_changed(
    obkv: &KvReader<FieldId>,
    searchable_fields: &Option<HashSet<FieldId>>,
) -> bool {
    for (field_id, field_bytes) in obkv.iter() {
        if searchable_fields.as_ref().map_or(true, |sf| sf.contains(&field_id)) {
            let del_add = KvReaderDelAdd::new(field_bytes);
            match (del_add.get(DelAdd::Deletion), del_add.get(DelAdd::Addition)) {
                // if both fields are None, check the next field.
                (None, None) => (),
                // if both contains a value and values are the same, check the next field.
                (Some(del), Some(add)) if del == add => (),
                // otherwise the fields are different, return true.
                _otherwise => return true,
            }
        }
    }

    false
}



/// Extract words mapped with their positions of a document.
fn tokens_from_document<'a>(
    obkv: &KvReader<FieldId>,
    searchable_fields: &Option<HashSet<FieldId>>,
    analyzer: &BoxAnalyzer,
    max_positions_per_attributes: u32,
    del_add: DelAdd,
    buffers: &'a mut Buffers,
) -> Result<&'a [u8]> {
    buffers.obkv_buffer.clear();
    let mut document_writer = KvWriterU16::new(&mut buffers.obkv_buffer);
    for (field_id, field_bytes) in obkv.iter() {
        // if field is searchable.
        if searchable_fields.as_ref().map_or(true, |sf| sf.contains(&field_id)) {
            // extract deletion or addition only.
            if let Some(field_bytes) = KvReaderDelAdd::new(field_bytes).get(del_add) {
                // parse json.
                let value =
                    serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;

                // prepare writing destination.
                buffers.obkv_positions_buffer.clear();
                let mut writer = KvWriterU16::new(&mut buffers.obkv_positions_buffer);

                // convert json into a unique string.
                buffers.field_buffer.clear();
                if let Some(field) = json_to_string(&value, &mut buffers.field_buffer) {
                    // create an iterator of token with their positions.
                    let tokens = process_tokens(analyzer.analyze(field).as_iter())
                        .take_while(|(p, _)| (*p as u32) < max_positions_per_attributes);

                    for (index, token) in tokens {
                        // keep a word only if it is not empty and fit in a LMDB key.
                        let token = token.text.trim();
                        if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                            let position: u16 = index
                                .try_into()
                                .map_err(|_| SerializationError::InvalidNumberSerialization)?;
                            writer.insert(position, token.as_bytes())?;
                        }
                    }

                    // write positions into document.
                    let positions = writer.into_inner()?;
                    document_writer.insert(field_id, positions)?;
                }
            }
        }
    }

    // returns a KV<FieldId, KV<u16, String>>
    Ok(document_writer.into_inner().map(|v| v.as_slice())?)
}

/// Transform a JSON value into a string that can be indexed.
fn json_to_string<'a>(value: &'a Value, buffer: &'a mut String) -> Option<&'a str> {
    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null | Value::Object(_) => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
        }
    }

    if let Value::String(string) = value {
        Some(string)
    } else if inner(value, buffer) {
        Some(buffer)
    } else {
        None
    }
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of 8 between words,
/// else we keep the standard proximity of 1 between words.
fn process_tokens<'token>(
    tokens: impl Iterator<Item = OwnedToken<'token>>,
) -> impl Iterator<Item = (usize, OwnedToken<'token>)> {
    tokens
        .scan((0, None), |(offset, prev_kind), mut token| {
            match token.token_kind {
                TokenKind::Word(_) if !token.text.is_empty() => {
                    *offset += match *prev_kind {
                        Some(TokenKind::Separator(SeparatorKind::Hard)) => 8,
                        Some(TokenKind::Word(_)) => 1,
                        _ => 0,
                    };
                    *prev_kind = Some(token.token_kind)
                }
                TokenKind::Separator(SeparatorKind::Hard) => {
                    *prev_kind = Some(token.token_kind);
                }
                _ => ()

            }
            Some((*offset, token))
        })
        .filter(|(_, t)| t.is_word())
}


#[derive(Default)]
struct Buffers {
    // the field buffer for each fields desserialization, and must be cleared between each field.
    field_buffer: String,
    // buffer used to store the value data containing an obkv.
    obkv_buffer: Vec<u8>,
    // buffer used to store the value data containing an obkv of tokens with their positions.
    obkv_positions_buffer: Vec<u8>,
}
