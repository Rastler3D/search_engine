use roaring::RoaringBitmap;
use crate::search::context::{Context, Fid};
use crate::search::query_graph::{GraphNode, NodeData, QueryGraph};
use crate::search::query_parser::{DerivativeTerm, OriginalTerm, Term, TermKind};
use crate::search::utils::bit_set::BitSet;
use crate::Result;

pub fn resolve_path_docids(path: BitSet, graph: &QueryGraph, context: &mut impl Context) -> Result<RoaringBitmap>{
    let node = &graph.nodes[graph.root];
    let mut docids = context.node_docids(graph.root, graph)?.clone();
    let mut successors = node.successors;
    for next_node in successors.intersection(&path).iter(){
        if next_node == graph.end && path.len() > 1{
            continue
        };
        resolve_path_docids_from(path.exclude(graph.root), next_node, graph, context, &mut docids)?
    }
    Ok(docids)
}

fn resolve_path_docids_from(path: BitSet, node_id: usize, graph: &QueryGraph, context: &mut impl Context, docids: &mut RoaringBitmap) -> Result<()>{
    let node = &graph.nodes[node_id];
    *docids &= context.node_docids(node_id, graph)?;

    let mut successors = node.successors;
    for next_node in successors.intersection(&path).iter(){
        if next_node == graph.end && path.len() > 1{
            continue
        };
        resolve_path_docids_from(path.exclude(node_id), next_node, graph, context, docids)?
    }

    Ok(())
}

pub fn resolve_node_docids(node: &GraphNode, context: &mut impl Context) -> Result<RoaringBitmap>{
    match &node.data {
        NodeData::Term(term) => {
            resolve_docids(term, context)
        }
        _ => context.all_docids()
    }
}


pub fn resolve_docids(term: &Term, context: &mut (impl Context + ?Sized)) -> Result<RoaringBitmap>{
    match term {
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Word(word)) |
        TermKind::Exact(OriginalTerm::Word(word)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word, ..), ..), ..
        } => {
            context.word_docids(word)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Prefix(prefix)) |
        TermKind::Exact(OriginalTerm::Prefix(prefix)), ..
        } => {
            context.prefix_docids(prefix)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Phrase(phrase)) |
        TermKind::Exact(OriginalTerm::Phrase(phrase)), ..
        } => {
            context.phrase_docids(phrase).cloned()
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Typo(words, ..) | DerivativeTerm::Synonym(words), ..) , .. } => {
            let mut result = RoaringBitmap::new();
            for word in words{
                result |= context.word_docids(word)?;
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(words, .. ), ..), .. } => {
            let mut result = RoaringBitmap::new();
            for split in words{
                result |= context.split_docids(split)?;
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::SynonymPhrase(words, .. ), ..), .. } => {
            let mut result = RoaringBitmap::new();
            for phrase in words{
                result |= context.phrase_docids(phrase)?;
            }

            Ok(result)
        },
        Term { term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _), .. } => {
            let mut result = RoaringBitmap::new();
            for prefix in prefixes{
                result |= context.prefix_docids(prefix)?;
            }

            Ok(result)
        }
    }
}

pub fn resolve_fid_docids(term: &Term, context: &mut (impl Context + ?Sized), fid: Fid) -> Result<RoaringBitmap>{
    match term {
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Word(word)) |
        TermKind::Exact(OriginalTerm::Word(word)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word, ..), ..), ..
        } => {
            context.word_fid_docids(word, fid)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Prefix(prefix)) |
        TermKind::Exact(OriginalTerm::Prefix(prefix)), ..
        } => {
            context.prefix_fid_docids(prefix, fid)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Phrase(phrase)) |
        TermKind::Exact(OriginalTerm::Phrase(phrase)), ..
        } => {
            let Some(word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            let mut word_fid_docids = context.word_fid_docids(word, fid)?;
            word_fid_docids &= context.phrase_docids(phrase)?;

            Ok(word_fid_docids)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Typo(words, ..) | DerivativeTerm::Synonym(words), ..) , .. } => {
            let mut result = RoaringBitmap::new();
            for word in words{
                result |= context.word_fid_docids(word, fid)?;
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(words, .. ), ..), .. } => {
            let mut result = RoaringBitmap::new();
            for split in words{
                let word_fid_docids = context.word_fid_docids(&split.0, fid)?;
                result |= word_fid_docids & context.split_docids(split)?;
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::SynonymPhrase(words, .. ), ..), .. } => {
            let mut result = RoaringBitmap::new();
            for phrase in words{
                let Some(word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
                let word_fid_docids = context.word_fid_docids(word, fid)?;
                result |= word_fid_docids & context.phrase_docids(phrase)?;
            }

            Ok(result)
        },
        Term { term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _), .. } => {
            let mut result = RoaringBitmap::new();
            for prefix in prefixes{
                result |= context.prefix_fid_docids(prefix, fid)?;
            }

            Ok(result)
        }
    }
}


pub fn phrase_resolve(phrase: &[String], context: &mut (impl Context + ?Sized)) -> Result<RoaringBitmap>{
    if phrase.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    let mut candidates = RoaringBitmap::new();
    for word in phrase.iter() {
        let word_docids = context.word_docids(word)?;
        if !word_docids.is_empty() {
            candidates |= word_docids;
        } else {
            return Ok(RoaringBitmap::new());
        }
    }

    let win_size = phrase.len().min(3);

    for win in phrase.windows(win_size) {
        let mut bitmaps = Vec::with_capacity(win_size.pow(2));
        for (offset, s1) in win
            .iter()
            .enumerate()
            .map(|(index, word)| (index, word))
        {
            for (dist, s2) in win
                .iter()
                .skip(offset + 1)
                .enumerate()
                .map(|(index, word)| (index, word))
            {
                if dist == 0 {
                    let docids = context.word_pair_proximity_docids(s1, s2, 1)?;
                    if docids.is_empty() {
                        return Ok(RoaringBitmap::new());
                    } else {
                        bitmaps.push(docids);
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        let docids = context.word_pair_proximity_docids(s1, s2, dist as u8 + 1)?;
                        if !docids.is_empty() {
                            bitmap |= docids;
                        }
                    }
                    if bitmap.is_empty() {
                        return Ok(bitmap);
                    } else {
                        bitmaps.push(bitmap);
                    }
                }
            }
        }

        bitmaps.sort_unstable_by_key(|a| a.len());

        for bitmap in bitmaps {
            candidates &= bitmap;

            // There will be no match, return early
            if candidates.is_empty() {
                break;
            }
        }
    }
    Ok(candidates)
}

pub fn split_resolve(split: &(String, String), context: &mut (impl Context + ?Sized)) -> Result<RoaringBitmap>{

    let positions = context.word_pair_proximity_docids(&split.0, &split.1, 1)?;

    Ok(positions)
}


pub fn resolve_docids_proximity(
    ctx: &mut (impl Context + ?Sized),
    left_term: &Term,
    right_term: &Term,
    cost: u8,
) -> Result<RoaringBitmap> {
    let forward_proximity = 1 + cost;
    let backward_proximity = cost;

    let docids = match (&left_term.term_kind, &right_term.term_kind) {
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, ..), ..)) =>{
            let mut docids = RoaringBitmap::new();
            compute_word_word_pair_proximity(ctx, word1, word2, cost, &mut docids)?;
            compute_word_word_pair_proximity(ctx, word1, word2, backward_proximity, &mut docids)?;
            docids
        },
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Exact(OriginalTerm::Prefix(prefix2)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix2))) =>{
            let mut docids = RoaringBitmap::new();
            compute_word_prefix_pair_proximity(ctx, word1, prefix2, forward_proximity, &mut docids)?;
            compute_word_prefix_pair_proximity(ctx, word1, prefix2, backward_proximity, &mut docids)?;
            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, ..), ..))
             =>{
            let mut docids = RoaringBitmap::new();
            compute_prefix_word_pair_proximity(ctx, prefix1, word2, forward_proximity, &mut docids)?;
            compute_prefix_word_pair_proximity(ctx, prefix1, word2, backward_proximity, &mut docids)?;
            docids
        },
        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, ..), ..)) => {
            let Some(phrase_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            compute_word_word_pair_proximity(ctx, phrase_last_word, word2, forward_proximity, &mut docids)?;
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Exact(OriginalTerm::Prefix(word2)) |
            TermKind::Normal(OriginalTerm::Prefix(word2)))=> {
            let Some(phrase_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            compute_word_prefix_pair_proximity(ctx, phrase_last_word, word2, forward_proximity, &mut docids)?;
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Exact(OriginalTerm::Phrase(phrase2)) |
            TermKind::Normal(OriginalTerm::Phrase(phrase2)))=> {
            let Some(phrase1_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let Some(phrase2_first_word) = phrase2.first() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            compute_word_word_pair_proximity(ctx, phrase1_last_word, phrase2_first_word, forward_proximity, &mut docids)?;
            docids &= ctx.phrase_docids(phrase1)?;
            docids &= ctx.phrase_docids(phrase2)?;
            docids
        },
        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
            TermKind::Derivative(DerivativeTerm::Synonym(words), ..))=> {
            let Some(phrase_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            for word in words{
                compute_word_word_pair_proximity(ctx, phrase_last_word, word, forward_proximity, &mut docids)?;
            }
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
            TermKind::Derivative(DerivativeTerm::Synonym(words), ..))=> {
            let mut docids = RoaringBitmap::new();
            for word2 in words{
                compute_word_word_pair_proximity(ctx, word1, word2, forward_proximity, &mut docids)?;
                compute_word_word_pair_proximity(ctx, word1, word2, backward_proximity, &mut docids)?;
            }

            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
            TermKind::Derivative(DerivativeTerm::Synonym(words), ..))=> {
            let mut docids = RoaringBitmap::new();
            for word2 in words{
                compute_prefix_word_pair_proximity(ctx, prefix1, word2, forward_proximity, &mut docids)?;
                compute_prefix_word_pair_proximity(ctx, prefix1, word2, backward_proximity, &mut docids)?;
            }
            docids
        },

        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, ..), ..)) => {
            let Some(phrase_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            for prefix in prefixes{
                compute_word_prefix_pair_proximity(ctx, phrase_last_word, prefix, forward_proximity, &mut docids)?;
            }
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for prefix2 in prefixes{
                compute_word_prefix_pair_proximity(ctx, word1, prefix2, forward_proximity, &mut docids)?;
                compute_word_prefix_pair_proximity(ctx, word1, prefix2, backward_proximity, &mut docids)?;
            }

            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for prefix2 in prefixes{
                compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, forward_proximity, &mut docids)?;
                compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, backward_proximity, &mut docids)?;
            }
            docids
        },
        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Derivative(DerivativeTerm::Split(splits, ..), ..)) => {
            let Some(phrase_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, phrase_last_word, &split.0, forward_proximity, &mut split_docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Derivative(DerivativeTerm::Split(splits, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, word1, &split.0, forward_proximity, &mut docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }

            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Derivative(DerivativeTerm::Split(splits, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_prefix_word_pair_proximity(ctx, prefix1, &split.0, forward_proximity, &mut docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }

            docids
        },

        (TermKind::Exact(OriginalTerm::Phrase(phrase1)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase1)),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let Some(phrase1_last_word) = phrase1.last() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase2_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, phrase1_last_word, phrase2_first_word, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }
            docids &= ctx.phrase_docids(phrase1)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase2_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, word1, &phrase2_first_word, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase2_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_prefix_word_pair_proximity(ctx, prefix1, &phrase2_first_word, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },

        (TermKind::Exact(OriginalTerm::Word(word1)) |
        TermKind::Normal(OriginalTerm::Word(word1)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word1, ..), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase2_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, word1, &phrase2_first_word, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms1, ..), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms2, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase1 in synonyms1 {
                let Some(phrase1_last_word) = synonym_phrase1.last() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids1 = RoaringBitmap::new();
                for synonym_phrase2 in synonyms2 {
                    let Some(phrase2_first_word) = synonym_phrase2.first() else { return Ok(RoaringBitmap::new()) };
                    let mut synonym_docids2 = RoaringBitmap::new();
                    compute_word_word_pair_proximity(ctx, phrase1_last_word, phrase2_first_word, forward_proximity, &mut synonym_docids2)?;
                    synonym_docids2 &= ctx.phrase_docids(synonym_phrase2)?;
                    synonym_docids1 |= synonym_docids2;
                }
                synonym_docids1 &= ctx.phrase_docids(synonym_phrase1)?;
                docids |= synonym_docids1;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits, ..), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
                let Some(phrase_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                for split in splits {
                    let mut split_docids = RoaringBitmap::new();
                    compute_word_word_pair_proximity(ctx, &split.1, phrase_first_word, forward_proximity, &mut synonym_docids)?;
                    split_docids &= ctx.split_docids(split)?;
                    synonym_docids |= split_docids;
                }
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, ..), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
                let Some(phrase_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                for prefix in prefixes {
                        compute_prefix_word_pair_proximity(ctx, &prefix, phrase_first_word, forward_proximity, &mut synonym_docids)?;
                    }
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
        TermKind::Derivative(DerivativeTerm::Synonym(words), ..),
            TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
                let Some(phrase_first_word) = synonym_phrase.first() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                for word in words {
                    compute_word_word_pair_proximity(ctx, &word, phrase_first_word, forward_proximity, &mut synonym_docids)?;
                }
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Ngram(word, _), _) |
        TermKind::Normal(OriginalTerm::Word(word)) |
        TermKind::Exact(OriginalTerm::Word(word)),
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase))) => {
            let Some(phrase_last_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            compute_word_word_pair_proximity(ctx, word, phrase_last_word, forward_proximity, &mut docids)?;
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for word1 in words{
                compute_word_word_pair_proximity(ctx, word1, word2, forward_proximity, &mut docids)?;
                compute_word_word_pair_proximity(ctx, word1, word2, backward_proximity, &mut docids)?;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words1), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words1, ..), ..),
            TermKind::Derivative(DerivativeTerm::Synonym(words2), _) |
            TermKind::Derivative(DerivativeTerm::Typo(words2, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for word1 in words1 {
                for word2 in words2 {
                    compute_word_word_pair_proximity(ctx, word1, word2, forward_proximity, &mut docids)?;
                    compute_word_word_pair_proximity(ctx, word1, word2, backward_proximity, &mut docids)?;
                }
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for word1 in words{
                for prefix2 in prefixes{
                    compute_word_prefix_pair_proximity(ctx, word1, prefix2, forward_proximity, &mut docids)?;
                    compute_word_prefix_pair_proximity(ctx, word1, prefix2, backward_proximity, &mut docids)?;
                }
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..),
            TermKind::Derivative(DerivativeTerm::Split(splits), _)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits {
            let mut split_docids = RoaringBitmap::new();
                for word in words {
                    compute_word_word_pair_proximity(ctx, word, &split.0, forward_proximity, &mut docids)?;
                }
            split_docids &= ctx.split_docids(split)?;
            docids |= split_docids;
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..),
            TermKind::Exact(OriginalTerm::Prefix(prefix)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix))) => {
            let mut docids = RoaringBitmap::new();
            for word1 in words{
                compute_word_prefix_pair_proximity(ctx, word1, prefix, forward_proximity, &mut docids)?;
                compute_word_prefix_pair_proximity(ctx, word1, prefix, backward_proximity, &mut docids)?;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
        TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..),
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase))) => {
            let mut docids = RoaringBitmap::new();
            let Some(phrase_first_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            for word in words{
                compute_word_word_pair_proximity(ctx, word, phrase_first_word, forward_proximity, &mut docids)?;
            }
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for prefix in prefixes{
                compute_word_word_pair_proximity(ctx, prefix, word2, forward_proximity, &mut docids)?;
                compute_word_word_pair_proximity(ctx, prefix, word2, backward_proximity, &mut docids)?;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _),
            TermKind::Derivative(DerivativeTerm::Synonym(words), _) |
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..)) => {
            let mut docids = RoaringBitmap::new();
            for prefix1 in prefixes{
                for word2 in words{
                    compute_prefix_word_pair_proximity(ctx, prefix1, word2, forward_proximity, &mut docids)?;
                    compute_prefix_word_pair_proximity(ctx, prefix1, word2, backward_proximity, &mut docids)?;
                }
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes1, _), _),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes2, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for prefix1 in prefixes1{
                for prefix2 in prefixes2{
                    compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, forward_proximity, &mut docids)?;
                    compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, backward_proximity, &mut docids)?;
                }
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _),
            TermKind::Derivative(DerivativeTerm::Split(splits), _)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits {
            let mut split_docids = RoaringBitmap::new();
                for prefix in prefixes {
                    compute_word_word_pair_proximity(ctx, prefix, &split.0, forward_proximity, &mut docids)?;
                }
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _),
            TermKind::Exact(OriginalTerm::Prefix(prefix2)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix2))) => {
            let mut docids = RoaringBitmap::new();
            for prefix1 in prefixes{
                compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, forward_proximity, &mut docids)?;
                compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, backward_proximity, &mut docids)?;

            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _),
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase))) => {
            let mut docids = RoaringBitmap::new();
            let Some(phrase_first_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            for prefix in prefixes{
                compute_prefix_word_pair_proximity(ctx, prefix, phrase_first_word, forward_proximity, &mut docids)?;
            }
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, phrase_last_word, word2, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
            TermKind::Derivative(DerivativeTerm::Synonym(words), ..)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
            let mut synonym_docids = RoaringBitmap::new();
            let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                for word in words {
                    compute_word_word_pair_proximity(ctx, phrase_last_word, word, forward_proximity, &mut synonym_docids)?;
                }
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
                let mut synonym_docids = RoaringBitmap::new();
                let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                for prefix in prefixes {
                    compute_word_prefix_pair_proximity(ctx, phrase_last_word, prefix, forward_proximity, &mut synonym_docids)?;
                }
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Derivative(DerivativeTerm::Split(splits), _)) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms {
                let mut synonym_docids = RoaringBitmap::new();
                let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                for split in splits{
                    let mut split_docids = RoaringBitmap::new();
                    compute_word_word_pair_proximity(ctx, phrase_last_word, &split.0, forward_proximity, &mut docids)?;
                    split_docids &= ctx.split_docids(split)?;
                    docids |= split_docids;
                }

                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Exact(OriginalTerm::Prefix(prefix2)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix2))) => {
            let mut docids = RoaringBitmap::new();
            for synonym_phrase in synonyms{
                let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                let mut synonym_docids = RoaringBitmap::new();
                compute_word_prefix_pair_proximity(ctx, phrase_last_word, prefix2, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonyms), _),
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase))) => {
            let mut docids = RoaringBitmap::new();
            let Some(phrase_first_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            for synonym_phrase in synonyms {
                let mut synonym_docids = RoaringBitmap::new();
                let Some(phrase_last_word) = synonym_phrase.last() else { return Ok(RoaringBitmap::new()) };
                compute_word_word_pair_proximity(ctx, phrase_last_word, phrase_first_word, forward_proximity, &mut synonym_docids)?;
                synonym_docids &= ctx.phrase_docids(synonym_phrase)?;
                docids |= synonym_docids;
            }
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits), _),
            TermKind::Exact(OriginalTerm::Word(word2)) |
            TermKind::Normal(OriginalTerm::Word(word2)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word2, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, &split.1, word2, forward_proximity, &mut docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits), _),
            TermKind::Derivative(DerivativeTerm::Typo(words, ..), ..) |
            TermKind::Derivative(DerivativeTerm::Synonym(words), ..)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits {
                let mut split_docids = RoaringBitmap::new();
                for word in words {
                    compute_word_word_pair_proximity(ctx, &split.1,word, forward_proximity, &mut docids)?;
                }
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits), _), TermKind::Derivative(DerivativeTerm::PrefixTypo(prefixes, _), _)) => {
            let mut docids = RoaringBitmap::new();
            for split in splits {
                let mut split_docids = RoaringBitmap::new();
                for prefix in prefixes {
                    compute_word_prefix_pair_proximity(ctx, &split.1, prefix , forward_proximity, &mut docids)?;
                }
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits1), _), TermKind::Derivative(DerivativeTerm::Split(splits2), _)) => {
            let mut docids = RoaringBitmap::new();
            for split1 in splits1 {
                let mut split_docids1 = RoaringBitmap::new();
                for split2 in splits2 {
                    let mut split_docids2 = RoaringBitmap::new();
                    compute_word_word_pair_proximity(ctx, &split1.1, &split2.0 , forward_proximity, &mut docids)?;
                    split_docids2 &= ctx.split_docids(split2)?;
                    split_docids1 |= split_docids2;
                }
                split_docids1 &= ctx.split_docids(split1)?;
                docids |= split_docids1;
            }
            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits), _),
            TermKind::Exact(OriginalTerm::Prefix(prefix2)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix2))) => {
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_word_prefix_pair_proximity(ctx, &split.1, prefix2, forward_proximity, &mut split_docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }

            docids
        },
        (TermKind::Derivative(DerivativeTerm::Split(splits), _),
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase))) => {
            let Some(phrase_first_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            for split in splits{
                let mut split_docids = RoaringBitmap::new();
                compute_word_word_pair_proximity(ctx, &split.1, phrase_first_word, forward_proximity, &mut split_docids)?;
                split_docids &= ctx.split_docids(split)?;
                docids |= split_docids;
            }
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix1)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix1)),
            TermKind::Exact(OriginalTerm::Prefix(prefix2)) |
            TermKind::Normal(OriginalTerm::Prefix(prefix2))) => {
            let mut docids = RoaringBitmap::new();
            compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, forward_proximity, &mut docids)?;
            compute_prefix_prefix_pair_proximity(ctx, prefix1, prefix2, backward_proximity, &mut docids)?;
            docids
        },
        (TermKind::Exact(OriginalTerm::Prefix(prefix)) |
        TermKind::Normal(OriginalTerm::Prefix(prefix)),
        TermKind::Exact(OriginalTerm::Phrase(phrase)) |
        TermKind::Normal(OriginalTerm::Phrase(phrase)))=> {
            let Some(phrase_first_word) = phrase.first() else { return Ok(RoaringBitmap::new()) };
            let mut docids = RoaringBitmap::new();
            compute_prefix_word_pair_proximity(ctx, prefix, phrase_first_word, forward_proximity, &mut docids)?;
            docids &= ctx.phrase_docids(phrase)?;

            docids
        },
    };

    Ok(docids)
}



fn compute_prefix_word_pair_proximity(
    ctx: &mut (impl Context + ?Sized),
    left_prefix: &str,
    right_word: &str,
    proximity: u8,
    docids: &mut RoaringBitmap,
) -> Result<()> {

    let new_docids = ctx.prefix_word_pair_proximity_docids(left_prefix, right_word, proximity)?;
    if !new_docids.is_empty() {
        *docids |= new_docids;
    }

    Ok(())
}

fn compute_word_prefix_pair_proximity(
    ctx: &mut (impl Context + ?Sized),
    left_word: &str,
    right_prefix: &str,
    proximity: u8,
    docids: &mut RoaringBitmap,
) -> Result<()> {

    let new_docids = ctx.word_prefix_pair_proximity_docids(left_word, right_prefix, proximity)?;
    if !new_docids.is_empty() {
        *docids |= new_docids;
    }

    Ok(())
}

fn compute_word_word_pair_proximity(
    ctx: &mut (impl Context + ?Sized),
    word1: &str,
    word2: &str,
    proximity: u8,
    docids: &mut RoaringBitmap,
) -> Result<()> {

    let new_docids = ctx.word_pair_proximity_docids(word1, word2, proximity)?;
    if !new_docids.is_empty() {
        *docids |= new_docids;
    }

    Ok(())
}

fn compute_prefix_prefix_pair_proximity(
    ctx: &mut (impl Context + ?Sized),
    prefix1: &str,
    prefix2: &str,
    proximity: u8,
    docids: &mut RoaringBitmap,
) -> Result<()> {

    let new_docids = ctx.prefix_prefix_pair_proximity_docids(prefix1, prefix2, proximity)?;
    if !new_docids.is_empty() {
        *docids |= new_docids;
    }

    Ok(())
}
