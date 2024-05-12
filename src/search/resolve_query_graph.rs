use roaring::RoaringBitmap;
use crate::search::context::{Context, Fid, Position};
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

pub fn resolve_node_docids(node: &GraphNode, context: &impl Context) -> Result<RoaringBitmap>{
    match &node.data {
        NodeData::Term(term) => {
            resolve_docids(term, context)
        }
        _ => context.all_docids()
    }
}

pub fn resolve_positions(term: &Term, context: &impl Context) -> Result<Vec<((Fid,Position), RoaringBitmap)>>{
    match &term {

        Term{ term_kind:
            TermKind::Normal(OriginalTerm::Word(word)) |
            TermKind::Exact(OriginalTerm::Word(word)) |
            TermKind::Derivative(DerivativeTerm::Ngram(word, ..), ..), ..
        } => {
            context.word_position_docids(word)
        },
        Term{ term_kind:
            TermKind::Normal(OriginalTerm::Prefix(prefix)) |
            TermKind::Exact(OriginalTerm::Prefix(prefix)), ..
        } => {
            context.prefix_position_docids(prefix)
        },
        Term{ term_kind:
            TermKind::Normal(OriginalTerm::Phrase(phrase)) |
            TermKind::Exact(OriginalTerm::Phrase(phrase)), ..
        } => {
            phrase_position_resolve(phrase, context)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Typo(words, ..) | DerivativeTerm::Synonym(words), ..) , .. } => {
            let mut result = Vec::new();
            for word in words{
                result.extend(context.word_position_docids(word)?);
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(words, .. ), ..), .. } => {
            let mut result = Vec::new();
            for word in words{
                result.extend(split_position_resolve(word, context)?);
            }

            Ok(result)
        },
        _ => todo!(),
        }
}

pub fn resolve_docids(term: &Term, context: &mut impl Context) -> Result<RoaringBitmap>{
    match &term {

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
            phrase_resolve(phrase, context)
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
                result |= split_resolve(split,context)?;
            }

            Ok(result)
        },
        _ => todo!(),
    }
}

pub fn resolve_start_positions(term: &Term, context: &impl Context) -> Result<Vec<((Fid,Position), RoaringBitmap)>>{
    match &term {

        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Word(word)) |
        TermKind::Exact(OriginalTerm::Word(word)) |
        TermKind::Derivative(DerivativeTerm::Ngram(word, ..), ..), ..
        } => {
            context.word_position_docids(word)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Prefix(prefix)) |
        TermKind::Exact(OriginalTerm::Prefix(prefix)), ..
        } => {
            context.prefix_position_docids(prefix)
        },
        Term{ term_kind:
        TermKind::Normal(OriginalTerm::Phrase(phrase)) |
        TermKind::Exact(OriginalTerm::Phrase(phrase)), ..
        } => {
            let mut positions = phrase_position_resolve(phrase, context)?;
            for (position,_) in &mut positions{
                position.1 -= phrase.len() as u32 - 1;
            }
            Ok(positions)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Typo(words, ..) | DerivativeTerm::Synonym(words), ..) , .. } => {
            let mut result = Vec::new();
            for word in words{
                result.extend(context.word_position_docids(word)?);
            }

            Ok(result)
        },
        Term{ term_kind: TermKind::Derivative(DerivativeTerm::Split(words, .. ), ..), .. } => {
            let mut result = Vec::new();
            for word in words{
                let mut positions = split_position_resolve(word, context)?;
                for (position,_) in &mut positions{
                    position.1 -= 1;
                }
                result.extend(positions);
            }

            Ok(result)
        },
        _ => todo!(),
    }
}

pub fn phrase_resolve(phrase: &Vec<String>, context: &mut impl Context) -> Result<RoaringBitmap>{
    if phrase.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    let mut candidates = RoaringBitmap::new();
    for word in phrase.iter() {
        if let Some(word_docids) = context.word_docids(word)? {
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
                    match context.word_pair_proximity_docids(s1, s2, 1)? {
                        Some(m) => bitmaps.push(m),
                        None => return Ok(RoaringBitmap::new()),
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        if let Some(m) =
                            context.get_db_word_pair_proximity_docids(s1, s2, dist as u8 + 1)?
                        {
                            bitmap |= m;
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

pub fn phrase_position_resolve(phrase: &Vec<String>, context: &impl Context) -> Result<Vec<((Fid, Position), RoaringBitmap)>>{
    let mut phrase = phrase.iter();
    let Some(word) = phrase.next() else {
        return Ok(Vec::new())
    };
    let mut positions = context.word_position_docids(word)?;
    for word in phrase{
        for (position, ref mut bitmap) in &mut positions{
            position.1+=1;
            let docids =context.word_within_position_docids(word, *position)?;
            *bitmap &= docids;

        }
    }

    Ok(positions)
}

pub fn split_position_resolve(split: &(String, String), context: &impl Context) -> Result<Vec<((Fid,Position), RoaringBitmap)>>{

    let mut positions = context.word_position_docids(&split.0)?;
    for (position, ref mut bitmap) in &mut positions{
        position.1+=1;
        let docids =context.word_within_position_docids(&split.1, *position)?;
        *bitmap &= docids;

    }

    Ok(positions)
}

pub fn split_resolve(split: &(String, String), context: &impl Context) -> Result<RoaringBitmap>{

    let positions = split_position_resolve(split, context)?;
    let mut result = RoaringBitmap::new();
    for (_, docids) in positions{
        result |= docids;
    }

    Ok(result)
}