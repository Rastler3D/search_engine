use std::borrow::Cow;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Instant;
use fst::{IntoStreamer, Streamer};
use fst::automaton::Str;
use itertools::Itertools;
use levenshtein_automata::LevenshteinAutomatonBuilder;
use thiserror::Error;
use crate::search::context::Context;
use crate::search::fst_utils::{Complement, Intersection, StartsWith, Union};
use crate::search::query_parser::{DerivativeTerm, OriginalTerm, Term, TermKind};
use crate::search::utils::bit_set::BitSet;
use crate::search::utils::vec_map::VecMap;
use crate::Result;
use crate::update::split_config::SplitJoinConfig;
use crate::update::typo_config::TypoConfig;

static LEVDIST2: LazyLock<LevenshteinAutomatonBuilder> = LazyLock::new(|| LevenshteinAutomatonBuilder::new(2, true));
static LEVDIST1: LazyLock<LevenshteinAutomatonBuilder> = LazyLock::new(|| LevenshteinAutomatonBuilder::new(1, true));

#[derive(Debug)]
pub enum NodeData{
    Start,
    Term(Term),
    End
}

#[derive(Debug)]
pub struct GraphNode{
    pub data: NodeData,
    pub predecessors: BitSet,
    pub successors: BitSet
}

#[derive(Debug)]
pub struct QueryGraph{
    pub root: usize,
    pub end: usize,
    pub nodes: Vec<GraphNode>,
    pub query_word: usize
}

#[derive(Error, Debug)]
pub enum QueryGraphError{
    #[error(transparent)]
    Heed(#[from] heed::Error)
}

impl QueryGraph {
    pub fn from_query(terms: Vec<Term>, context: &mut impl Context) -> Result<QueryGraph>{

        let graph = Self::build_flat_graph(terms)
            .ngrams(context)?
            .prefixes(context)?
            .typos(context)?
            .synonyms(context)?
            .splits(context)?;
        Ok(graph)
    }

    fn ngrams(mut self, context: &mut impl Context) -> Result<QueryGraph>{
        let ngram = context.split_join_config()?.ngram;

       'outer: for n in 2..=ngram{
            for idx in (self.root+1..self.end){
                let Some(nodes) = self.nodes.get(idx..idx+n) else {
                    break 'outer
                };
                let node = Self::make_ngram(nodes, idx+n - 1);
                if let Some(node) = node{
                    self.insert_node(node)
                }
            }
        }

        Ok(self)
    }

    fn make_ngram(nodes: &[GraphNode], orig_idx: usize) -> Option<GraphNode>{
        let mut term = String::new();
        let mut predecessors = BitSet::new();
        let mut successors = BitSet::new();
        let mut start = 0;
        let mut end = 0;
        let mut is_negative = false;
        let mut is_prefix = false;

        for node in nodes{
            match &node.data {
                NodeData::Term(orig_term) => {
                    match &orig_term.term_kind {
                        TermKind::Normal(kind@(OriginalTerm::Prefix(ref text) | OriginalTerm::Word(ref text))) => {
                            if term.is_empty(){
                                predecessors = node.predecessors;
                                is_negative = orig_term.is_negative;
                                start = *orig_term.position.start();
                            }
                            successors = node.successors;
                            is_prefix = matches!(kind, OriginalTerm::Prefix(_));
                            end = *orig_term.position.end();

                            term.push_str(text)
                        }
                        _ => { return None }
                    }
                }
                _ => return None
            }
        }

        let node = GraphNode{
            successors,
            predecessors,
            data: NodeData::Term(Term{
                term_kind: TermKind::Derivative(DerivativeTerm::Ngram(term, nodes.len() as u8), orig_idx),
                is_negative,
                position: start..=end,
            })
        };

        Some(node)
    }

    fn prefixes(mut self, _: &mut impl Context) -> Result<QueryGraph>{
        let mut nodes = Vec::new();
        for node in &self.nodes{
            match &node.data {
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Prefix(text)), .. }) => {
                    nodes.push(GraphNode{
                        predecessors: node.predecessors,
                        successors: node.successors,
                        data: NodeData::Term(Term{
                            position: term.position.clone(),
                            is_negative: term.is_negative,
                            term_kind: TermKind::Normal(OriginalTerm::Word(text.clone()))
                        })
                    });
                },
                _ => continue
            }
        }

        for node in nodes {
            self.insert_node(node)
        }

        Ok(self)
    }
    fn get_first(s: &str) -> &str {
        match s.chars().next() {
            Some(c) => &s[..c.len_utf8()],
            None => panic!("unexpected empty query"),
        }
    }

    pub fn typos(mut self, context: &mut impl Context) -> Result<QueryGraph>{
        let typo_config = context.typo_config()?;
        let words = context.exact_words()?;
        let mut nodes = Vec::new();
        'node: for (idx,node) in self.nodes.iter().enumerate(){
            match &node.data {
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Prefix(text)), .. }) => {
                    let typos_allowed = typo_config.allowed_typos(&text);
                    let dfa = match typos_allowed {
                        1 => LEVDIST1.build_prefix_dfa(&text),
                        2 => LEVDIST2.build_prefix_dfa(&text),
                        _ => continue 'node,
                    };
                    let mut typos = 0;
                    let mut one_typos = Vec::new();
                    let mut two_typos = Vec::new();

                    let mut stream = words.search_with_state(&dfa).into_stream();
                    while let Some((derived_word, state)) = stream.next(){
                        if typos >= typo_config.max_typos{
                            continue 'node;
                        } else { typos += 1 }
                        let Ok(derived_word) = std::str::from_utf8(derived_word) else { continue };

                        let typos = dfa.distance(state).to_u8();
                        if typos >=2{
                            two_typos.push(derived_word.to_string());
                        } else if typos == 1 {
                            one_typos.push(derived_word.to_string());
                        }
                    }

                    if !one_typos.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(one_typos, 1), idx)
                            })
                        });
                    }
                    if !two_typos.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::PrefixTypo(two_typos, 2), idx)
                            })
                        });
                    }


                },
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Word(text)), .. }) => {
                    let typos_allowed = typo_config.allowed_typos(&text);
                    let dfa = match typos_allowed {
                        1 => LEVDIST1.build_dfa(&text),
                        2 => LEVDIST2.build_dfa(&text),
                        _ => continue,
                    };


                    let mut stream = words.search_with_state(&dfa).into_stream();

                    let mut typos = 0;
                    let mut one_typos = Vec::new();
                    let mut two_typos = Vec::new();

                    while let Some((derived_word, state)) = stream.next(){
                        if typos >= typo_config.max_typos{
                            continue 'node;
                        } else { typos += 1 }
                        let Ok(derived_word) = std::str::from_utf8(derived_word) else { continue };

                        let typos = dfa.distance(state).to_u8();

                        if typos >=2{
                            two_typos.push(derived_word.to_string());
                        } else if typos == 1 {
                            one_typos.push(derived_word.to_string());
                        }
                    }
                    if !one_typos.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::Typo(one_typos, 1), idx)
                            })
                        });
                    }
                    if !two_typos.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::Typo(two_typos, 2), idx)
                            })
                        });
                    }
                },
                _ => continue
            }
        }

        for node in nodes {
            self.insert_node(node)
        }

        Ok(self)
    }

    fn synonyms(mut self, context: &mut impl Context) -> Result<QueryGraph>{
        let synonyms = context.synonyms()?;
        let mut nodes = Vec::new();
        for (idx,node) in self.nodes.iter().enumerate(){
            match &node.data {
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Word(text)) | TermKind::Exact(OriginalTerm::Word(text)), .. }) => {
                    let synonyms = synonyms.get(&[text.to_string()] as &[String]);
                    let mut synonym_words = Vec::new();
                    let mut synonym_phrases = Vec::new();
                    for synonym in synonyms.into_iter().flatten(){
                        if synonym.len() == 1{
                            synonym_words.push(synonym[0].to_string());
                        } else {
                            synonym_phrases.push(synonym.clone());
                        }
                    }

                    if !synonym_words.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::Synonym(synonym_words), idx)
                            })
                        });
                    }

                    if !synonym_phrases.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonym_phrases), idx)
                            })
                        });
                    }

                },
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Phrase(phrase)) | TermKind::Exact(OriginalTerm::Phrase(phrase)), .. }) => {
                    let synonyms = synonyms.get(&phrase as &[String]);

                    let mut synonym_words = Vec::new();
                    let mut synonym_phrases = Vec::new();
                    for synonym in synonyms.into_iter().flatten(){
                        if synonym.len() == 1{
                            synonym_words.push(synonym[0].to_string());
                        } else {
                            synonym_phrases.push(synonym.clone());
                        }
                    }

                    if !synonym_words.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::Synonym(synonym_words), idx)
                            })
                        });
                    }

                    if !synonym_phrases.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::SynonymPhrase(synonym_phrases), idx)
                            })
                        });
                    }

                },
                _ => continue
            }
        }

        for node in nodes {
            self.insert_node(node)
        }

        Ok(self)
    }

    fn splits(mut self, context: &mut impl Context) -> Result<QueryGraph>{
        let mut nodes = Vec::new();
        let split_config = context.split_join_config()?;
        for (idx,node) in self.nodes.iter().enumerate(){
            match &node.data {
                NodeData::Term(term@Term{ term_kind: TermKind::Normal(OriginalTerm::Word(text)), .. }) => {
                    let splits = Self::split_best_frequency(
                        |left, right| context.word_pair_frequency(left, right, 1),
                        split_config.split_take_n,
                        &text
                    )?.collect::<Vec<_>>();

                    if !splits.is_empty(){
                        nodes.push(GraphNode{
                            predecessors: node.predecessors,
                            successors: node.successors,
                            data: NodeData::Term(Term{
                                position: term.position.clone(),
                                is_negative: term.is_negative,
                                term_kind: TermKind::Derivative(DerivativeTerm::Split(splits), idx)
                            })
                        });
                    }

                },
                _ => continue
            }
        }

        for node in nodes {
            self.insert_node(node)
        }

        Ok(self)
    }

    fn split_best_frequency(
        mut word_pair_frequency: impl FnMut(&str, &str) -> Result<u64>,
        take_top: usize,
        word: &str,
    ) -> Result<impl Iterator<Item = (String, String)> + '_> {
        let chars = word.char_indices().skip(1);
        let mut best = Vec::new();
        for (i, _) in chars {
            let (left, right) = word.split_at(i);

            let pair_freq = word_pair_frequency(left, right)?;

            if pair_freq != 0 {
                best.push((pair_freq, left, right));
            }
        }
        best.sort_unstable_by_key(|x| x.0);

        Ok(best.into_iter().rev().map(|x| (x.1.to_string(), x.2.to_string())).take(take_top))
    }
    fn insert_node(&mut self, node: GraphNode){
        let node_id = self.nodes.len();
        for predecessor in node.predecessors.iter(){
            self.nodes[predecessor].successors.insert(node_id);
        }

        for successor in node.successors.iter(){
            self.nodes[successor].predecessors.insert(node_id);
        }

        self.nodes.push(node);
    }
    fn build_flat_graph(terms: Vec<Term>) -> QueryGraph{
        let mut nodes = vec![GraphNode{ data: NodeData::Start, successors: BitSet::default(), predecessors: BitSet::default() }];
        let mut last_id = 0;
        let mut query_words = 0;
        for term in terms{
            last_id = Self::append_successor(&mut nodes, last_id, NodeData::Term(term));
            query_words+=1;
        }

        last_id = Self::append_successor(&mut nodes, last_id, NodeData::End);

        QueryGraph{
            root: 0,
            end: last_id,
            nodes: nodes,
            query_word: query_words
        }
    }

    fn append_successor(nodes: &mut Vec<GraphNode>, node_id: usize,  successor: NodeData) -> usize{
        let successor_id = nodes.len();
        nodes[node_id].successors.insert(successor_id);
        nodes.push(GraphNode{
            data: successor,
            successors: BitSet::new(),
            predecessors: BitSet::from_iter([node_id])
        });

        successor_id
    }
}


#[cfg(test)]
pub mod tests {
    use fst::Set;
    use crate::search::query_parser::parse_query;
    use crate::search::query_parser::tests::build_analyzer;
    use analyzer::analyzer::Analyzer;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use roaring::RoaringBitmap;
    use crate::Criterion;
    use crate::search::context::{Fid, Position};
    use crate::update::split_config::SplitJoinConfig;
    use super::*;

    #[derive(Debug)]
    pub struct TestContext {
        synonyms: HashMap<Vec<String>, Vec<Vec<String>>>,
        postings: HashMap<String, RoaringBitmap>,
        positions: HashMap<String, Vec<((Fid, Position),RoaringBitmap)>>,
        exact_words: fst::Set<Cow<'static, [u8]>>,
        all_docids: RoaringBitmap
    }
    impl Context for TestContext {
        fn word_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
            Ok(self.postings.get(word).cloned().unwrap_or(RoaringBitmap::new()))
        }


        fn prefix_docids(&mut self, word: &str) -> Result<RoaringBitmap> {
            todo!()
        }


        fn synonyms(&self) -> Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
            Ok(self.synonyms.clone())
        }

        fn word_documents_count(&mut self, word: &str) -> Result<u64> {
            todo!()
        }

        fn all_docids(&self) -> Result<RoaringBitmap> {
            Ok(self.all_docids.clone())
        }

        fn split_join_config(&self) -> Result<SplitJoinConfig> {
            Ok(SplitJoinConfig{
                split_take_n: 5,
                ngram: 3
            })
        }


        fn typo_config(&self) -> Result<TypoConfig> {
            Ok(TypoConfig{
                max_typos: 100,
                word_len_two_typo: 5,
                word_len_one_typo: 3
            })
        }


        fn exact_words(&mut self) -> Result<Set<Cow<[u8]>>>{
            Ok(self.exact_words.clone())
        }

        fn word_pair_frequency(
            &mut self,
            left_word: &str,
            right_word: &str,
            _proximity: u8,
        ) -> Result<u64> {
            match self.word_docids(&format!("{} {}", left_word, right_word)) {
                Ok(rb) => Ok(rb.len()),
                _ => Ok(0),
            }
        }

        fn word_position_docids(&mut self, word: &str, position: Position) -> Result<RoaringBitmap> {
            // let res =match self.positions.get(word) {
            //     Some(pos)=> Ok(pos.clone()),
            //     _ => Ok(Vec::new()),
            // };
            // res
            todo!()
        }


        fn word_positions(&mut self, word: &str) -> Result<Vec<Position>> {
            todo!()
        }

        fn prefix_position_docids(&mut self, word: &str, position: Position) -> Result<RoaringBitmap> {
            todo!()
        }

        fn prefix_positions(&mut self, word: &str) -> Result<Vec<Position>> {
            todo!()
        }

        fn word_prefix_pair_proximity_docids(&mut self, word: &str, prefix: &str, proximity: u8) -> Result<RoaringBitmap> {
            match self.word_docids(&format!("{} {}", word, prefix)) {
                Ok(rb) => Ok(rb),
                _ => Ok(RoaringBitmap::new()),
            }
        }

        fn prefix_word_pair_proximity_docids(&mut self, prefix: &str, word: &str, proximity: u8) -> Result<RoaringBitmap> {
            match self.word_docids(&format!("{} {}", word, prefix)) {
                Ok(rb) => Ok(rb),
                _ => Ok(RoaringBitmap::new()),
            }
        }

        fn word_pair_proximity_docids(&mut self, word1: &str, word2: &str, proximity: u8) -> Result<RoaringBitmap> {
            match self.word_docids(&format!("{} {}", word1, word2)) {
                Ok(rb) => Ok(rb),
                _ => Ok(RoaringBitmap::new()),
            }
        }

        fn prefix_prefix_pair_proximity_docids(&mut self, prefix1: &str, prefix2: &str, proximity: u8) -> Result<RoaringBitmap> {
            match self.word_docids(&format!("{} {}", prefix1, prefix2)) {
                Ok(rb) => Ok(rb),
                _ => Ok(RoaringBitmap::new()),
            }
        }


        fn ranking_rules(&self) -> Result<Vec<Criterion>> {
            Ok(vec![Criterion::Proximity])
        }

        fn word_fid_docids(&mut self, word: &str, fid: Fid) -> Result<RoaringBitmap> {
            todo!()
        }

        fn prefix_fid_docids(&mut self, prefix: &str, fid: Fid) -> Result<RoaringBitmap> {
            todo!()
        }

        fn word_fids(&mut self, word: &str) -> Result<Vec<Fid>> {
            todo!()
        }

        fn prefix_fids(&mut self, prefix: &str) -> Result<Vec<Fid>> {
            todo!()
        }

        fn node_docids(&mut self, node_id: usize, graph: &QueryGraph) -> Result<&RoaringBitmap> {
            todo!()
        }

        fn path_docids(&mut self, path: BitSet, graph: &QueryGraph) -> Result<&RoaringBitmap> {
            todo!()
        }

        fn phrase_docids(&mut self, path: &[String]) -> Result<&RoaringBitmap> {
            todo!()
        }

        fn split_docids(&mut self, path: &(String, String)) -> Result<&RoaringBitmap> {
            todo!()
        }
    }

    impl Default for TestContext {
        fn default() -> TestContext {
            let mut rng = StdRng::seed_from_u64(102);
            let rng = &mut rng;

            fn random_postings<R: Rng>(rng: &mut R, len: usize) -> RoaringBitmap {
                let mut values = Vec::<u32>::with_capacity(len);
                let rnd =
                while values.len() != len {
                    values.push(values.len() as u32);
                };
                values.sort_unstable();
                RoaringBitmap::from_sorted_iter(values.into_iter()).unwrap()
            }

            fn random_position<R: Rng>(rng: &mut R, len: usize) -> Vec<((Fid,Position), RoaringBitmap)> {
                let mut values = Vec::with_capacity(len);
                while values.len() != len {
                    let fid = rng.gen_range(0u16..10);
                    let position = rng.gen_range(0u16..100);
                    values.push(((fid,position), values.len()  as u32));
                }
                let mut result = Vec::new();
                for (key, val) in &values.iter().group_by(|&&x| x.0){
                    let bitset = RoaringBitmap::from_iter(val.map(|x| x.1));
                    result.push((key, bitset));
                }
                result
            }
            let mut fst = fst::SetBuilder::memory();
            fst.insert("hello").unwrap();
            fst.insert("hhewlo").unwrap();
            fst.insert("hhewwo").unwrap();
            fst.insert("hnello").unwrap();

            let exact_words = fst.into_set().map_data(Cow::Owned).unwrap();

            let mut context = TestContext {
                synonyms: HashMap::from([
                    (vec![String::from("hello")], vec![vec![String::from("hi")], vec![String::from("good"), String::from("morning")]]),
                    (vec![String::from("world")], vec![vec![String::from("earth")], vec![String::from("nature")], ]),
                    (vec![String::from("nyc")], vec![vec![String::from("new"), String::from("york")], vec![String::from("new"), String::from("york"), String::from("city")], ]),
                    (vec![String::from("new"), String::from("york")], vec![vec![String::from("nyc")], vec![String::from("new"), String::from("york"), String::from("city")], ]),
                    (vec![String::from("new"), String::from("york"), String::from("city")], vec![vec![String::from("nyc")], vec![String::from("new"), String::from("york")], ]),
                ]),
                postings: HashMap::from([
                    (String::from("hello"),random_postings(rng,   1500)),
                    (String::from("hi"),random_postings(rng,   4000)),
                    (String::from("word"),random_postings(rng,   2500)),
                    (String::from("split"),random_postings(rng,    400)),
                    (String::from("ngrams"),random_postings(rng,   1400)),
                    (String::from("world"),random_postings(rng, 15_000)),
                    (String::from("earth"),random_postings(rng,   8000)),
                    (String::from("2021"),random_postings(rng,    100)),
                    (String::from("2020"),random_postings(rng,    500)),
                    (String::from("is"),random_postings(rng, 50_000)),
                    (String::from("this"),random_postings(rng, 50_000)),
                    (String::from("good"),random_postings(rng,   1250)),
                    (String::from("morning"),random_postings(rng,    125)),
                    (String::from("word split"),random_postings(rng,   5000)),
                    (String::from("quick brownfox"),random_postings(rng,   7000)),
                    (String::from("quickbrown fox"),random_postings(rng,   8000)),
                ]),
                positions: HashMap::from([
                    (String::from("hello"),random_position(rng,   1500)),
                    (String::from("hi"),random_position(rng,   4000)),
                    (String::from("word"),random_position(rng,   2500)),
                    (String::from("split"),random_position(rng,    400)),
                    (String::from("ngrams"),random_position(rng,   1400)),
                    (String::from("world"),random_position(rng, 15_000)),
                    (String::from("earth"),random_position(rng,   8000)),
                    (String::from("2021"),random_position(rng,    100)),
                    (String::from("2020"),random_position(rng,    500)),
                    (String::from("is"),random_position(rng, 50_000)),
                    (String::from("this"),random_position(rng, 50_000)),
                    (String::from("good"),random_position(rng,   1250)),
                    (String::from("morning"),random_position(rng,    125)),
                    (String::from("word split"),random_position(rng,   5000)),
                    (String::from("quick brownfox"),random_position(rng,   7000)),
                    (String::from("quickbrown fox"),random_position(rng,   8000)),
                ]),
                exact_words,
                all_docids: RoaringBitmap::new()
            };

            let mut docids = RoaringBitmap::new();
            for value in context.positions.values(){
                for value in value{
                    docids |= &value.1;
                }

            }
            context.all_docids = docids;

            context
        }
    }

    #[test]
    fn test_flat_graph() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context);

        println!("{:#?}", query_graph);
    }

    #[test]
    fn visualize_graph() {
        let analyzer = build_analyzer();
        let stream = analyzer.analyze("Hello world HWWs Swwws earth quickbrownfox");
        let parsed_query = parse_query(stream);
        let mut context = TestContext::default();
        let query_graph = QueryGraph::from_query(parsed_query, &mut context);
        println!("{:#?}", query_graph.unwrap());
    }
}