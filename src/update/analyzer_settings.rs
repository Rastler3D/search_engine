use std::collections::HashMap;
use std::sync::Arc;
use deserr::Deserr;
use serde::{Deserialize, Serialize};
use serde_json::value::{RawValue, to_raw_value};
use analyzer::analyzer::{Analyzer, BoxAnalyzer};
use analyzer::char_filter::{character_filter_layer};
use analyzer::tokenizer::BoxTokenizer;
use analyzer::analyzer::text_analyzer::TextAnalyzer;
use analyzer::language_detection::whichlang::WhichLangDetector;
use analyzer::token_filter::lower_case::LowerCaseFilter;
use analyzer::token_filter::token_filter_layer::{BaseLevel, TokenFilterLayers};
use analyzer::tokenizer::whitespace_tokenizer::WhitespaceTokenizer;
use crate::update::Setting;
use crate::vector::settings::EmbeddingSettings;


#[derive(Clone, Serialize, Deserialize, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct AnalyzerSettings {
    #[deserr(try_from(&String) = serde_json::from_str -> serde_json::error::Error)]
    analyzer: BoxAnalyzer
}

impl AnalyzerSettings{
    pub(crate) fn need_reindex(
        old: &Setting<AnalyzerConfig>,
        new: &Setting<AnalyzerConfig>,
    ) -> bool {
        match (old, new) {
            (
                Setting::Set(old),
                Setting::Set(new),
            ) => {
                old != new
            }
            (Setting::Reset, Setting::Reset) | (_, Setting::NotSet) => false,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AnalyzerConfig {
    pub analyzer_config: Box<RawValue>,
}

impl Eq for AnalyzerConfig {

}

impl PartialEq for AnalyzerConfig{
    fn eq(&self, other: &Self) -> bool {
        self.analyzer_config.get() == other.analyzer_config.get()
    }
}

impl From<AnalyzerSettings> for AnalyzerConfig{
    fn from(value: AnalyzerSettings) -> Self {
        let analyzer_config = to_raw_value(&value).unwrap();

        Self{ analyzer_config }
    }
}

impl AnalyzerConfig{
    pub fn into_string(self) -> String{
        Box::<str>::from(self.analyzer_config).into_string()
    }
}
#[derive(Clone, Default)]
pub struct AnalyzerConfigs(HashMap<String, AnalyzerConfig>);

impl AnalyzerConfigs {
    /// Create the map from its internal component.s
    pub fn new(data: impl IntoIterator<Item = (String, AnalyzerConfig)>) -> Self {
        Self(HashMap::from_iter(data))
    }

    /// Get an embedder configuration and template from its name.
    pub fn get(&self, name: &str) -> Option<BoxAnalyzer> {
        let analyzer = self.0.get(name)?;
        serde_json::from_str(analyzer.analyzer_config.get()).ok()
    }

    /// Get the default embedder configuration, if any.
    pub fn get_default(&self) -> Option<BoxAnalyzer> {
        self.get(self.get_default_analyzer_name())
    }

    pub fn get_default_analyzer_name(&self) -> &str {
        let mut it = self.0.keys();
        let first_name = it.next();
        let second_name = it.next();
        match (first_name, second_name) {
            (None, _) => "default",
            (Some(first), None) => first,
            (Some(_), Some(_)) => "default",
        }
    }
}

// impl IntoIterator for AnalyzerConfigs {
//     type Item = (String, (Arc<Embedder>, Arc<Prompt>));
//
//     type IntoIter = std::collections::hash_map::IntoIter<String, (Arc<Embedder>, Arc<Prompt>)>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         self.0.into_iter()
//     }
// }

impl Default for AnalyzerConfig {
    fn default() -> Self {
        AnalyzerConfig::from(AnalyzerSettings::default())
    }
}

impl Default for AnalyzerSettings {
    fn default() -> Self {
        Self{
            analyzer: default_analyzer()
        }
    }
}

pub fn default_analyzer() -> BoxAnalyzer{
    let token_filters= BaseLevel.wrap_layer(LowerCaseFilter {});

    let mut tokenizer = WhitespaceTokenizer {};
    let text = "Helloworld WorldHello";

    let mut analyzer = TextAnalyzer {
        character_filters: character_filter_layer::BaseLevel,
        language_detector: WhichLangDetector{},
        tokenizer: tokenizer,
        token_filters: token_filters,
    };
    BoxAnalyzer::new(analyzer)
}
