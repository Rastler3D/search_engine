use deserr::Deserr;
use serde::{Deserialize, Serialize};
use crate::update::Setting;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct TypoConfig{
    pub max_typos: u32,
    pub word_len_one_typo: u32,
    pub word_len_two_typo: u32,
}

impl TypoConfig {
    pub fn allowed_typos(&self, word: &str) -> u8{
        let len = word.chars().count() as u32;
        if len < self.word_len_one_typo || self.max_typos == 0{
            0
        } else if len < self.word_len_two_typo {
            1
        } else {
            2
        }
    }
}

impl Default for TypoConfig {
    fn default() -> Self {
        TypoConfig{
            max_typos: 20,
            word_len_one_typo: 4,
            word_len_two_typo: 7
        }
    }
}

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct TypoSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_typos: Setting<u32>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub word_len_one_typo: Setting<u32>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub word_len_two_typo: Setting<u32>,
}