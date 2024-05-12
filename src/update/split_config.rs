use deserr::Deserr;
use serde::{Deserialize, Serialize};
use crate::update::Setting;


#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct SplitJoinConfig{
    pub split_take_n: usize,
    pub ngram: usize
}

impl Default for SplitJoinConfig {
    fn default() -> Self {
        SplitJoinConfig{
            split_take_n: 4,
            ngram: 3
        }
    }
}

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct SplitJoinSetting{
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub split_take_n: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub ngram: Setting<usize>
}