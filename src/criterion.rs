use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AscDesc, Member};

#[derive(Error, Debug)]
pub enum CriterionError {
    #[error("Правило ранжирования `{name}` недействительно. К допустимым правилам ранжирования относятся: words, typo, sort, proximity, attribute, exactness и пользовательские правила ранжирования.")]
    InvalidName { name: String },
    #[error("`{name}` является зарезервированным ключевым словом и поэтому не может быть использовано в качестве правила ранжирования")]
    ReservedName { name: String },
    #[error(
        "`{name}` является зарезервированным ключевым словом и поэтому не может быть использовано в качестве правила ранжирования. \
`{name}` может быть использовано только для сортировки во время поиска"
    )]
    ReservedNameForSort { name: String },
    #[error(
        "`{name}` является зарезервированным ключевым словом и поэтому не может быть использовано в качестве правила ранжирования. \
`{name}` может быть использовано только для фильтрации во время поиска"
    )]
    ReservedNameForFilter { name: String },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {

    Words,

    Typo,

    Proximity,

    Attribute,

    Sort,

    Exactness,

    Asc(String),

    Desc(String),
}

impl Criterion {

    pub fn field_name(&self) -> Option<&str> {
        match self {
            Criterion::Asc(name) | Criterion::Desc(name) => Some(name),
            _otherwise => None,
        }
    }
}

impl FromStr for Criterion {
    type Err = CriterionError;

    fn from_str(text: &str) -> Result<Criterion, Self::Err> {
        match text {
            "words" => Ok(Criterion::Words),
            "typo" => Ok(Criterion::Typo),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "sort" => Ok(Criterion::Sort),
            "exactness" => Ok(Criterion::Exactness),
            text => match AscDesc::from_str(text)? {
                AscDesc::Asc(Member::Field(field)) => Ok(Criterion::Asc(field)),
                AscDesc::Desc(Member::Field(field)) => Ok(Criterion::Desc(field)),
            },
        }
    }
}

pub fn default_criteria() -> Vec<Criterion> {
    vec![
        Criterion::Words,
        Criterion::Typo,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::Sort,
        Criterion::Exactness,
    ]
}

impl fmt::Display for Criterion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Criterion::*;

        match self {
            Words => f.write_str("words"),
            Typo => f.write_str("typo"),
            Proximity => f.write_str("proximity"),
            Attribute => f.write_str("attribute"),
            Sort => f.write_str("sort"),
            Exactness => f.write_str("exactness"),
            Asc(attr) => write!(f, "{}:asc", attr),
            Desc(attr) => write!(f, "{}:desc", attr),
        }
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use CriterionError::*;

    use super::*;

    #[test]
    fn parse_criterion() {
        let valid_criteria = [
            ("words", Criterion::Words),
            ("typo", Criterion::Typo),
            ("proximity", Criterion::Proximity),
            ("attribute", Criterion::Attribute),
            ("sort", Criterion::Sort),
            ("exactness", Criterion::Exactness),
            ("price:asc", Criterion::Asc(S("price"))),
            ("price:desc", Criterion::Desc(S("price"))),
            ("price:asc:desc", Criterion::Desc(S("price:asc"))),
            ("truc:machin:desc", Criterion::Desc(S("truc:machin"))),
            ("hello-world!:desc", Criterion::Desc(S("hello-world!"))),
            ("it's spacy over there:asc", Criterion::Asc(S("it's spacy over there"))),
        ];

        for (input, expected) in valid_criteria {
            let res = input.parse::<Criterion>();
            assert!(
                res.is_ok(),
                "Failed to parse `{}`, was expecting `{:?}` but instead got `{:?}`",
                input,
                expected,
                res
            );
            assert_eq!(res.unwrap(), expected);
        }

        let invalid_criteria = [
            ("words suffix", InvalidName { name: S("words suffix") }),
            ("prefix typo", InvalidName { name: S("prefix typo") }),
            ("proximity attribute", InvalidName { name: S("proximity attribute") }),
            ("price", InvalidName { name: S("price") }),
            ("asc:price", InvalidName { name: S("asc:price") }),
            ("price:deesc", InvalidName { name: S("price:deesc") }),
            ("price:aasc", InvalidName { name: S("price:aasc") }),
            ("price:asc and desc", InvalidName { name: S("price:asc and desc") }),
            ("price:asc:truc", InvalidName { name: S("price:asc:truc") }),
            ("_geo:asc", ReservedName { name: S("_geo") }),
            ("_geoDistance:asc", ReservedName { name: S("_geoDistance") }),
            ("_geoPoint:asc", ReservedNameForSort { name: S("_geoPoint") }),
            ("_geoPoint(42, 75):asc", ReservedNameForSort { name: S("_geoPoint") }),
            ("_geoRadius:asc", ReservedNameForFilter { name: S("_geoRadius") }),
            ("_geoRadius(42, 75, 59):asc", ReservedNameForFilter { name: S("_geoRadius") }),
            ("_geoBoundingBox:asc", ReservedNameForFilter { name: S("_geoBoundingBox") }),
            (
                "_geoBoundingBox([42, 75], [75, 59]):asc",
                ReservedNameForFilter { name: S("_geoBoundingBox") },
            ),
        ];

        for (input, expected) in invalid_criteria {
            let res = input.parse::<Criterion>();
            assert!(
                res.is_err(),
                "Should no be able to parse `{}`, was expecting an error but instead got: `{:?}`",
                input,
                res
            );
            let res = res.unwrap_err();
            assert_eq!(
                res.to_string(),
                expected.to_string(),
                "Bad error for input {}: got `{:?}` instead of `{:?}`",
                input,
                res,
                expected
            );
        }
    }
}
