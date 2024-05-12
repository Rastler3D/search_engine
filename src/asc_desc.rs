//! This module provides the `AscDesc` type and defines all the errors related to this type.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

//use crate::search::facet::BadGeoError;
use crate::{CriterionError,  UserError};

/// This error type is never supposed to be shown to the end user.
/// You must always cast it to a sort error or a criterion error.
#[derive(Error, Debug)]
pub enum AscDescError {
    #[error("Invalid syntax for the asc/desc parameter: expected expression ending by `:asc` or `:desc`, found `{name}`.")]
    InvalidSyntax { name: String },
    #[error("`{name}` is a reserved keyword and thus can't be used as a asc/desc rule.")]
    ReservedKeyword { name: String },
}



impl From<AscDescError> for CriterionError {
    fn from(error: AscDescError) -> Self {
        match error {
            AscDescError::InvalidSyntax { name } => CriterionError::InvalidName { name },
            AscDescError::ReservedKeyword { name } => CriterionError::ReservedName { name },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Member {
    Field(String),
}

impl FromStr for Member {
    type Err = AscDescError;

    fn from_str(text: &str) -> Result<Member, Self::Err> {
        Ok(Member::Field(text.to_string()))
    }
}

impl fmt::Display for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Member::Field(name) => f.write_str(name),
        }
    }
}

impl Member {
    pub fn field(&self) -> Option<&str> {
        match self {
            Member::Field(field) => Some(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum AscDesc {
    Asc(Member),
    Desc(Member),
}

impl AscDesc {
    pub fn member(&self) -> &Member {
        match self {
            AscDesc::Asc(member) => member,
            AscDesc::Desc(member) => member,
        }
    }

    pub fn field(&self) -> Option<&str> {
        self.member().field()
    }
}

impl FromStr for AscDesc {
    type Err = AscDescError;

    fn from_str(text: &str) -> Result<AscDesc, Self::Err> {
        match text.rsplit_once(':') {
            Some((left, "asc")) => Ok(AscDesc::Asc(left.parse()?)),
            Some((left, "desc")) => Ok(AscDesc::Desc(left.parse()?)),
            _ => Err(AscDescError::InvalidSyntax { name: text.to_string() }),
        }
    }
}

#[derive(Error, Debug)]
pub enum SortError {
    #[error("Invalid syntax for the sort parameter: expected expression ending by `:asc` or `:desc`, found `{name}`.")]
    InvalidName { name: String },
    #[error("`{name}` is a reserved keyword and thus can't be used as a sort expression.")]
    ReservedName { name: String },
}

impl From<AscDescError> for SortError {
    fn from(error: AscDescError) -> Self {
        match error {
            AscDescError::InvalidSyntax { name } => SortError::InvalidName { name },
            AscDescError::ReservedKeyword { name } => SortError::ReservedName { name },
        }
    }
}

impl From<SortError> for crate::Error {
    fn from(error: SortError) -> Self {
        Self::UserError(UserError::SortError(error))
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use AscDesc::*;
    use AscDescError::*;
    use Member::*;

    use super::*;

    #[test]
    fn parse_asc_desc() {
        let valid_req = [
            ("truc:asc", Asc(Field(S("truc")))),
            ("bidule:desc", Desc(Field(S("bidule")))),
            ("a-b:desc", Desc(Field(S("a-b")))),
            ("a:b:desc", Desc(Field(S("a:b")))),
            ("a12:asc", Asc(Field(S("a12")))),
            ("42:asc", Asc(Field(S("42")))),
            ("truc(12, 13):desc", Desc(Field(S("truc(12, 13)")))),
        ];

        for (req, expected) in valid_req {
            let res = req.parse::<AscDesc>();
            assert!(
                res.is_ok(),
                "Failed to parse `{}`, was expecting `{:?}` but instead got `{:?}`",
                req,
                expected,
                res
            );
            assert_eq!(res.unwrap(), expected);
        }

        let invalid_req = [
            ("truc:machin", InvalidSyntax { name: S("truc:machin") }),
            ("truc:deesc", InvalidSyntax { name: S("truc:deesc") }),
            ("truc:asc:deesc", InvalidSyntax { name: S("truc:asc:deesc") }),
            ("42desc", InvalidSyntax { name: S("42desc") }),
            ("_geoPoint:asc", ReservedKeyword { name: S("_geoPoint") }),
            ("_geoDistance:asc", ReservedKeyword { name: S("_geoDistance") }),
            ("_geoPoint(42.12 , 59.598)", InvalidSyntax { name: S("_geoPoint(42.12 , 59.598)") }),
            (
                "_geoPoint(42.12 , 59.598):deesc",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):deesc") },
            ),
            (
                "_geoPoint(42.12 , 59.598):machin",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):machin") },
            ),
            (
                "_geoPoint(42.12 , 59.598):asc:aasc",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):asc:aasc") },
            ),
            (
                "_geoPoint(42,12 , 59,598):desc",
                ReservedKeyword { name: S("_geoPoint(42,12 , 59,598)") },
            ),
            ("_geoPoint(35, 85, 75):asc", ReservedKeyword { name: S("_geoPoint(35, 85, 75)") }),
            ("_geoPoint(18):asc", ReservedKeyword { name: S("_geoPoint(18)") }),
            // ("_geoPoint(200, 200):asc", GeoError(BadGeoError::Lat(200.))),
            // ("_geoPoint(90.000001, 0):asc", GeoError(BadGeoError::Lat(90.000001))),
            // ("_geoPoint(0, -180.000001):desc", GeoError(BadGeoError::Lng(-180.000001))),
            // ("_geoPoint(159.256, 130):asc", GeoError(BadGeoError::Lat(159.256))),
            // ("_geoPoint(12, -2021):desc", GeoError(BadGeoError::Lng(-2021.))),
            ("_geo(12, -2021):asc", ReservedKeyword { name: S("_geo(12, -2021)") }),
            ("_geo(12, -2021):desc", ReservedKeyword { name: S("_geo(12, -2021)") }),
            ("_geoDistance(12, -2021):asc", ReservedKeyword { name: S("_geoDistance(12, -2021)") }),
            (
                "_geoDistance(12, -2021):desc",
                ReservedKeyword { name: S("_geoDistance(12, -2021)") },
            ),
        ];

        for (req, expected_error) in invalid_req {
            let res = req.parse::<AscDesc>();
            assert!(
                res.is_err(),
                "Should no be able to parse `{}`, was expecting an error but instead got: `{:?}`",
                req,
                res,
            );
            let res = res.unwrap_err();
            assert_eq!(
                res.to_string(),
                expected_error.to_string(),
                "Bad error for input {}: got `{:?}` instead of `{:?}`",
                req,
                res,
                expected_error
            );
        }
    }
}
