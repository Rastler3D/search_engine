use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::ops::Bound::{self, Excluded, Included};
use std::str::FromStr;

use either::Either;
use super::condition::Condition;
use roaring::RoaringBitmap;
use query_lang::query::ast::{AndOperator, EqOperator, ExistsOperator, FieldOperator, InOperator, IsEmptyOperator, LeafValue, NotOperator, Operator, OrOperator, Predicate, Value};
use query_lang::query::ParseError;

use super::facet_range_search;
use crate::error::{Error, UserError};
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec, OrderedF64Codec,
};
use crate::{distance_between_two_points, lat_lng_to_xyz, FieldId, Index, Result};

/// The maximum number of filters the filter AST can process.
const MAX_FILTER_DEPTH: usize = 2000;

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    condition: Predicate,
}



#[derive(Debug)]
enum FilterError<'a> {
    AttributeNotFilterable { attribute: &'a str, filterable_fields: HashSet<String> },
    TooDeep,
}
impl<'a> std::error::Error for FilterError<'a> {}


impl<'a> Display for FilterError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttributeNotFilterable { attribute, filterable_fields } => {
                if filterable_fields.is_empty() {
                    write!(
                        f,
                        "Attribute `{}` is not filterable. This index does not have configured filterable attributes.",
                        attribute,
                    )
                } else {
                    let filterables_list = filterable_fields
                        .iter()
                        .map(AsRef::as_ref)
                        .collect::<Vec<&str>>()
                        .join(" ");

                    write!(
                        f,
                        "Attribute `{}` is not filterable. Available filterable attributes are: `{}`.",
                        attribute,
                        filterables_list,
                    )
                }
            }
            Self::TooDeep => write!(
                f,
                "Too many filter conditions, can't process more than {} filters.",
                MAX_FILTER_DEPTH
            ),
        }
    }
}

impl<'a> From<ParseError> for Error {
    fn from(error: ParseError) -> Self {
        Self::UserError(UserError::InvalidFilter(error.to_string()))
    }
}

impl From<Filter> for Predicate{
    fn from(f: Filter) -> Self {
        f.condition
    }
}

impl Filter {
    pub fn from_json(filter: &serde_json::Value) -> Result<Option<Self>> {
        match filter {
            serde_json::Value::Object(_) => {
                let condition = Filter::from_str(&filter.to_string())?;
                Ok(condition)
            }
            v => Err(Error::UserError(UserError::InvalidFilterExpression(
                &["String"],
                v.clone(),
            ))),
        }
    }



    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expression: &str) -> Result<Option<Self>> {
        let condition = match Predicate::from_str(expression) {
            Ok(predicate) => Ok(predicate),
            Err(e) => Err(Error::UserError(UserError::InvalidFilter(e.to_string()))),
        }?;

        Ok(Some(Self { condition }))
    }
}

impl Filter {
    pub fn evaluate(&self, rtxn: &heed::RoTxn, index: &Index) -> Result<RoaringBitmap> {
        // to avoid doing this for each recursive call we're going to do it ONCE ahead of time
        let filterable_fields = index.filterable_fields(rtxn)?;

        Self::inner_evaluate(self.condition.clone(), rtxn, index, &filterable_fields, "")
    }

    fn evaluate_condition(
        rtxn: &heed::RoTxn,
        index: &Index,
        field_id: FieldId,
        operator: Condition,
    ) -> Result<RoaringBitmap> {
        let numbers_db = index.facet_id_f64_docids;
        let strings_db = index.facet_id_string_docids;

        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.

        let (left, right) = match operator {
            Condition::GreaterThan(val) => {
                match val {
                    Value::Number(number) => {
                        (Excluded(number.as_f64()), Included(f64::MAX))
                    }
                    _ => {
                        return Err(Error::UserError(UserError::InvalidFilterExpression(&["Number"], val.into())))
                    }
                }

            }
            Condition::GreaterThanOrEqual(val) => {
                match val {
                    Value::Number(number) => {
                        (Included(number.as_f64()), Included(f64::MAX))
                    }
                    _ => {
                        return Err(Error::UserError(UserError::InvalidFilterExpression(&["Number"], val.into())))
                    }
                }

            }
            Condition::LowerThan(val) =>{
                match val {
                    Value::Number(number) => {
                        (Included(f64::MIN), Excluded(number.as_f64()))
                    }
                    _ => {
                        return Err(Error::UserError(UserError::InvalidFilterExpression(&["Number"], val.into())))
                    }
                }
            } ,
            Condition::LowerThanOrEqual(val) => {
                match val {
                    Value::Number(number) => {
                        (Included(f64::MIN), Included(number.as_f64()))
                    }
                    _ => {
                        return Err(Error::UserError(UserError::InvalidFilterExpression(&["Number"], val.into())))
                    }
                }
            }
            Condition::Between { from, to } => {
                match (from, to) {
                    (Value::Number(from), Value::Number(to)) => {
                        (Included(from.as_f64()), Included(to.as_f64()))
                    }
                    val => {
                        return Err(Error::UserError(UserError::InvalidFilterExpression(&["Number"], val.0.into())))
                    }
                }

            }
            Condition::Empty => {
                let is_empty = index.empty_faceted_documents_ids(rtxn, field_id)?;
                return Ok(is_empty);
            }
            Condition::Exists => {
                let exist = index.exists_faceted_documents_ids(rtxn, field_id)?;
                return Ok(exist);
            }
            Condition::Equal(val) => {
                return match val {
                    Value::Null => {
                        let is_null = index.null_faceted_documents_ids(rtxn, field_id)?;
                        Ok(is_null)
                    }
                    value @ (Value::Bool(_) | Value::String(_)) => {
                        let val = value.to_string();
                        let string_docids = strings_db
                            .get(
                                rtxn,
                                &FacetGroupKey {
                                    field_id,
                                    level: 0,
                                    left_bound: &crate::normalize_facet(&val),
                                },
                            )?
                            .map(|v| v.bitmap)
                            .unwrap_or_default();

                        Ok(string_docids)
                    },
                    Value::Number(number) => {
                        let number_docids = numbers_db
                            .get(rtxn, &FacetGroupKey { field_id, level: 0, left_bound: number.as_f64() })?
                            .map(|v| v.bitmap)
                            .unwrap_or_default();

                        Ok(number_docids)
                    }

                    val => {
                        Err(Error::UserError(UserError::InvalidFilterExpression(&["Null", "Bool", "Number", "String"], val.into())))
                    }
                }
            }
            Condition::NotEqual(val) => {
                let operator = Condition::Equal(val);
                let docids = Self::evaluate_condition(rtxn, index, field_id, operator)?;
                let all_ids = index.documents_ids(rtxn)?;
                return Ok(all_ids - docids);
            }
        };

        let mut output = RoaringBitmap::new();
        Self::explore_facet_number_levels(rtxn, numbers_db, field_id, left, right, &mut output)?;
        Ok(output)
    }

    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_number_levels(
        rtxn: &heed::RoTxn,
        db: heed::Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
        field_id: FieldId,
        left: Bound<f64>,
        right: Bound<f64>,
        output: &mut RoaringBitmap,
    ) -> Result<()> {
        match (left, right) {
            // lower TO upper when lower > upper must return no result
            (Included(l), Included(r)) if l > r => return Ok(()),
            (Included(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Included(r)) if l >= r => return Ok(()),
            (_, _) => (),
        }
        facet_range_search::find_docids_of_facet_within_bounds::<OrderedF64Codec>(
            rtxn, db, field_id, &left, &right, output,
        )?;

        Ok(())
    }

    fn evaluate_operator(
        operator: Operator,
        rtxn: &heed::RoTxn,
        index: &Index,
        filterable_fields: &HashSet<String>,
        field: &str,
    ) -> Result<RoaringBitmap> {
        match operator {
            Operator::Field(FieldOperator{ field: field_path, predicate }) => {
                let field = if field.is_empty(){
                    field_path.to_string()
                } else {
                    format!("{field}.{field_path}")
                };

                Self::inner_evaluate(predicate, rtxn, index, filterable_fields, &field)
            }
            Operator::Not(NotOperator(predicate)) => {
                let all_ids = index.documents_ids(rtxn)?;
                let selected = Self::inner_evaluate(
                    predicate,
                    rtxn,
                    index,
                    filterable_fields,
                    field
                )?;

                Ok(all_ids - selected)
            }
            Operator::In(InOperator(values)) => {
                if crate::is_faceted(field, filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;

                    let mut bitmap = RoaringBitmap::new();
                    if let Some(fid) = field_ids_map.id(field) {
                        for value in values {
                            let op = Condition::Equal(value);
                            let el_bitmap = Self::evaluate_condition(rtxn, index, fid, op)?;
                            bitmap |= el_bitmap;
                        }
                    }
                    Ok(bitmap)
                } else {
                    Err(Error::UserError(UserError::InvalidFilter(FilterError::AttributeNotFilterable {
                        attribute: field,
                        filterable_fields: filterable_fields.clone(),
                    }.to_string())))
                }
            }

            Operator::Or(OrOperator(predicates)) => {
                let mut bitmap = RoaringBitmap::new();
                for predicate in predicates {
                    bitmap |=
                        Self::inner_evaluate(predicate, rtxn, index, filterable_fields, field)?;
                }
                Ok(bitmap)
            }
            Operator::And(AndOperator(predicates)) => {
                let mut predicates = predicates.into_iter();
                if let Some(first_predicate) = predicates.next() {
                    let mut bitmap = Self::inner_evaluate(
                        first_predicate,
                        rtxn,
                        index,
                        filterable_fields,
                        field
                    )?;
                    for predicate in predicates {
                        if bitmap.is_empty() {
                            return Ok(bitmap);
                        }
                        bitmap &= Self::inner_evaluate(
                            predicate,
                            rtxn,
                            index,
                            filterable_fields,
                            field
                        )?;
                    }
                    Ok(bitmap)
                } else {
                    Ok(RoaringBitmap::new())
                }
            },
            operator @ (Operator::Exists(_) | Operator::IsEmpty(_)) => {
                let (&Operator::Exists(ExistsOperator(value)) | &Operator::IsEmpty(IsEmptyOperator(value))) = &operator else {
                    unreachable!()
                } ;
                if crate::is_faceted(field, filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;
                    let docids = if let Some(fid) = field_ids_map.id(field) {
                        Self::evaluate_condition(rtxn, index, fid, Condition::from(operator))?
                    } else {
                        RoaringBitmap::new()
                    };
                    if value {
                        Ok(docids)
                    } else {
                        Ok(index.documents_ids(rtxn)? - docids)
                    }
                } else {
                    Err(Error::UserError(UserError::InvalidFilter(FilterError::AttributeNotFilterable {
                        attribute: field,
                        filterable_fields: filterable_fields.clone(),
                    }.to_string())))
                }
            }
            operator => {
                if crate::is_faceted(field, filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;
                    if let Some(fid) = field_ids_map.id(field) {
                        Self::evaluate_condition(rtxn, index, fid, Condition::from(operator))
                    } else {
                        Ok(RoaringBitmap::new())
                    }
                } else {
                    Err(Error::UserError(UserError::InvalidFilter(FilterError::AttributeNotFilterable {
                        attribute: field,
                        filterable_fields: filterable_fields.clone(),
                    }.to_string())))
                }
            }
        }
    }

    fn inner_evaluate(
        predicate: Predicate,
        rtxn: &heed::RoTxn,
        index: &Index,
        filterable_fields: &HashSet<String>,
        field: &str,
    ) -> Result<RoaringBitmap> {
        match predicate {
            Predicate::Leaf(LeafValue(value)) => {
                Self::evaluate_operator(
                    Operator::Eq(EqOperator(value)),
                    rtxn,
                    index,
                    filterable_fields,
                    field
                )
            }
            Predicate::Operators(operators) => {
                let mut operators = operators.into_iter();
                if let Some(first_operator) = operators.next() {
                    let mut bitmap = Self::evaluate_operator(
                        first_operator,
                        rtxn,
                        index,
                        filterable_fields,
                        field
                    )?;
                    for operator in operators {
                        if bitmap.is_empty() {
                            return Ok(bitmap);
                        }
                        bitmap &= Self::evaluate_operator(
                            operator,
                            rtxn,
                            index,
                            filterable_fields,
                            field
                        )?;
                    }
                    Ok(bitmap)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
        }
    }
}

impl From<Predicate> for Filter {
    fn from(predicate: Predicate) -> Self {
        Self { condition: predicate }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::fmt::Write;
//     use std::iter::FromIterator;
//
//     use big_s::S;
//     use either::Either;
//     use maplit::hashset;
//     use roaring::RoaringBitmap;
//
//     use crate::index::tests::TempIndex;
//     use crate::search::facet::Filter;
//
//     #[test]
//     fn empty_db() {
//         let index = TempIndex::new();
//         //Set the filterable fields to be the channel.
//         index
//             .update_settings(|settings| {
//                 settings.set_filterable_fields(hashset! { S("PrIcE") });
//             })
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         let filter = Filter::from_str("PrIcE < 1000").unwrap().unwrap();
//         let bitmap = filter.evaluate(&rtxn, &index).unwrap();
//         assert!(bitmap.is_empty());
//
//         let filter = Filter::from_str("NOT PrIcE >= 1000").unwrap().unwrap();
//         let bitmap = filter.evaluate(&rtxn, &index).unwrap();
//         assert!(bitmap.is_empty());
//     }
//
//
//     #[test]
//     fn not_filterable() {
//         let index = TempIndex::new();
//
//         let rtxn = index.read_txn().unwrap();
//         let filter = Filter::from_str("_geoRadius(42, 150, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `_geo` is not filterable. This index does not have configured filterable attributes."
//         ));
//
//         let filter = Filter::from_str("_geoBoundingBox([42, 150], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `_geo` is not filterable. This index does not have configured filterable attributes."
//         ));
//
//         let filter = Filter::from_str("dog = \"bernese mountain\"").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `dog` is not filterable. This index does not have configured filterable attributes."
//         ));
//         drop(rtxn);
//
//         index
//             .update_settings(|settings| {
//                 settings.set_searchable_fields(vec![S("title")]);
//                 settings.set_filterable_fields(hashset! { S("title") });
//             })
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         let filter = Filter::from_str("_geoRadius(-100, 150, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `_geo` is not filterable. Available filterable attributes are: `title`."
//         ));
//
//         let filter = Filter::from_str("_geoBoundingBox([42, 150], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `_geo` is not filterable. Available filterable attributes are: `title`."
//         ));
//
//         let filter = Filter::from_str("name = 12").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().starts_with(
//             "Attribute `name` is not filterable. Available filterable attributes are: `title`."
//         ));
//     }
//
//     #[test]
//     fn escaped_quote_in_filter_value_2380() {
//         let index = TempIndex::new();
//
//         index
//             .add_documents(documents!([
//                 {
//                     "id": "test_1",
//                     "monitor_diagonal": "27' to 30'"
//                 },
//                 {
//                     "id": "test_2",
//                     "monitor_diagonal": "27\" to 30\""
//                 },
//                 {
//                     "id": "test_3",
//                     "monitor_diagonal": "27\" to 30'"
//                 },
//             ]))
//             .unwrap();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_filterable_fields(hashset!(S("monitor_diagonal")));
//             })
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         let mut search = crate::Search::new(&rtxn, &index);
//         // this filter is copy pasted from #2380 with the exact same espace sequence
//         search.filter(Filter::from_str("monitor_diagonal = '27\" to 30\\''").unwrap().unwrap());
//         let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
//         assert_eq!(documents_ids, vec![2]);
//
//         search.filter(Filter::from_str(r#"monitor_diagonal = "27' to 30'" "#).unwrap().unwrap());
//         let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
//         assert_eq!(documents_ids, vec![0]);
//
//         search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30\"" "#).unwrap().unwrap());
//         let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
//         assert_eq!(documents_ids, vec![1]);
//
//         search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30'" "#).unwrap().unwrap());
//         let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
//         assert_eq!(documents_ids, vec![2]);
//     }
//
//     #[test]
//     fn zero_radius() {
//         let index = TempIndex::new();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_filterable_fields(hashset! { S("_geo") });
//             })
//             .unwrap();
//
//         index
//             .add_documents(documents!([
//               {
//                 "id": 1,
//                 "name": "Nàpiz' Milano",
//                 "address": "Viale Vittorio Veneto, 30, 20124, Milan, Italy",
//                 "type": "pizza",
//                 "rating": 9,
//                 "_geo": {
//                   "lat": 45.4777599,
//                   "lng": 9.1967508
//                 }
//               },
//               {
//                 "id": 2,
//                 "name": "Artico Gelateria Tradizionale",
//                 "address": "Via Dogana, 1, 20123 Milan, Italy",
//                 "type": "ice cream",
//                 "rating": 10,
//                 "_geo": {
//                   "lat": 45.4632046,
//                   "lng": 9.1719421
//                 }
//               },
//             ]))
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         let mut search = crate::Search::new(&rtxn, &index);
//
//         search.filter(Filter::from_str("_geoRadius(45.4777599, 9.1967508, 0)").unwrap().unwrap());
//         let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
//         assert_eq!(documents_ids, vec![0]);
//     }
//
//     #[test]
//     fn geo_radius_error() {
//         let index = TempIndex::new();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
//                 settings.set_filterable_fields(hashset! { S("_geo"), S("price") });
//             })
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         // georadius have a bad latitude
//         let filter = Filter::from_str("_geoRadius(-100, 150, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(
//             error.to_string().starts_with(
//                 "Bad latitude `-100`. Latitude must be contained between -90 and 90 degrees."
//             ),
//             "{}",
//             error.to_string()
//         );
//
//         // georadius have a bad latitude
//         let filter = Filter::from_str("_geoRadius(-90.0000001, 150, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees."
//         ));
//
//         // georadius have a bad longitude
//         let filter = Filter::from_str("_geoRadius(-10, 250, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(
//             error.to_string().contains(
//                 "Bad longitude `250`. Longitude must be contained between -180 and 180 degrees."
//             ),
//             "{}",
//             error.to_string(),
//         );
//
//         // georadius have a bad longitude
//         let filter = Filter::from_str("_geoRadius(-10, 180.000001, 10)").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees."
//         ));
//     }
//
//     #[test]
//     fn geo_bounding_box_error() {
//         let index = TempIndex::new();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
//                 settings.set_filterable_fields(hashset! { S("_geo"), S("price") });
//             })
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//
//         // geoboundingbox top left coord have a bad latitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([-90.0000001, 150], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(
//             error.to_string().starts_with(
//                 "Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees."
//             ),
//             "{}",
//             error.to_string()
//         );
//
//         // geoboundingbox top left coord have a bad latitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([90.0000001, 150], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(
//             error.to_string().starts_with(
//                 "Bad latitude `90.0000001`. Latitude must be contained between -90 and 90 degrees."
//             ),
//             "{}",
//             error.to_string()
//         );
//
//         // geoboundingbox bottom right coord have a bad latitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([30, 10], [-90.0000001, 150])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees."
//         ));
//
//         // geoboundingbox bottom right coord have a bad latitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([30, 10], [90.0000001, 150])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad latitude `90.0000001`. Latitude must be contained between -90 and 90 degrees."
//         ));
//
//         // geoboundingbox top left coord have a bad longitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([-10, 180.000001], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees."
//         ));
//
//         // geoboundingbox top left coord have a bad longitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([-10, -180.000001], [30, 10])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad longitude `-180.000001`. Longitude must be contained between -180 and 180 degrees."
//         ));
//
//         // geoboundingbox bottom right coord have a bad longitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([30, 10], [-10, -180.000001])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad longitude `-180.000001`. Longitude must be contained between -180 and 180 degrees."
//         ));
//
//         // geoboundingbox bottom right coord have a bad longitude
//         let filter =
//             Filter::from_str("_geoBoundingBox([30, 10], [-10, 180.000001])").unwrap().unwrap();
//         let error = filter.evaluate(&rtxn, &index).unwrap_err();
//         assert!(error.to_string().contains(
//             "Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees."
//         ));
//     }
//
//     #[test]
//     fn filter_depth() {
//         // generates a big (2 MiB) filter with too much of ORs.
//         let tipic_filter = "account_ids=14361 OR ";
//         let mut filter_string = String::with_capacity(tipic_filter.len() * 14360);
//         for i in 1..=14361 {
//             let _ = write!(&mut filter_string, "account_ids={}", i);
//             if i != 14361 {
//                 let _ = write!(&mut filter_string, " OR ");
//             }
//         }
//
//         // Note: the filter used to be rejected for being too deep, but that is
//         // no longer the case
//         let filter = Filter::from_str(&filter_string).unwrap();
//         assert!(filter.is_some());
//     }
//
//     #[test]
//     fn empty_filter() {
//         let option = Filter::from_str("     ").unwrap();
//         assert_eq!(option, None);
//     }
//
//     #[test]
//     fn non_finite_float() {
//         let index = TempIndex::new();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_searchable_fields(vec![S("price")]); // to keep the fields order
//                 settings.set_filterable_fields(hashset! { S("price") });
//             })
//             .unwrap();
//         index
//             .add_documents(documents!([
//                 {
//                     "id": "test_1",
//                     "price": "inf"
//                 },
//                 {
//                     "id": "test_2",
//                     "price": "2000"
//                 },
//                 {
//                     "id": "test_3",
//                     "price": "infinity"
//                 },
//             ]))
//             .unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//         let filter = Filter::from_str("price = inf").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert!(result.contains(0));
//         let filter = Filter::from_str("price < inf").unwrap().unwrap();
//         assert!(matches!(
//             filter.evaluate(&rtxn, &index),
//             Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
//         ));
//
//         let filter = Filter::from_str("price = NaN").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert!(result.is_empty());
//         let filter = Filter::from_str("price < NaN").unwrap().unwrap();
//         assert!(matches!(
//             filter.evaluate(&rtxn, &index),
//             Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
//         ));
//
//         let filter = Filter::from_str("price = infinity").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert!(result.contains(2));
//         let filter = Filter::from_str("price < infinity").unwrap().unwrap();
//         assert!(matches!(
//             filter.evaluate(&rtxn, &index),
//             Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
//         ));
//     }
//
//     #[test]
//     fn filter_number() {
//         let index = TempIndex::new();
//
//         index
//             .update_settings(|settings| {
//                 settings.set_primary_key("id".to_owned());
//                 settings.set_filterable_fields(hashset! { S("id"), S("one"), S("two") });
//             })
//             .unwrap();
//
//         let mut docs = vec![];
//         for i in 0..100 {
//             docs.push(serde_json::json!({ "id": i, "two": i % 10 }));
//         }
//
//         index.add_documents(documents!(docs)).unwrap();
//
//         let rtxn = index.read_txn().unwrap();
//         for i in 0..100 {
//             let filter_str = format!("id = {i}");
//             let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//             let result = filter.evaluate(&rtxn, &index).unwrap();
//             assert_eq!(result, RoaringBitmap::from_iter([i]));
//         }
//         for i in 0..100 {
//             let filter_str = format!("id > {i}");
//             let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//             let result = filter.evaluate(&rtxn, &index).unwrap();
//             assert_eq!(result, RoaringBitmap::from_iter((i + 1)..100));
//         }
//         for i in 0..100 {
//             let filter_str = format!("id < {i}");
//             let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//             let result = filter.evaluate(&rtxn, &index).unwrap();
//             assert_eq!(result, RoaringBitmap::from_iter(0..i));
//         }
//         for i in 0..100 {
//             let filter_str = format!("id <= {i}");
//             let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//             let result = filter.evaluate(&rtxn, &index).unwrap();
//             assert_eq!(result, RoaringBitmap::from_iter(0..=i));
//         }
//         for i in 0..100 {
//             let filter_str = format!("id >= {i}");
//             let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//             let result = filter.evaluate(&rtxn, &index).unwrap();
//             assert_eq!(result, RoaringBitmap::from_iter(i..100));
//         }
//         for i in 0..100 {
//             for j in i..100 {
//                 let filter_str = format!("id {i} TO {j}");
//                 let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//                 let result = filter.evaluate(&rtxn, &index).unwrap();
//                 assert_eq!(result, RoaringBitmap::from_iter(i..=j));
//             }
//         }
//         let filter = Filter::from_str("one >= 0 OR one <= 0").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert_eq!(result, RoaringBitmap::default());
//
//         let filter = Filter::from_str("one = 0").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert_eq!(result, RoaringBitmap::default());
//
//         for i in 0..10 {
//             for j in i..10 {
//                 let filter_str = format!("two {i} TO {j}");
//                 let filter = Filter::from_str(&filter_str).unwrap().unwrap();
//                 let result = filter.evaluate(&rtxn, &index).unwrap();
//                 assert_eq!(
//                     result,
//                     RoaringBitmap::from_iter((0..100).filter(|x| (i..=j).contains(&(x % 10))))
//                 );
//             }
//         }
//         let filter = Filter::from_str("two != 0").unwrap().unwrap();
//         let result = filter.evaluate(&rtxn, &index).unwrap();
//         assert_eq!(result, RoaringBitmap::from_iter((0..100).filter(|x| x % 10 != 0)));
//     }
// }
