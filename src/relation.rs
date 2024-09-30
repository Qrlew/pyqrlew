use crate::{
    dataset::Dataset,
    dialect::Dialect,
    dp_event::RelationWithDpEvent,
    error::{MissingKeyError, Result},
};
use pyo3::prelude::*;
use qrlew::{
    ast,
    data_type::DataTyped,
    dialect_translation::{
        bigquery::BigQueryTranslator,
        mssql::MsSqlTranslator,
        postgresql::PostgreSqlTranslator,
        mysql::MySqlTranslator,
        hive::HiveTranslator,
        databricks::DatabricksTranslator,
        redshiftsql::RedshiftSqlTranslator,
        RelationWithTranslator,
    },
    differential_privacy::DpParameters,
    expr::Identifier,
    hierarchy::Hierarchy,
    privacy_unit_tracking::{self, PrivacyUnit},
    relation::{self, Variant},
    sql::parse_expr,
    synthetic_data::SyntheticData,
};
use qrlew_sarus::protobuf::{print_to_string, type_};
use std::{collections::HashMap, ops::Deref, str, sync::Arc};

/// A Relation is a Dataset transformed by a SQL query
#[pyclass(name = "_Relation")]
#[derive(Clone)]
pub struct Relation(Arc<relation::Relation>);

impl Deref for Relation {
    type Target = relation::Relation;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Relation {
    pub fn new(relation: Arc<relation::Relation>) -> Self {
        Relation(relation)
    }
}

#[derive(FromPyObject, Clone)]
pub enum PrivacyUnitType<'a> {
    Type1(Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>),
    Type2(
        (
            Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>,
            bool,
        ),
    ),
    Type3(
        (
            Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str, &'a str)>,
            bool,
        ),
    ),
}

impl<'a> From<PrivacyUnitType<'a>> for PrivacyUnit {
    fn from(input: PrivacyUnitType<'a>) -> Self {
        match input {
            PrivacyUnitType::Type1(data) => PrivacyUnit::from(data),
            PrivacyUnitType::Type2(data) => PrivacyUnit::from(data),
            PrivacyUnitType::Type3(data) => PrivacyUnit::from(data),
        }
    }
}

/// An Enum for the privacy unit tracking propagation
/// Soft will protect only when it does not affect the meaning of the original query and fail otherwise.
/// Hard will protect at all cost. It will succeed most of the time.
#[pyclass]
#[derive(Clone)]
pub enum Strategy {
    Soft,
    Hard,
}

impl From<Strategy> for privacy_unit_tracking::Strategy {
    fn from(value: Strategy) -> Self {
        match value {
            Strategy::Soft => privacy_unit_tracking::Strategy::Soft,
            Strategy::Hard => privacy_unit_tracking::Strategy::Hard,
        }
    }
}

#[pymethods]
impl Relation {
    #[staticmethod]
    /// Builds a `Relation` from a query and a dataset
    ///
    /// Args:
    ///     query (str): sql query.
    ///     dataset (Dataset): the Dataset.
    ///     dialect (Optional[Dialect]): query's dialect. If not provided, it is assumed to be PostgreSql
    ///
    /// Returns:
    ///     Relation:
    pub fn from_query(query: &str, dataset: &Dataset, dialect: Option<Dialect>) -> Result<Self> {
        dataset.relation(query, dialect)
    }

    /// String representation of the `Relation` in the default dialect
    pub fn __str__(&self) -> String {
        let relation = self.0.as_ref();
        let query =
            ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator)).to_string();
        format!("{}", query)
    }

    /// GraphViz representation of the `Relation`
    pub fn dot(&self) -> Result<String> {
        let mut out: Vec<u8> = vec![];
        self.0.as_ref().dot(&mut out, &[]).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    /// Returns a string representation of the Relation's schema.
    ///
    /// Returns:
    ///     str:
    pub fn schema(&self) -> String {
        self.0.schema().to_string()
    }

    /// Returns a protobuf compatible string representation of the Relation's schema data type.
    ///
    /// Returns:
    ///     str:
    pub fn type_(&self) -> Result<String> {
        let data_type = self.0.schema().data_type();
        let struct_type_proto: type_::Type = (&data_type).try_into()?;
        Ok(print_to_string(&struct_type_proto)?)
    }
    /// Returns as RelationWithDpEvent where it's relation propagates the privacy unit
    /// through the query.
    ///
    /// Args:
    ///     dataset (Dataset):
    ///         Dataset with needed relations
    ///     privacy_unit (Sequence[Tuple[str, Sequence[Tuple[str, str, str]], str]]):
    ///         privacy unit to be propagated.
    ///         example to better understand the structure of privacy_unit
    ///     epsilon_delta (Mapping[str, float]): epsilon and delta budget
    ///     max_multiplicity (Optional[float]): maximum number of rows per privacy unit in absolute terms
    ///     max_multiplicity_share (Optional[float]): maximum number of rows per privacy unit in relative terms
    ///         w.r.t. the dataset size. The actual max_multiplicity used to bound the PU contribution will be
    ///         minimum(max_multiplicity, max_multiplicity_share*dataset.size).
    ///     synthetic_data (Optional[Sequence[Tuple[Sequence[str],Sequence[str]]]]): Sequence of pairs
    ///         of original table path and its corresponding synthetic version. Each table must be specified.
    ///         (e.g.: (["retail_schema", "features"], ["retail_schema", "features_synthetic"])).
    ///
    /// Returns:
    ///     RelationWithDpEvent:
    ///
    pub fn rewrite_as_privacy_unit_preserving<'a>(
        &'a self,
        dataset: &'a Dataset,
        privacy_unit: PrivacyUnitType<'a>,
        epsilon_delta: HashMap<&'a str, f64>,
        max_multiplicity: Option<f64>,
        max_multiplicity_share: Option<f64>,
        synthetic_data: Option<Vec<(Vec<&'a str>, Vec<&'a str>)>>,
        strategy: Option<Strategy>,
    ) -> Result<RelationWithDpEvent> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = synthetic_data.map(|sd| {
            SyntheticData::new(
                sd.into_iter()
                    .map(|(path, iden)| {
                        let iden_as_vec_of_strings: Vec<String> =
                            iden.iter().map(|s| s.to_string()).collect();
                        (path, Identifier::from(iden_as_vec_of_strings))
                    })
                    .collect(),
            )
        });
        let privacy_unit = PrivacyUnit::from(privacy_unit);
        let epsilon = epsilon_delta
            .get("epsilon")
            .ok_or(MissingKeyError("epsilon".to_string()))?;
        let delta = epsilon_delta
            .get("delta")
            .ok_or(MissingKeyError("delta".to_string()))?;
        let dp_parameters = {
            let mut dp_parameters = DpParameters::from_epsilon_delta(*epsilon, *delta);
            if let Some(max_multiplicity) = max_multiplicity {
                dp_parameters = dp_parameters.with_privacy_unit_max_multiplicity(max_multiplicity);
            }
            if let Some(max_multiplicity_share) = max_multiplicity_share {
                dp_parameters =
                    dp_parameters.with_privacy_unit_max_multiplicity_share(max_multiplicity_share);
            }
            dp_parameters
        };
        let relation_with_dp_event = relation.rewrite_as_privacy_unit_preserving(
            &relations,
            synthetic_data,
            privacy_unit,
            dp_parameters,
            strategy.map(|s| s.into()),
        )?;
        Ok(RelationWithDpEvent::new(Arc::new(relation_with_dp_event)))
    }

    /// It transforms a Relation into its differentially private equivalent.
    ///
    /// Args:
    ///     dataset (Dataset):
    ///         Dataset with needed relations
    ///     privacy_unit (Sequence[Tuple[str, Sequence[Tuple[str, str, str]], str]]):
    ///         privacy unit to be propagated.
    ///         example to better understand the structure of privacy_unit
    ///     epsilon_delta (Mapping[str, float]): epsilon and delta budget
    ///     max_multiplicity (Optional[float]): maximum number of rows per privacy unit in absolute terms
    ///     max_multiplicity_share (Optional[float]): maximum number of rows per privacy unit in relative terms
    ///         w.r.t. the dataset size. The actual max_multiplicity used to bound the PU contribution will be
    ///         minimum(max_multiplicity, max_multiplicity_share*dataset.size).
    ///     synthetic_data (Optional[Sequence[Tuple[Sequence[str],Sequence[str]]]]): Sequence of pairs
    ///         of original table path and its corresponding synthetic version. Each table must be specified.
    ///         (e.g.: (["retail_schema", "features"], ["retail_schema", "features_synthetic"])).
    ///
    /// Returns:
    ///     RelationWithDpEvent:
    ///
    pub fn rewrite_with_differential_privacy<'a>(
        &'a self,
        dataset: &'a Dataset,
        privacy_unit: PrivacyUnitType<'a>,
        epsilon_delta: HashMap<&'a str, f64>,
        max_multiplicity: Option<f64>,
        max_multiplicity_share: Option<f64>,
        synthetic_data: Option<Vec<(Vec<&'a str>, Vec<&'a str>)>>,
    ) -> Result<RelationWithDpEvent> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = synthetic_data.map(|sd| {
            SyntheticData::new(
                sd.into_iter()
                    .map(|(path, iden)| {
                        let iden_as_vec_of_strings: Vec<String> =
                            iden.iter().map(|s| s.to_string()).collect();
                        (path, Identifier::from(iden_as_vec_of_strings))
                    })
                    .collect(),
            )
        });
        let privacy_unit = PrivacyUnit::from(privacy_unit);
        let epsilon = epsilon_delta
            .get("epsilon")
            .ok_or(MissingKeyError("epsion".to_string()))?;
        let delta = epsilon_delta
            .get("delta")
            .ok_or(MissingKeyError("delta".to_string()))?;
        let dp_parameters = {
            let mut dp_parameters = DpParameters::from_epsilon_delta(*epsilon, *delta);
            if let Some(max_multiplicity) = max_multiplicity {
                dp_parameters = dp_parameters.with_privacy_unit_max_multiplicity(max_multiplicity);
            }
            if let Some(max_multiplicity_share) = max_multiplicity_share {
                dp_parameters =
                    dp_parameters.with_privacy_unit_max_multiplicity_share(max_multiplicity_share);
            }
            dp_parameters
        };
        let relation_with_dp_event = relation.rewrite_with_differential_privacy(
            &relations,
            synthetic_data,
            privacy_unit,
            dp_parameters,
        )?;
        Ok(RelationWithDpEvent::new(Arc::new(relation_with_dp_event)))
    }

    /// Returns an SQL representation of the Relation.
    ///
    /// Args:
    ///     dialect (Optional[Dialect]): dialect of generated sql query. If no dialect is provided,
    ///         the query will be in PostgreSql.
    ///
    /// Returns:
    ///     str:
    pub fn to_query(&self, dialect: Option<Dialect>) -> String {
        let relation = &*(self.0);
        let dialect = dialect.unwrap_or(Dialect::PostgreSql);
        match dialect {
            Dialect::PostgreSql => {
                ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator))
                    .to_string()
            }
            Dialect::MsSql => {
                ast::Query::from(RelationWithTranslator(&relation, MsSqlTranslator)).to_string()
            }
            Dialect::BigQuery => {
                ast::Query::from(RelationWithTranslator(&relation, BigQueryTranslator)).to_string()
            }
            Dialect::MySql => {
                ast::Query::from(RelationWithTranslator(&relation, MySqlTranslator)).to_string()
            }
            Dialect::Hive => {
                ast::Query::from(RelationWithTranslator(&relation, HiveTranslator)).to_string()
            }
            Dialect::Databricks => {
                ast::Query::from(RelationWithTranslator(&relation, DatabricksTranslator)).to_string()
            }
            Dialect::RedshiftSql => {
                ast::Query::from(RelationWithTranslator(&relation, RedshiftSqlTranslator)).to_string()
            }
        }
    }

    pub fn rename_fields(&self, fields: Vec<(&str, &str)>) -> Result<Self> {
        let fields_mapping: HashMap<&str, &str> = fields.into_iter().collect();
        let relation = self.deref().clone();
        Ok(Relation::new(Arc::new(relation.rename_fields(|n, _| {
            fields_mapping.get(n).cloned().unwrap_or(n).to_string()
        }))))
    }

    pub fn compose(&self, relations: Vec<(Vec<String>, Relation)>) -> Result<Self> {
        let outer_relations = self.deref();
        let inner_relations: Hierarchy<Arc<qrlew::Relation>> = relations
            .into_iter()
            .map(|(path, rel)| (Identifier::from(path), rel.0))
            .collect();
        let composed = outer_relations.compose(&inner_relations);
        Ok(Relation::new(Arc::new(composed)))
    }

    pub fn with_field(&self, name: &str, expr: &str) -> Result<Self> {
        let expr = parse_expr(expr)?;
        Ok(Relation::new(Arc::new(
            self.deref().clone().with_field(name, (&expr).try_into()?),
        )))
    }
}

#[cfg(test)]
mod tests {

    use qrlew::{
        ast,
        dialect_translation::{postgresql::PostgreSqlTranslator, RelationWithTranslator},
    };

    use crate::{
        dataset::Dataset,
        relation::{PrivacyUnitType, Relation},
    };
    use std::collections::HashMap;

    const DATASET: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "e9cb9391ca184e89897f49bd75387a46", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
    const SCHEMA: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "5321f24ffb324a9e958c77ceb09b6cc8", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract", "type": {"name": "extract", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "extract", "type": {"name": "Union", "union": {"fields": [{"name": "beacon", "type": {"name": "Struct", "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "type": {"name": "Struct", "struct": {"fields": [{"name": "age", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_weights", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807", "base": "INT64", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Id", "id": {"base": "STRING", "unique": false}, "properties": {}}}]}, "properties": {}}, "protected": {"label": "data", "paths": [], "properties": {}}, "properties": {"max_max_multiplicity": "1", "foreign_keys": "", "primary_keys": ""}}"#;
    const SIZE: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Size", "uuid": "cd8ec3f7958e4b2c842bc66ffa55e40c", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract_sizes", "statistics": {"name": "Union", "union": {"fields": [{"name": "extract", "statistics": {"name": "Union", "union": {"fields": [{"name": "beacon", "statistics": {"name": "Struct", "size": "100", "multiplicity": 1.0, "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "statistics": {"name": "Datetime", "size": "100", "multiplicity": 1.0, "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "statistics": {"name": "Struct", "size": "199", "multiplicity": 1.0, "struct": {"fields": [{"name": "age", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {}}, "properties": {}}]}, "properties": {}}, "properties": {}}"#;

    // const DATASET: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "32a28b78e29543729f9aa6530b7fe9ef", "name": "Transformed", "spec": {"transformed": {"transform": "8feff2ec007340348704d908772ef5ff", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
    // const SCHEMA: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "9e7813d17b5644bca143b65f3d118c24", "dataset": "32a28b78e29543729f9aa6530b7fe9ef", "name": "Transformed_schema", "type": {"name": "transformed_schema", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "st51_cwyuajpk", "type": {"name": "Union", "union": {"fields": [{"name": "vaymkrql", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "oasezpii", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "float", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "date", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "public_fk", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "rsawqlwy", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "float", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "date", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "public_fk", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "agjtmkwt", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "min": null, "max": null, "possible_values": []}, "properties": {}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"base": "FLOAT64", "min": "-1125899906842624", "max": "1125899906842624", "possible_values": []}, "properties": {}}}]}, "properties": {}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_weights", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807", "base": "INT64", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_protected_entity", "type": {"name": "Id", "id": {"base": "STRING", "unique": false}, "properties": {}}}]}, "properties": {}}, "protected": {"label": "data", "paths": [], "properties": {}}, "properties": {"max_max_multiplicity": "1", "foreign_keys": "", "primary_keys": ""}}"#;

    #[test]
    fn test_create_ds() {
        let ds: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "e9cb9391ca184e89897f49bd75387a46", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
        let sch: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "f0e998eb7b904be9bcd656c4157357f6", "dataset": "f31d342bc8284fa2b8f36fbfb869aa3a", "name": "Transformed_schema", "type": {"name": "transformed_schema", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "st51_bicdlwoy", "type": {"name": "Union", "union": {"fields": [{"name": "xwiromvh", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}]}}}, {"name": "qqnhlkqe", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "float", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}}}, {"name": "date", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "public_fk", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}]}}}, {"name": "bgpqlcws", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "float", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}}}, {"name": "date", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "public_fk", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}]}}}, {"name": "tyzgsphn", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"min": -1125899906842624.0, "max": 1125899906842624.0}}}]}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_weights", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_protected_entity", "type": {"name": "Id", "id": {"base": "STRING"}}}]}}, "protected": {"label": "data"}, "properties": {"primary_keys": "", "max_max_multiplicity": "1", "foreign_keys": ""}}"#;
        let size: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Size", "uuid": "27eade642ec54c05a2a757c9846775a0", "dataset": "f31d342bc8284fa2b8f36fbfb869aa3a", "name": "Transformed_schema_sizes", "statistics": {"name": "Union", "union": {"fields": [{"name": "st51_bicdlwoy", "statistics": {"name": "Union", "union": {"fields": [{"name": "xwiromvh", "statistics": {"name": "Struct", "struct": {"fields": [{"name": "id", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "text", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_is_public", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_privacy_unit", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_weights", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "3", "multiplicity": 1.0}}}], "size": "3", "multiplicity": 1.0}}}, {"name": "qqnhlkqe", "statistics": {"name": "Struct", "struct": {"fields": [{"name": "id", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "integer", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "float", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "900", "multiplicity": 1.0}}}, {"name": "datetime", "statistics": {"name": "Datetime", "datetime": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "date", "statistics": {"name": "Datetime", "datetime": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "boolean", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "text", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "public_fk", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_is_public", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_privacy_unit", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_weights", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "900", "multiplicity": 1.0}}}], "size": "900", "multiplicity": 1.0}}}, {"name": "bgpqlcws", "statistics": {"name": "Struct", "struct": {"fields": [{"name": "id", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "integer", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "float", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "900", "multiplicity": 1.0}}}, {"name": "datetime", "statistics": {"name": "Datetime", "datetime": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "date", "statistics": {"name": "Datetime", "datetime": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "boolean", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "text", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "public_fk", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_is_public", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_privacy_unit", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "900", "multiplicity": 1.0}}}, {"name": "sarus_weights", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "900", "multiplicity": 1.0}}}], "size": "900", "multiplicity": 1.0}}}, {"name": "tyzgsphn", "statistics": {"name": "Struct", "struct": {"fields": [{"name": "id", "statistics": {"name": "Integer", "integer": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "text", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_is_public", "statistics": {"name": "Boolean", "boolean": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_privacy_unit", "statistics": {"name": "Text", "text": {"distribution": {"integer": {"min": "-9223372036854775808", "max": "9223372036854775807"}}, "size": "3", "multiplicity": 1.0}}}, {"name": "sarus_weights", "statistics": {"name": "Float", "float": {"distribution": {"double": {"min": -1125899906842624.0, "max": 1125899906842624.0}}, "size": "3", "multiplicity": 1.0}}}], "size": "3", "multiplicity": 1.0}}}]}}}]}}}"#;

        let dataset = Dataset::new(ds, sch, size).unwrap();
        for (path, rel) in dataset.relations() {
            println!("{:?}", path);
        }
    }

    #[test]
    fn test_rewrite_with_differential_privacy() {
        let dataset = Dataset::new(DATASET, SCHEMA, SIZE).unwrap();
        println!("{:?}", dataset.relations()[1].0);

        let synthetic_data = Some(vec![(
            vec!["extract", "census"],
            vec!["extract", "census_sd"],
        )]);
        let privacy_unit = PrivacyUnitType::Type1(vec![(
            "census",
            Vec::<(&str, &str, &str)>::new(),
            "_PRIVACY_UNIT_ROW_",
        )]);
        let budget: HashMap<&str, f64> = [("epsilon", 1.), ("delta", 0.005)]
            .iter()
            .cloned()
            .collect();

        let queries = [
            // "SELECT SUM(CASE WHEN age > 90 THEN 1 ELSE 0 END) AS s1 FROM census WHERE age > 20 AND age < 90;",
            // "SELECT SUM(capital_loss / 100000.) AS my_sum FROM census WHERE capital_loss > 2231. AND capital_loss < 4356.;",
            // "SELECT SUM(capital_loss) FROM census GROUP BY capital_loss",
            "SELECT SUM(DISTINCT capital_loss) AS s1 FROM census WHERE capital_loss > 2231 AND capital_loss < 4356 GROUP BY sex HAVING COUNT(*) > 5;",
        ];

        for query in queries {
            let relation = Relation::from_query(query, &dataset, None).unwrap();
            println!("{}", relation.0);
            let dp_relation = relation
                .rewrite_with_differential_privacy(
                    &dataset,
                    privacy_unit.clone(),
                    budget.clone(),
                    None,
                    None,
                    synthetic_data.clone(),
                )
                .unwrap();
            let dp_query = dp_relation.relation().to_query(None);
            println!("\n\n{dp_query}");
        }

        // No synthetic data
        for query in queries {
            let relation = Relation::from_query(query, &dataset, None).unwrap();
            println!("{}", relation.0);
            let dp_relation = relation
                .rewrite_with_differential_privacy(
                    &dataset,
                    privacy_unit.clone(),
                    budget.clone(),
                    None,
                    None,
                    None,
                )
                .unwrap();
            let dp_query = dp_relation.relation().to_query(None);
            println!("\n\n{dp_query}");
        }
    }

    #[test]
    fn test_quoting() {
        let dataset = Dataset::new(DATASET, SCHEMA, SIZE).unwrap();
        println!("{:?}", dataset.relations()[1].0);

        let query = r#"SELECT "age" AS s1 FROM census;"#;
        let relation = Relation::from_query(query, &dataset, None).unwrap();

        let trans =
            ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator)).to_string();
        println!("{}", trans);
    }

    #[test]
    fn test_from_query() {
        let schema = r#"{"uuid": "1bbca24f17254f8e973c8da75acaf9c1", "dataset": "81dc19763aa9496994e34e594344f286", "name": "extract", "type": {"name": "extract", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "extract", "type": {"name": "Union", "union": {"fields": [{"name": "beacon", "type": {"name": "Struct", "struct": {"fields": [{"name": "検知日時", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "2021-07-05 06:52:33", "max": "2021-07-05 07:50:37"}}}, {"name": "UserId", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["15e312ba59601abc91fdb52a70d29895", "3223c59750514585158af4186fc3c095", "50a09785c81cc5b14261f4342fd6fe8e", "66413034a0d2e6f4f1597e0a5c74caa0", "cc952708879cdb9336eef72e6e97da6b", "d76f3a8a7706a25f986c0c838c70c332", "dda5e65a9d33778f65f226b51cea4b60", "e90fd3187c30793b2d21dbff7572603f", "fdfec2f921ca5ae92e36e6d16c4bb881", "fe2859394ac81a68d486925dc6343e49"]}}}, {"name": "所属部署", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["3階", "4階"]}}}, {"name": "フロア名", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["１階", "３階", "４階"]}}}, {"name": "Beacon名", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["3階執務1", "3階執務3", "3階執務4", "3階執務9", "4階執務1", "4階執務2", "まちづくり執務1", "まちづくり執務2", "まちづくり執務3", "喫煙ルーム（社内）", "営本打ち合わせスペース1", "営本打ち合わせスペース2", "営本打ち合わせスペース3"]}}}, {"name": "RSSI", "type": {"name": "Integer", "integer": {"min": "70", "max": "95", "possibleValues": ["70", "72", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "94", "95"]}}}, {"name": "マップのX座標", "type": {"name": "Integer", "integer": {"min": "74", "max": "645", "possibleValues": ["150", "201", "253", "486", "504", "506", "526", "527", "577", "627", "645", "74", "81"]}}}, {"name": "マップのY座標", "type": {"name": "Integer", "integer": {"min": "70", "max": "290", "possibleValues": ["205", "225", "226", "228", "229", "231", "287", "290", "70", "75", "77"]}}}]}}}, {"name": "census", "type": {"name": "Struct", "struct": {"fields": [{"name": "age", "type": {"name": "Integer", "integer": {"min": "20", "max": "90"}}}, {"name": "workclass", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["?", "Federal-gov", "Local-gov", "Private", "Self-emp-inc", "Self-emp-not-inc", "State-gov"]}}}, {"name": "fnlwgt", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}}}, {"name": "education", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["10th", "11th", "12th", "1st-4th", "5th-6th", "7th-8th", "9th", "Assoc-acdm", "Assoc-voc", "Bachelors", "Doctorate", "HS-grad", "Masters", "Prof-school", "Some-college"]}}}, {"name": "education_num", "type": {"name": "Integer", "integer": {"min": "2", "max": "16", "possibleValues": ["10", "11", "12", "13", "14", "15", "16", "2", "3", "4", "5", "6", "7", "8", "9"]}}}, {"name": "marital_status", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["Divorced", "Married-civ-spouse", "Married-spouse-absent", "Never-married", "Separated", "Widowed", "None"]}}}, {"name": "occupation", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["?", "Adm-clerical", "Craft-repair", "Exec-managerial", "Farming-fishing", "Handlers-cleaners", "Machine-op-inspct", "Other-service", "Prof-specialty", "Protective-serv", "Sales", "Tech-support", "Transport-moving"]}}}, {"name": "relationship", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["Husband", "Not-in-family", "Other-relative", "Own-child", "Unmarried", "Wife"]}}}, {"name": "race", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["Asian-Pac-Islander", "Black", "White"]}}}, {"name": "sex", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["Female", "Male"]}}}, {"name": "capital_gain", "type": {"name": "Integer", "integer": {"possibleValues": ["0"]}}}, {"name": "capital_loss", "type": {"name": "Integer", "integer": {"min": "2231", "max": "4356"}}}, {"name": "hours_per_week", "type": {"name": "Integer", "integer": {"min": "6", "max": "99"}}}, {"name": "native_country", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["?", "Canada", "China", "Greece", "India", "Mexico", "Philippines", "South", "Taiwan", "Trinadad&Tobago", "United-States", "Vietnam"]}}}, {"name": "income", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8", "possibleValues": ["<=50K", ">50K"]}}}]}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Optional", "optional": {"type": {"name": "Id", "id": {"base": "STRING"}}}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"max": 1.7976931348623157e308}}}]}}, "properties": {"foreign_keys": "", "primary_keys": "", "max_max_multiplicity": "1"}}"#;
        let ds: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "e9cb9391ca184e89897f49bd75387a46", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
        let size = "";
        let dataset = Dataset::new(ds, schema, size).unwrap();

        let queries = vec![
            (
                vec![
                    "dataset_name".to_string(),
                    "my_schema".to_string(),
                    "boomers".to_string(),
                ],
                "SELECT * FROM extract.census WHERE age >= 60".to_string(),
            ),
            (
                vec![
                    "dataset_name".to_string(),
                    "my_schema".to_string(),
                    "genx".to_string(),
                ],
                "SELECT * FROM extract.census WHERE age >= 40 AND age < 60".to_string(),
            ),
            (
                vec![
                    "dataset_name".to_string(),
                    "my_schema".to_string(),
                    "millenials".to_string(),
                ],
                "SELECT * FROM extract.census WHERE age >= 30 AND age < 40".to_string(),
            ),
            (
                vec![
                    "dataset_name".to_string(),
                    "my_schema".to_string(),
                    "genz".to_string(),
                ],
                "SELECT * FROM extract.census WHERE age < 30".to_string(),
            ),
        ];
        let new_ds = dataset.from_queries(queries, None).unwrap();

        println!("{:?}", new_ds.schema());
        let rels = new_ds.relations();
    }
}
