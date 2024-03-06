use std::{collections::HashMap, ops::Deref, str, sync::Arc};
use pyo3::prelude::*;
use qrlew::{
    ast,
    differential_privacy::DpParameters,
    expr::Identifier,
    privacy_unit_tracking::PrivacyUnit,
    relation::{self, Variant},
    synthetic_data::SyntheticData,
    dialect_translation::{
        bigquery::BigQueryTranslator, mssql::MsSqlTranslator, postgresql::PostgreSqlTranslator, RelationWithTranslator
    }
};
use crate::{
    dataset::Dataset,
    error::{MissingKeyError, Result},
    dp_event::RelationWithDpEvent,
    dialect::Dialect,
};

/// A Relation is a Dataset transformed by a SQL query
#[pyclass(name="_Relation")]
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
        let query = ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator)).to_string();
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
        (*self.0).schema().to_string()
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
        privacy_unit: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>,
        epsilon_delta: HashMap<&'a str, f64>,
        max_multiplicity: Option<f64>,
        max_multiplicity_share: Option<f64>,
        synthetic_data: Option<Vec<(Vec<&'a str>, Vec<&'a str>)>>,
    ) -> Result<RelationWithDpEvent> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = synthetic_data.map(|sd| SyntheticData::new(sd
            .into_iter()
            .map(|(path, iden)| {
                let iden_as_vec_of_strings: Vec<String> = iden.iter().map(|s| s.to_string()).collect();
                (path, Identifier::from(iden_as_vec_of_strings))
            }).collect())
        );
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
                dp_parameters = dp_parameters.with_privacy_unit_max_multiplicity_share(max_multiplicity_share);
            }
            dp_parameters
        };
        let relation_with_dp_event = relation.rewrite_as_privacy_unit_preserving(
            &relations,
            synthetic_data,
            privacy_unit,
            dp_parameters,
        )?;
        Ok(RelationWithDpEvent::new(Arc::new(
            relation_with_dp_event,
        )))
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
        privacy_unit: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>,
        epsilon_delta: HashMap<&'a str, f64>,
        max_multiplicity: Option<f64>,
        max_multiplicity_share: Option<f64>,
        synthetic_data: Option<Vec<(Vec<&'a str>, Vec<&'a str>)>>,
    ) -> Result<RelationWithDpEvent> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = synthetic_data.map(|sd| SyntheticData::new(sd
                .into_iter()
                .map(|(path, iden)| {
                    let iden_as_vec_of_strings: Vec<String> = iden.iter().map(|s| s.to_string()).collect();
                    (path, Identifier::from(iden_as_vec_of_strings))
                }).collect())
            );
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
                dp_parameters = dp_parameters.with_privacy_unit_max_multiplicity_share(max_multiplicity_share);
            }
            dp_parameters
        };
        let relation_with_dp_event = relation.rewrite_with_differential_privacy(
            &relations,
            synthetic_data,
            privacy_unit,
            dp_parameters,
        )?;
        Ok(RelationWithDpEvent::new(Arc::new(
            relation_with_dp_event,
        )))
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
            Dialect::PostgreSql => ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator)).to_string(),
            Dialect::MsSql => ast::Query::from(RelationWithTranslator(&relation, MsSqlTranslator)).to_string(),
            Dialect::BigQuery => ast::Query::from(RelationWithTranslator(&relation, BigQueryTranslator)).to_string(),
        }
    }
}

#[cfg(test)]
mod tests {

    use qrlew::{ast, dialect_translation::{postgresql::PostgreSqlTranslator, RelationWithTranslator}};

    use crate::{
        dataset::Dataset,
        relation::Relation
    };
    use std::collections::HashMap;

    const DATASET: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "e9cb9391ca184e89897f49bd75387a46", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
    const SCHEMA: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "5321f24ffb324a9e958c77ceb09b6cc8", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract", "type": {"name": "extract", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "extract", "type": {"name": "Union", "union": {"fields": [{"name": "beacon", "type": {"name": "Struct", "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "type": {"name": "Struct", "struct": {"fields": [{"name": "age", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_weights", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807", "base": "INT64", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_protected_entity", "type": {"name": "Id", "id": {"base": "STRING", "unique": false}, "properties": {}}}]}, "properties": {}}, "protected": {"label": "data", "paths": [], "properties": {}}, "properties": {"max_max_multiplicity": "1", "foreign_keys": "", "primary_keys": ""}}"#;
    const SIZE: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Size", "uuid": "cd8ec3f7958e4b2c842bc66ffa55e40c", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract_sizes", "statistics": {"name": "Union", "union": {"fields": [{"name": "extract", "statistics": {"name": "Union", "union": {"fields": [{"name": "beacon", "statistics": {"name": "Struct", "size": "100", "multiplicity": 1.0, "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "statistics": {"name": "Datetime", "size": "100", "multiplicity": 1.0, "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "statistics": {"name": "Struct", "size": "199", "multiplicity": 1.0, "struct": {"fields": [{"name": "age", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {}}, "properties": {}}]}, "properties": {}}, "properties": {}}"#;


    #[test]
    fn test_rewrite_with_differential_privacy() {
        let dataset = Dataset::new(DATASET, SCHEMA, SIZE).unwrap();
        println!("{:?}", dataset.relations()[1].0) ;

        let synthetic_data = Some(vec![(vec!["extract", "census"], vec!["extract", "census"])]);
        let privacy_unit = vec![("census", Vec::<(&str, &str, &str)>::new(), "_PRIVACY_UNIT_ROW_")];
        let budget: HashMap<&str, f64> = [("epsilon", 1.), ("delta", 0.005)].iter().cloned().collect();

        let queries = [
            // "SELECT SUM(CASE WHEN age > 90 THEN 1 ELSE 0 END) AS s1 FROM census WHERE age > 20 AND age < 90;",
            // "SELECT SUM(capital_loss / 100000.) AS my_sum FROM census WHERE capital_loss > 2231. AND capital_loss < 4356.;",
            // "SELECT SUM(capital_loss) FROM census GROUP BY capital_loss",
            "SELECT SUM(DISTINCT capital_loss) AS s1 FROM census WHERE capital_loss > 2231 AND capital_loss < 4356 GROUP BY sex HAVING COUNT(*) > 5;",
        ];

        for query in queries {
            let relation = Relation::from_query(query, &dataset, None).unwrap();
            println!("{}", relation.0);
            let dp_relation = relation.rewrite_with_differential_privacy(
                &dataset, privacy_unit.clone(), budget.clone(), None, None, synthetic_data.clone()
            ).unwrap();
            let dp_query = dp_relation.relation().to_query(None);
            println!("\n\n{dp_query}");
        }

        // No synthetic data
        for query in queries {
            let relation = Relation::from_query(query, &dataset, None).unwrap();
            println!("{}", relation.0);
            let dp_relation = relation.rewrite_with_differential_privacy(
                &dataset, privacy_unit.clone(), budget.clone(), None, None, None
            ).unwrap();
            let dp_query = dp_relation.relation().to_query(None);
            println!("\n\n{dp_query}");
        }
    }

    #[test]
    fn test_quoting() {
        let dataset = Dataset::new(DATASET, SCHEMA, SIZE).unwrap();
        println!("{:?}", dataset.relations()[1].0) ;

        let tr = PostgreSqlTranslator;
        let query = r#"SELECT "age" AS s1 FROM census;"#;
        let relation = Relation::from_query(query, &dataset, None).unwrap();

        let trans = ast::Query::from(RelationWithTranslator(&relation, PostgreSqlTranslator)).to_string();
        println!("{}",trans);
    }
}
