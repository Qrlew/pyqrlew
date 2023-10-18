use std::ops::Deref;
use qrlew::{builder::With, sql::{self, relation::QueryWithRelations}, relation::{self, Variant as _}, hierarchy::{Hierarchy, Path}, ast::Query};
use qrlew_sarus::{data_spec, protobuf::print_to_string};
use std::sync::Arc;
use pyo3::{pyclass, pymethods};

use crate::{relation::Relation, error::Result};

#[pyclass]
#[derive(Clone)]
pub struct Dataset(data_spec::Dataset);

impl Deref for Dataset {
    type Target = data_spec::Dataset;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Dataset> for data_spec::Dataset {
    fn from(value: Dataset) -> Self {
        value.0
    }
}

#[pymethods]
impl Dataset {
    #[new]
    pub fn new(dataset: &str, schema: &str, size: &str) -> Result<Self> {
        Ok(Dataset(data_spec::Dataset::parse_from_dataset_schema_size(
            dataset, schema, size,
        )?))
    }
    #[getter]
    pub fn schema(&self) -> Result<String> { 
        Ok(print_to_string(self.0.schema())?)
    }

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.deref().relations()
            .into_iter()
            .map(|(i, r)| (i, Relation::new(r)))
            .collect()
    }

    pub fn sql(&self, query: &str) -> Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.deref().relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation::new(Arc::new(query_with_relations.try_into()?)))
    }

    pub fn sql_from_dict(&self, queries: Vec<(Vec<String>, String)>) -> Result<Self> {
        let relations = self.deref().relations();

        let result_relations: Hierarchy<Arc<relation::Relation>> = queries
        .iter()
        .map(|(path, query)| {
            let parsed = sql::relation::parse(query)?;
            let query_with_rel = parsed.with(&relations);
            let rel = relation::Relation::try_from(query_with_rel)?;
            Ok( (path.clone(), Arc::new(rel)) )
        })
        .collect::<Result<Hierarchy<Arc<relation::Relation>>>>()?;
        let ds: data_spec::Dataset = (&result_relations).try_into()?;
        Ok(Dataset(ds))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qrlew::{ast::Query, display::Dot as _, DataType, Ready};
    const DATASET: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "e9cb9391ca184e89897f49bd75387a46", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}"#;
    const SCHEMA: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "5321f24ffb324a9e958c77ceb09b6cc8", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract", "type": {"name": "extract", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "extract", "type": {"name": "Union", "union": {"fields": [{"name": "beacon", "type": {"name": "Struct", "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "type": {"name": "Struct", "struct": {"fields": [{"name": "age", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "type": {"name": "Integer", "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {"public_fields": "[]"}}}]}, "properties": {"public_fields": "[]"}}}, {"name": "sarus_weights", "type": {"name": "Integer", "integer": {"min": "-9223372036854775808", "max": "9223372036854775807", "base": "INT64", "possible_values": []}, "properties": {}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}, "properties": {}}}, {"name": "sarus_protected_entity", "type": {"name": "Id", "id": {"base": "STRING", "unique": false}, "properties": {}}}]}, "properties": {}}, "protected": {"label": "data", "paths": [], "properties": {}}, "properties": {"max_max_multiplicity": "1", "foreign_keys": "", "primary_keys": ""}}"#;
    const SIZE: &str = r#"{"@type": "sarus_data_spec/sarus_data_spec.Size", "uuid": "cd8ec3f7958e4b2c842bc66ffa55e40c", "dataset": "c0d13d2c5d404e2c9930e01f63e18cee", "name": "extract_sizes", "statistics": {"name": "Union", "union": {"fields": [{"name": "extract", "statistics": {"name": "Union", "union": {"fields": [{"name": "beacon", "statistics": {"name": "Struct", "size": "100", "multiplicity": 1.0, "struct": {"fields": [{"name": "\u691c\u77e5\u65e5\u6642", "statistics": {"name": "Datetime", "size": "100", "multiplicity": 1.0, "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "01-01-01 00:00:00", "max": "9999-12-31 00:00:00"}, "properties": {}}}, {"name": "UserId", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u6240\u5c5e\u90e8\u7f72", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "\u30d5\u30ed\u30a2\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "Beacon\u540d", "statistics": {"name": "Text UTF-8", "size": "100", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "RSSI", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eX\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "\u30de\u30c3\u30d7\u306eY\u5ea7\u6a19", "statistics": {"name": "Integer", "size": "100", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}]}, "properties": {}}}, {"name": "census", "statistics": {"name": "Struct", "size": "199", "multiplicity": 1.0, "struct": {"fields": [{"name": "age", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "workclass", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "fnlwgt", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "education_num", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "marital_status", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "occupation", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "relationship", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "race", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "sex", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "capital_gain", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "capital_loss", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "hours_per_week", "statistics": {"name": "Integer", "size": "199", "multiplicity": 1.0, "integer": {"base": "INT64", "min": "-9223372036854775808", "max": "9223372036854775807", "possible_values": []}, "properties": {}}}, {"name": "native_country", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}, {"name": "income", "statistics": {"name": "Text UTF-8", "size": "199", "multiplicity": 1.0, "text": {"encoding": "UTF-8"}, "properties": {}}}]}, "properties": {}}}]}, "properties": {}}, "properties": {}}]}, "properties": {}}, "properties": {}}"#;

    // #[test]
    // fn test_dataset() {
    //     let dataset = Dataset::new(DATASET, SCHEMA, SIZE)?;
    //     println!("{}", dataset);
    // }

    #[test]
    fn test_hierarchy() {
        let values = Hierarchy::from([
            (vec!["a", "b", "c"], 1),
            (vec!["a", "b", "d"], 2),
        ]);
        println!("{:?} -> {}", ["b", "c"], values[["b", "c"]]);
    }

    #[test]
    fn test_rels() {
        let schema: relation::Schema = vec![
            ("a", DataType::float()),
            ("b", DataType::float_interval(-2., 2.)),
            ("c", DataType::float()),
            ("d", DataType::float_interval(0., 1.)),
        ]
        .into_iter()
        .collect();
        let table: relation::Relation = relation::Relation::table()
            .name("table")
            .schema(schema.clone())
            .size(100)
            .build();
        let hier = Hierarchy::from([(vec!["a", "b"], Arc::new(table))]);
        let ds = data_spec::Dataset::try_from(&hier).unwrap();
        println!("{:?}", ds)
    }
}
