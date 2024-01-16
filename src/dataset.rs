use pyo3::prelude::*;
use qrlew::{
    builder::With,
    hierarchy::Hierarchy,
    relation,
    sql, dialect_translation::{postgres::PostgresTranslator, QueryToRelationTranslator, mssql::MSSQLTranslator},
};
use qrlew_sarus::{
    data_spec,
    protobuf::{
        print_to_string,
        type_::{Type, type_},
    },
};
use std::{ops::Deref, borrow::BorrowMut};
use std::sync::Arc;

use crate::{error::{Result, Error, MissingKeyError}, relation::Relation};

/// A Dataset is a set of SQL Tables
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
    /// The schema of the Dataset
    #[getter]
    pub fn schema(&self) -> Result<String> {
        Ok(print_to_string(self.0.schema())?)
    }

    #[getter]
    pub fn size(&self) -> Option<String> {
        match self.0.size() {
            Some(size_proto) => print_to_string(size_proto).ok(),
            None => None,
        }
    }

    // TODO use with_range from Sarus
    // pub fn range(&self, schema_name: &str, table_name: &str, field_name: &str, min: f64, max: f64) -> Result<Self> {
    //     let mut new_schema = self.0.schema().clone();
    //     println!("DEBUG range {}", new_schema.mut_type());
    //     let mut sarus_data = new_schema.mut_type().mut_struct().mut_fields().iter_mut().find(|field| field.name()=="sarus_data").ok_or_else(|| MissingKeyError(schema_name.into()))?;
    //     let mut schema = sarus_data.mut_type().mut_union().mut_fields().iter_mut().find(|field| field.name()==schema_name).ok_or_else(|| MissingKeyError(schema_name.into()))?;
    //     let mut table = schema.mut_type().mut_union().mut_fields().iter_mut().find(|field| field.name()==table_name).ok_or_else(|| MissingKeyError(table_name.into()))?;
    //     let mut field = table.mut_type().mut_struct().mut_fields().iter_mut().find(|field| field.name()==field_name).ok_or_else(|| MissingKeyError(field_name.into()))?;
    //     match &mut field.mut_type().type_ {
    //         Some(type_::Type::Integer(integer)) => {
    //             integer.set_min(min.round() as i64);
    //             integer.set_max(max.round() as i64);
    //         },
    //         Some(type_::Type::Float(float)) => {
    //             float.set_min(min);
    //             float.set_max(max);
    //         },
    //         Some(type_::Type::Optional(optional)) => {
    //             match &mut optional.mut_type().type_ {
    //                 Some(type_::Type::Integer(integer)) => {
    //                     integer.set_min(min.round() as i64);
    //                     integer.set_max(max.round() as i64);
    //                 },
    //                 Some(type_::Type::Float(float)) => {
    //                     float.set_min(min);
    //                     float.set_max(max);
    //                 },
    //                 _ => (),
    //             }
    //         },
    //         _ => (),
    //     }
    //     Ok(Dataset(data_spec::Dataset::new(self.0.dataset().clone(), new_schema, self.0.size().cloned())))
    // }

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.deref()
            .relations()
            .into_iter()
            .map(|(i, r)| (i, Relation::new(r)))
            .collect()
    }

    pub fn relation(&self, query: &str, dialect: Option<Dialect>) -> Result<Relation> {
        let dialect = dialect.unwrap_or(Dialect::Postgres);
        let relations = self.deref().relations();
        
        match dialect {
            Dialect::Postgres => {
                let translator = PostgresTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            },
            Dialect::Mssql => {
                let translator = MSSQLTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            }, 
        
        }  
    }

    pub fn from_queries(&self, queries: Vec<(Vec<String>, String)>, dialect: Option<Dialect>) -> Result<Self> {
        let relations = self.deref().relations();
        let dialect = dialect.unwrap_or(Dialect::Postgres);

        let result_relations: Result<Hierarchy<Arc<relation::Relation>>> = queries
            .iter()
            .map(|(path, query)| {
                match dialect {
                    Dialect::Postgres => {
                        let tranlator = PostgresTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, tranlator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, tranlator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    },
                    Dialect::Mssql => {
                        let tranlator = MSSQLTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, tranlator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, tranlator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    }
                }
            })
            .collect();
        let ds: data_spec::Dataset = (&result_relations?).try_into()?;
        Ok(Dataset(ds))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}
