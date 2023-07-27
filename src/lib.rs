use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use qrlew::relation::Variant;
use qrlew::{builder::With, expr, relation, sql, ast};
use qrlew_sarus::{data_spec, protobuf};
use serde_json::{from_str, Value};
use std::{error, rc::Rc, result};

/*
Error management
 */

#[derive(Debug)]
struct Error(Box<dyn error::Error>);

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyRuntimeError::new_err(err.0.to_string())
    }
}

impl From<protobuf::ParseError> for Error {
    fn from(err: protobuf::ParseError) -> Error {
        Error(Box::new(err))
    }
}
impl From<protobuf::PrintError> for Error {
    fn from(err: protobuf::PrintError) -> Error {
        Error(Box::new(err))
    }
}
impl From<data_spec::Error> for Error {
    fn from(err: data_spec::Error) -> Error {
        Error(Box::new(err))
    }
}
impl From<relation::Error> for Error {
    fn from(err: relation::Error) -> Error {
        Error(Box::new(err))
    }
}
impl From<sql::Error> for Error {
    fn from(err: sql::Error) -> Error {
        Error(Box::new(err))
    }
}

type Result<T> = result::Result<T, Error>;

/*
Basic classes
 */

#[pyclass]
struct Dataset(data_spec::Dataset);

#[pymethods]
impl Dataset {
    #[new]
    pub fn new(dataset: &str, schema: &str, size: &str) -> Result<Self> {
        Ok(Dataset(data_spec::Dataset::parse_from_dataset_schema_size(
            dataset, schema, size,
        )?))
    }

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.0
            .relations()
            .into_iter()
            .map(|(i, r)| (i, Relation(r)))
            .collect()
    }

    pub fn sql(&self, query: &str) -> Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.0.relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation(Rc::new(query_with_relations.try_into()?)))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }

    // fn schema_type_data(&self) -> String {
    //     (self.0).schema_type_data()
    // }
}

#[pyclass(unsendable)]
struct Relation(Rc<relation::Relation>);

#[pymethods]
impl Relation {
    pub fn parse(&self, query: &str, dataset: &Dataset) -> Result<Self> {
        dataset.sql(query)
    }

    pub fn __str__(&self) -> String {
        let relation = &*(self.0);
        format!("{}", relation)
    }

    pub fn dot(&self) -> Result<String> {
        let mut out: Vec<u8> = vec![];
        (*self.0).dot(&mut out, &[]).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    pub fn schema(&self) -> String {
        (*self.0).schema().to_string()
    }

    pub fn protect(&self, dataset: &Dataset, protected_entity: &str) -> Result<Self> {
        let pe = parse_protected_entity(protected_entity);
        Ok(Relation(Rc::new(protect((*(self.0)).clone(), dataset, &pe))))
    }

    pub fn dp_compilation(&self, dataset: &Dataset, protected_entity: &str, epsilon: f64, delta: f64) -> Result<Self> {
        let pe = parse_protected_entity(protected_entity);
        Ok(Relation(Rc::new(dp_compilation((*(self.0)).clone(), dataset, &pe, epsilon, delta))))
    }

    pub fn render(&self) -> String {
        let relation = &*(self.0);
        let ast_query: ast::Query = relation.into();
        format!("{}", ast_query)
    }
}

fn protect<'a>(
    relation: relation::Relation,
    dataset: &Dataset,
    vec_of_string: &'a Vec<(String, Vec<(String, String, String)>, String)>,
) -> relation::Relation {
    let relations = dataset.0.relations();
    let vec_pe = vec_of_string
        .iter()
        .map(|(t, fk, n)| {
            let bidings = fk.iter()
            .map(|(a1, a2, a3)| (a1.as_str(), a2.as_str(), a3.as_str()))
            .collect::<Vec<_>>();
            (
                t.as_str(),
                bidings,
                n.as_str(),
            )
        }).collect::<Vec<_>>();
    let slice_pe = vec_pe.iter()
    .map(|(t, fk, n)| (*t, fk.as_slice(), *n)).collect::<Vec<_>>();
    relation.force_protect_from_field_paths(&relations, slice_pe.as_slice())
}

fn dp_compilation<'a>(
    relation: relation::Relation,
    dataset: &Dataset,
    vec_of_string: &'a Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64
) -> relation::Relation {
    let relations = dataset.0.relations();
    let vec_pe = vec_of_string
        .iter()
        .map(|(t, fk, n)| {
            let bidings = fk.iter()
            .map(|(a1, a2, a3)| (a1.as_str(), a2.as_str(), a3.as_str()))
            .collect::<Vec<_>>();
            (
                t.as_str(),
                bidings,
                n.as_str(),
            )
        }).collect::<Vec<_>>();
    let slice_pe = vec_pe.iter()
    .map(|(t, fk, n)| (*t, fk.as_slice(), *n)).collect::<Vec<_>>();
    relation.dp_compilation(&relations, slice_pe.as_slice(), epsilon, delta)
}

fn parse_protected_entity(str_pe: &str) -> Vec<(String, Vec<(String, String, String)>, String)> {
    let v: Value = serde_json::from_str(str_pe).expect("Failed to parse JSON");
    let mut peid: Vec<(String, Vec<(String, String, String)>, String)> = vec![];

    if let Value::Array(json_array) = &v["protected_entity"] {
        for table in json_array {
            if let Value::Array(table_array) = &table {
                assert_eq!(table_array.len(), 3);
                let tablename = if let Value::String(s) = &table_array[0] {
                    s.to_string()
                } else {
                    panic!()
                };
                let name = if let Value::String(s) = &table_array[2] {
                    s.to_string()
                } else {
                    panic!()
                };
                let foreign_keys: Vec<(String, String, String)> =
                    if let Value::Array(a) = &table_array[1] {
                        a.iter()
                            .map(|v| {
                                if let Value::Array(a) = &v {
                                    assert_eq!(a.len(), 3);
                                    let my_list = a
                                        .iter()
                                        .map(|e| {
                                            if let Value::String(s) = e {
                                                s.to_string()
                                            } else {
                                                panic!()
                                            }
                                        })
                                        .collect::<Vec<String>>();
                                    (my_list[0].clone(), my_list[1].clone(), my_list[2].clone())
                                } else {
                                    println!("v = {:?}", v);
                                    panic!()
                                }
                            })
                            .collect()
                    } else {
                        panic!()
                    };
                peid.push((tablename, foreign_keys, name))
            } else {
                panic!()
            }
        }
        peid
    } else {
        panic!()
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_parse_protected_entity() {
        let str_pe = r#"
        {
            "protected_entity": [
              ["rel1", [["col1", "rel2", "col2"], ["col3", "rel4", "col4"]], "coln"],
              ["rel2", [["col1", "rel2", "col2"]], "coln"]
            ]
        }
        "#;
        let peid = parse_protected_entity(str_pe);
        let v: Value = serde_json::from_str(str_pe).expect("Failed to parse JSON");
        let mut peid: Vec<(&String, Vec<[&String; 3]>, &String)> = vec![];

        if let Value::Array(json_array) = &v["protected_entity"] {
            for table in json_array {
                if let Value::Array(table_array) = &table {
                    assert_eq!(table_array.len(), 3);
                    let tablename = if let Value::String(s) = &table_array[0] {
                        s
                    } else {
                        panic!()
                    };
                    let name = if let Value::String(s) = &table_array[2] {
                        s
                    } else {
                        panic!()
                    };

                    let foreign_keys: Vec<[&std::string::String; 3]> =
                        if let Value::Array(a) = &table_array[1] {
                            a.iter()
                                .map(|v| {
                                    if let Value::Array(a) = &v {
                                        assert_eq!(a.len(), 3);
                                        let my_list = a
                                            .iter()
                                            .map(|e| {
                                                if let Value::String(s) = e {
                                                    s
                                                } else {
                                                    panic!()
                                                }
                                            })
                                            .collect::<Vec<&String>>();
                                        [my_list[0], my_list[1], my_list[2]]
                                    } else {
                                        println!("v = {:?}", v);
                                        panic!()
                                    }
                                })
                                .collect()
                        } else {
                            panic!()
                        };

                    peid.push((tablename, foreign_keys, name))
                }
            }
        }
        println!("{:?}", peid);
    }
}
