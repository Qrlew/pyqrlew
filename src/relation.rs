use pyo3::prelude::*;
use qrlew::relation::Variant;
use qrlew::{relation, ast};
use serde_json::Value;
use std::rc::Rc;
use crate::{error, dataset};


#[pyclass(unsendable)]
pub struct Relation(pub Rc<relation::Relation>);

#[pymethods]
impl Relation {
    pub fn parse(&self, query: &str, dataset: &dataset::Dataset) -> error::Result<Self> {
        dataset.sql(query)
    }

    pub fn __str__(&self) -> String {
        let relation = &*(self.0);
        format!("{}", relation)
    }

    pub fn dot(&self) -> error::Result<String> {
        let mut out: Vec<u8> = vec![];
        (*self.0).dot(&mut out, &[]).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    pub fn schema(&self) -> String {
        (*self.0).schema().to_string()
    }

    pub fn protect(&self, dataset: &dataset::Dataset, protected_entity: &str) -> error::Result<Self> {
        let pe = parse_protected_entity(protected_entity);
        Ok(Relation(Rc::new(protect((*(self.0)).clone(), dataset, &pe))))
    }

    pub fn dp_compilation(&self, dataset: &dataset::Dataset, protected_entity: &str, epsilon: f64, delta: f64) -> error::Result<Self> {
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
    dataset: &dataset::Dataset,
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
    dataset: &dataset::Dataset,
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
