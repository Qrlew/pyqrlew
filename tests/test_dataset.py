import os
import numpy as np
import pandas as pd
from pyqrlew.io import PostgreSQL
from pyqrlew import Dialect, Dataset
from pyqrlew.utils import display_graph
from pyqrlew.wrappers import Relation

# pytest -s tests/test_dataset.py::test_ranges
def test_ranges():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    new_dataset = dataset.extract.census.age.with_range(20, 42)
    relation = new_dataset.relation('SELECT age FROM extract.census')
    print(relation.schema())
    assert(relation.schema()=='{age: int[20 42]}')

# pytest -s tests/test_dataset.py::test_possible_values
def test_possible_values():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    new_dataset = dataset.extract.census.workclass.with_possible_values(['Local-gov', 'Private'])
    relation = new_dataset.relation('SELECT workclass FROM extract.census')
    print(relation.schema())
    assert(relation.schema()=='{workclass: str{Local-gov, Private}}')

# pytest -s tests/test_dataset.py::test_constraint
def test_constraint():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    new_dataset = dataset.extract.census.age.with_unique_constraint()
    relation = new_dataset.relation('SELECT age FROM extract.census')
    print(relation.schema())
    assert(relation.schema()=='{age: int[20 90] (UNIQUE)}')

# pytest -s tests/test_dataset.py::test_relation
def test_relation():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    relation = dataset.extract.census.relation()
    print(relation.schema())


# pytest -s tests/test_dataset.py::test_from_queries
def test_from_queries():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db
    queries = [
        (("dataset_name", "my_schema", "boomers",), "SELECT * FROM extract.census WHERE age >= 60"),
        (("dataset_name", "my_schema", "genx",), "SELECT * FROM extract.census WHERE age >= 40 AND age < 60"),
        (("dataset_name", "my_schema", "millenials",), "SELECT * FROM extract.census WHERE age >= 30 AND age < 40"),
        (("dataset_name", "my_schema", "genz",), "SELECT * FROM extract.census WHERE age < 30"),
    ]

    new_ds = dataset.from_queries(queries)
    genx = new_ds.my_schema.genx.relation()
    print(genx.schema())

# pytest -s tests/test_dataset.py::test_from_dp_compiled_queries
def test_from_dp_compiled_queries():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db

    query = "SELECT COUNT(*) FROM extract.census"
    rel = Relation.from_query(query, dataset)
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_")
    ]
    import sqlalchemy as sa
    epsilon_delta={"epsilon": 1.0, "delta": 1e-3}
    dprel_and_event = rel.rewrite_with_differential_privacy(
        dataset=dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )
    
    dprel = dprel_and_event.relation()
    # display_graph(dprel.dot())

    new_rel = Relation.from_query(dprel.to_query(), dataset)
    # display_graph(new_rel.dot())


def test_from_database(tables, engine):
    ds =  Dataset.from_database('ds_name', engine, None)
    print(ds)
    print(ds.schema)
    paths_rels = ds.relations()
    for path, rel in paths_rels:
        print(path)
        # display_graph(rel.dot())


def test_from_str():
    dataset = """
    {
    "@type": "sarus_data_spec/sarus_data_spec.Dataset",
    "doc": "",
    "name": "Transformed",
    "properties": {},
    "spec": {
        "transformed": {
        "arguments": [
            "8dbf3c1ea95cafa31275b5724452c428"
        ],
        "named_arguments": {
            "user_type": "f4c3b41cf3c04d600ac265d6b4e003be"
        },
        "transform": "db88b3e93f318830c4c9b7b151b5b685"
        }
    },
    "uuid": "365b586cf9459f727f42337e36ea834c"
    }
    """
    schema = """
    {
  "@type": "sarus_data_spec/sarus_data_spec.Schema",
  "dataset": "365b586cf9459f727f42337e36ea834c",
  "name": "demo_db_schema",
  "privacy_unit": {
    "label": "",
    "paths": [],
    "properties": {}
  },
  "properties": {
    "foreign_keys": "{\\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHAoNcHJpbWFyeV90YWJsZRILCglwdWJsaWNfZms=\\": \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHAoUcHJpbWFyeV9wdWJsaWNfdGFibGUSBAoCaWQ=\\", \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSFwoPc2Vjb25kYXJ5X3RhYmxlEgQKAmlk\\": \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSFQoNcHJpbWFyeV90YWJsZRIECgJpZA==\\", \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHgoPc2Vjb25kYXJ5X3RhYmxlEgsKCXB1YmxpY19maw==\\": \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHgoWc2Vjb25kYXJ5X3B1YmxpY190YWJsZRIECgJpZA==\\"}",
    "primary_keys": "[\\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSFQoNcHJpbWFyeV90YWJsZRIECgJpZA==\\", \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHAoUcHJpbWFyeV9wdWJsaWNfdGFibGUSBAoCaWQ=\\", \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSHgoWc2Vjb25kYXJ5X3B1YmxpY190YWJsZRIECgJpZA==\\"]"
  },
  "type": {
    "name": "Union",
    "properties": {},
    "union": {
      "fields": [
        {
          "name": "primary_table",
          "type": {
            "name": "Struct",
            "properties": {},
            "struct": {
              "fields": [
                {
                  "name": "id",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "unique": true
                    },
                    "name": "Id",
                    "properties": {}
                  }
                },
                {
                  "name": "integer",
                  "type": {
                    "integer": {
                      "base": "INT32",
                      "max": "99",
                      "min": "0",
                      "possible_values": []
                    },
                    "name": "Integer",
                    "properties": {
                      "possible_values": "[82, 4, 38, 18, 60, 85, 81, 86, 57, 61, 54, 35, 1, 88, 24, 31, 56, 73, 80, 62, 12, 51, 64, 71, 84, 79, 32, 16, 43, 99, 39, 19, 66, 48, 45, 5, 2, 67, 30, 77, 36, 59, 50, 29, 52, 44, 6, 21, 98, 94, 72, 13, 26, 14, 25, 40, 11, 89, 9, 46, 28, 20, 87, 96, 49, 58, 97, 95, 47, 55, 3, 92, 7, 37, 90, 74, 42, 22, 23, 41, 0, 91, 69, 17, 65, 15, 75, 76, 10, 70, 34, 78, 27, 63, 68, 33, 93, 83, 8, 53]",
                      "possible_values_length": "100"
                    }
                  }
                },
                {
                  "name": "float",
                  "type": {
                    "float": {
                      "base": "FLOAT32",
                      "max": 0.9992868304252625,
                      "min": 0.00011310506670270115,
                      "possible_values": []
                    },
                    "name": "Float64",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "datetime",
                  "type": {
                    "datetime": {
                      "base": "INT64_NS",
                      "format": "%Y-%m-%d %H:%M:%S",
                      "max": "1980-01-31 23:23:22",
                      "min": "1980-01-01 01:04:49",
                      "possible_values": []
                    },
                    "name": "Datetime",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "date",
                  "type": {
                    "date": {
                      "base": "INT32",
                      "format": "%Y-%m-%d",
                      "max": "2000-01-01",
                      "min": "1980-01-03",
                      "possible_values": []
                    },
                    "name": "Date",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "boolean",
                  "type": {
                    "boolean": {},
                    "name": "Boolean",
                    "properties": {}
                  }
                },
                {
                  "name": "text",
                  "type": {
                    "name": "Text UTF-8",
                    "properties": {
                      "max_length": "8",
                      "min_length": "8",
                      "possible_values_length": "301",
                      "text_alphabet_name": "Simple",
                      "text_char_set": "[48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]"
                    },
                    "text": {
                      "encoding": "UTF-8",
                      "possible_values": []
                    }
                  }
                },
                {
                  "name": "public_fk",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "reference": {
                        "label": "primary_public_table",
                        "paths": [
                          {
                            "label": "id",
                            "paths": [],
                            "properties": {}
                          }
                        ],
                        "properties": {}
                      },
                      "unique": false
                    },
                    "name": "Id",
                    "properties": {}
                  }
                }
              ]
            }
          }
        },
        {
          "name": "secondary_table",
          "type": {
            "name": "Struct",
            "properties": {},
            "struct": {
              "fields": [
                {
                  "name": "id",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "reference": {
                        "label": "primary_table",
                        "paths": [
                          {
                            "label": "id",
                            "paths": [],
                            "properties": {}
                          }
                        ],
                        "properties": {}
                      },
                      "unique": false
                    },
                    "name": "Id",
                    "properties": {}
                  }
                },
                {
                  "name": "integer",
                  "type": {
                    "integer": {
                      "base": "INT32",
                      "max": "99",
                      "min": "0",
                      "possible_values": []
                    },
                    "name": "Integer",
                    "properties": {
                      "possible_values": "[96, 71, 38, 41, 40, 6, 37, 95, 21, 9, 2, 57, 17, 88, 48, 72, 77, 22, 34, 68, 10, 32, 7, 58, 18, 46, 30, 26, 31, 51, 15, 67, 60, 28, 74, 54, 63, 75, 42, 47, 69, 4, 62, 64, 82, 55, 86, 36, 39, 73, 93, 59, 49, 19, 81, 98, 89, 0, 5, 53, 11, 76, 84, 12, 27, 90, 79, 78, 92, 52, 33, 70, 1, 85, 16, 24, 23, 91, 97, 35, 83, 25, 99, 29, 20, 13, 61, 87, 45, 50, 3, 56, 14, 66, 80, 43, 44, 94, 8, 65]",
                      "possible_values_length": "100"
                    }
                  }
                },
                {
                  "name": "float",
                  "type": {
                    "float": {
                      "base": "FLOAT32",
                      "max": 0.9992868304252625,
                      "min": 0.00011310506670270115,
                      "possible_values": []
                    },
                    "name": "Float64",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "datetime",
                  "type": {
                    "datetime": {
                      "base": "INT64_NS",
                      "format": "%Y-%m-%d %H:%M:%S",
                      "max": "1980-01-31 23:23:22",
                      "min": "1980-01-01 01:04:49",
                      "possible_values": []
                    },
                    "name": "Datetime",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "date",
                  "type": {
                    "date": {
                      "base": "INT32",
                      "format": "%Y-%m-%d",
                      "max": "2000-01-01",
                      "min": "1980-01-03",
                      "possible_values": []
                    },
                    "name": "Date",
                    "properties": {
                      "possible_values_length": "301"
                    }
                  }
                },
                {
                  "name": "boolean",
                  "type": {
                    "boolean": {},
                    "name": "Boolean",
                    "properties": {}
                  }
                },
                {
                  "name": "text",
                  "type": {
                    "name": "Text UTF-8",
                    "properties": {
                      "max_length": "8",
                      "min_length": "8",
                      "possible_values_length": "301",
                      "text_alphabet_name": "Simple",
                      "text_char_set": "[48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]"
                    },
                    "text": {
                      "encoding": "UTF-8",
                      "possible_values": []
                    }
                  }
                },
                {
                  "name": "public_fk",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "reference": {
                        "label": "secondary_public_table",
                        "paths": [
                          {
                            "label": "id",
                            "paths": [],
                            "properties": {}
                          }
                        ],
                        "properties": {}
                      },
                      "unique": false
                    },
                    "name": "Id",
                    "properties": {}
                  }
                }
              ]
            }
          }
        },
        {
          "name": "primary_public_table",
          "type": {
            "name": "Struct",
            "properties": {},
            "struct": {
              "fields": [
                {
                  "name": "id",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "unique": true
                    },
                    "name": "Id",
                    "properties": {}
                  }
                },
                {
                  "name": "text",
                  "type": {
                    "name": "Text UTF-8",
                    "properties": {
                      "max_length": "8",
                      "min_length": "8",
                      "possible_values": "[\\"NFNZN2W4\\", \\"JPA2Z33I\\", \\"JAQLY7IO\\"]",
                      "possible_values_length": "3",
                      "text_alphabet_name": "Simple",
                      "text_char_set": "[50, 51, 52, 55, 65, 70, 73, 74, 76, 78, 79, 80, 81, 87, 89, 90]"
                    },
                    "text": {
                      "encoding": "UTF-8",
                      "possible_values": []
                    }
                  }
                }
              ]
            }
          }
        },
        {
          "name": "secondary_public_table",
          "type": {
            "name": "Struct",
            "properties": {},
            "struct": {
              "fields": [
                {
                  "name": "id",
                  "type": {
                    "id": {
                      "base": "INT32",
                      "unique": true
                    },
                    "name": "Id",
                    "properties": {}
                  }
                },
                {
                  "name": "text",
                  "type": {
                    "name": "Text UTF-8",
                    "properties": {
                      "max_length": "8",
                      "min_length": "8",
                      "possible_values": "[\\"JPA2Z33I\\", \\"JAQLY7IO\\"]",
                      "possible_values_length": "2",
                      "text_alphabet_name": "Simple",
                      "text_char_set": "[50, 51, 55, 65, 73, 74, 76, 79, 80, 81, 89, 90]"
                    },
                    "text": {
                      "encoding": "UTF-8",
                      "possible_values": []
                    }
                  }
                }
              ]
            }
          }
        }
      ]
    }
  },
  "uuid": "cf940c8889fc6321607d80bb8553aa39"
}
    """

    parent_ds = Dataset.from_str(dataset, schema, "")
    
    prt_query = """
    SELECT anon_1."id", anon_1."integer", anon_1."float", anon_1."datetime", anon_1."date", anon_1."boolean", anon_1."text", anon_1."public_fk", 0 AS sarus_is_public, md5(concat(md5(concat('3c7787272a91a5753234666ca398ba8d3cf6d9ff6dda137886ea9a77a3d19f7b', md5(concat(md5(CAST(id AS VARCHAR)), md5(CAST(integer AS VARCHAR)), md5(CAST(float AS VARCHAR)), md5(CAST(datetime AS VARCHAR)), md5(CAST(date AS VARCHAR)), md5(CAST(boolean AS VARCHAR)), md5(CAST(text AS VARCHAR)), md5(CAST(public_fk AS VARCHAR)))))), md5(concat('0e8b99dba4369b3265b0b9e1337d3ae04d70881174bce77685e75fd9b46e23e0')), md5(concat('17bbc4d13311f67df8bc1e832c9a5a082f61c28be77e933c7759c9d70b343b86')), md5(concat('2f4f7d6eaa60d8aaa2b102efb6054a94e76aa4020776c63b6f5ab230eec43e1b')))) AS sarus_privacy_unit, 1.0 AS sarus_weights 
    FROM (SELECT "primary_table"."id" AS "id", "primary_table"."integer" AS "integer", "primary_table"."float" AS "float", "primary_table"."datetime" AS "datetime", "primary_table"."date" AS "date", "primary_table"."boolean" AS "boolean", "primary_table"."text" AS "text", "primary_table"."public_fk" AS "public_fk" 
    FROM "primary_table") AS anon_1
    """

    sdr_query = """
    SELECT anon_1."id", anon_1."integer", anon_1."float", anon_1."datetime", anon_1."date", anon_1."boolean", anon_1."text", anon_1."public_fk", 0 AS sarus_is_public, md5(concat(md5(concat('3c7787272a91a5753234666ca398ba8d3cf6d9ff6dda137886ea9a77a3d19f7b')), md5(concat('0e8b99dba4369b3265b0b9e1337d3ae04d70881174bce77685e75fd9b46e23e0', md5(concat(md5(CAST(id AS VARCHAR)), md5(CAST(integer AS VARCHAR)), md5(CAST(float AS VARCHAR)), md5(CAST(datetime AS VARCHAR)), md5(CAST(date AS VARCHAR)), md5(CAST(boolean AS VARCHAR)), md5(CAST(text AS VARCHAR)), md5(CAST(public_fk AS VARCHAR)))))), md5(concat('17bbc4d13311f67df8bc1e832c9a5a082f61c28be77e933c7759c9d70b343b86')), md5(concat('2f4f7d6eaa60d8aaa2b102efb6054a94e76aa4020776c63b6f5ab230eec43e1b')))) AS sarus_privacy_unit, 1.0 AS sarus_weights 
    FROM (SELECT "secondary_table"."id" AS "id", "secondary_table"."integer" AS "integer", "secondary_table"."float" AS "float", "secondary_table"."datetime" AS "datetime", "secondary_table"."date" AS "date", "secondary_table"."boolean" AS "boolean", "secondary_table"."text" AS "text", "secondary_table"."public_fk" AS "public_fk" 
    FROM "secondary_table") AS anon_1
    """

    pr_pub = """
    SELECT "primary_public_table"."id", "primary_public_table"."text", 1 AS sarus_is_public, NULL AS sarus_privacy_unit, 1.0 AS sarus_weights 
    FROM "primary_public_table"
    """

    sd_pub = """
    SELECT "secondary_public_table"."id", "secondary_public_table"."text", 1 AS sarus_is_public, NULL AS sarus_privacy_unit, 1.0 AS sarus_weights 
    FROM "secondary_public_table"
    """

    queries = [
        (('demo_db_schema', 'primary_table'), prt_query),
        (('demo_db_schema', 'secondary_table'), sdr_query),
        (('demo_db_schema', 'primary_public_table'), pr_pub),
        (('demo_db_schema', 'secondary_public_table'), sd_pub),
    ]
    current_ds = parent_ds.from_queries(queries)
    relations = [
        (path, Relation.from_query(q, parent_ds))
        for (path, q) in queries
    ]

    q = """
  WITH "map_bk32" ("id", "integer", "float", "datetime", "date", "boolean", "text", "public_fk") AS (SELECT "id" AS "id", "integer" AS "integer", "float" AS "float", "datetime" AS "datetime", "date" AS "date", "boolean" AS "boolean", "text" AS "text", "public_fk" AS "public_fk" FROM "secondary_table"), "map_ozmc" ("id", "integer", "float", "datetime", "date", "boolean", "text", "public_fk", "sarus_is_public", "sarus_privacy_unit", "sarus_weights") AS (SELECT "id" AS "id", "integer" AS "integer", "float" AS "float", "datetime" AS "datetime", "date" AS "date", "boolean" AS "boolean", "text" AS "text", "public_fk" AS "public_fk", 0 AS "sarus_is_public", MD5(CONCAT(MD5(CONCAT('3c7787272a91a5753234666ca398ba8d3cf6d9ff6dda137886ea9a77a3d19f7b')), MD5(CONCAT('0e8b99dba4369b3265b0b9e1337d3ae04d70881174bce77685e75fd9b46e23e0', MD5(CONCAT(MD5(CAST("id" AS TEXT)), MD5(CAST("integer" AS TEXT)), MD5(CAST("float" AS TEXT)), MD5(CAST("datetime" AS TEXT)), MD5(CAST("date" AS TEXT)), MD5(CAST("boolean" AS TEXT)), MD5(CAST("text" AS TEXT)), MD5(CAST("public_fk" AS TEXT)))))), MD5(CONCAT('17bbc4d13311f67df8bc1e832c9a5a082f61c28be77e933c7759c9d70b343b86')), MD5(CONCAT('2f4f7d6eaa60d8aaa2b102efb6054a94e76aa4020776c63b6f5ab230eec43e1b')))) AS "sarus_privacy_unit", 1 AS "sarus_weights" FROM "map_bk32"), "map_sdzy" ("datetime_1") AS (SELECT "datetime" AS "datetime_1" FROM "map_ozmc"), "map_ikls" ("datetime_1") AS (SELECT (1000000000) * (CAST(EXTRACT(EPOCH FROM "datetime_1") AS FLOAT)) AS "datetime_1" FROM "map_sdzy"), "map_tgsd" ("datetime_1") AS (SELECT "datetime_1" AS "datetime_1" FROM "map_ikls"), "map_zzej" ("binned") AS (SELECT CASE WHEN (("datetime_1") >= (315545268000000000)) AND (("datetime_1") <= (318209002000000000)) THEN 315545268000000000 ELSE NULL END AS "binned" FROM "map_tgsd"), "map_giv8" ("binned") AS (SELECT "binned" AS "binned" FROM "map_zzej"), "map_iee8" ("field_epaa") AS (SELECT "binned" AS "field_epaa" FROM "map_giv8"), "reduce_2o86" ("field_epaa", "field_15zm") AS (SELECT "field_epaa" AS "field_epaa", COUNT("field_epaa") AS "field_15zm" FROM "map_iee8" GROUP BY "field_epaa"), "map_q1q3" ("binned", "count_") AS (SELECT "field_epaa" AS "binned", ("field_15zm") - (((1.6509217023849487) * (LOG((1) - ((2) * (ABS((RANDOM()) - (0.5))))))) * (SIGN((RANDOM()) - (0.5)))) AS "count_" FROM "reduce_2o86"), "map_ehyc" ("field_epaa") AS (SELECT "binned" AS "field_epaa" FROM "map_q1q3" WHERE ("count_") >= (34.21248936119867)), "reduce_iso8" ("min_value") AS (SELECT MIN("field_epaa") AS "min_value" FROM "map_ehyc"), "map_8ui2" ("min_value") AS (SELECT GREATEST("min_value", 315545268000000000) AS "min_value" FROM "reduce_iso8"), "map_zzvv" ("field_kvi9", "field__1dq", "min_value") AS (SELECT '"secondary_table"."datetime"' AS "field_kvi9", 'min' AS "field__1dq", "min_value" AS "min_value" FROM "map_8ui2") SELECT * FROM "map_zzvv"
    """

    rel = Relation.from_query(q, current_ds)
    display_graph(rel.dot())
    # composed = rel.compose(relations)
    # display_graph(composed.dot())

