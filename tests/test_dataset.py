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

def test_ranges_with_ds_from_engine(tables, engine):
    ds =  Dataset.from_database('ds_name', engine, None)
    new_dataset = ds.primary_public_table.id.with_range(-10.0, 13.0)
    relation = new_dataset.relation('SELECT id FROM primary_public_table')
    assert(relation.schema()=='{id: int[-10 13]}')


# pytest -s tests/test_dataset.py::test_possible_values
def test_possible_values():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    new_dataset = dataset.extract.census.workclass.with_possible_values(['Local-gov', 'Private'])
    relation = new_dataset.relation('SELECT workclass FROM extract.census')
    print(relation.schema())
    assert(relation.schema()=='{workclass: str{Local-gov, Private}}')

def test_possible_values_with_ds_from_engine(tables, engine):
    """Test the consistency of results for queries stored in a file."""
    ds =  Dataset.from_database('ds_name', engine, None)
    new_dataset = ds.primary_public_table.text.with_possible_values(["A", "B", "C"])
    relation = new_dataset.relation('SELECT "text" FROM primary_public_table')
    assert(relation.schema()=='{text: str{A, B, C}}')

# pytest -s tests/test_dataset.py::test_constraint
def test_constraint():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db        
    new_dataset = dataset.extract.census.age.with_unique_constraint()
    relation = new_dataset.relation('SELECT age FROM extract.census')
    print(relation.schema())
    assert(relation.schema()=='{age: int[20 90] (UNIQUE)}')

def test_constraint_with_ds_from_engine(tables, engine):
    ds =  Dataset.from_database('ds_name', engine, None)
    new_dataset = ds.primary_public_table.id.with_unique_constraint()
    relation = new_dataset.relation('SELECT id FROM primary_public_table')
    assert(relation.schema()=='{id: int (UNIQUE)}')

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
            "c07f899129e90190f48db0aebbff8abe"
        ],
        "named_arguments": {
            "privacy_unit_tracking_paths": "ece2e9d477e6c88513551c78499cbbad",
            "public_paths": "c3e688ad907078c5d834bc3a2cf4bca8"
        },
        "transform": "5c620caa2fcc37332fcd73d66372ffb5"
        }
    },
    "uuid": "50c5f0570960e686b724275e0adb39af"
    }
    """
    schema = """
    {
    "@type": "sarus_data_spec/sarus_data_spec.Schema",
    "dataset": "50c5f0570960e686b724275e0adb39af",
    "name": "demo_db_schema",
    "privacy_unit": {
        "label": "sarus_data",
        "paths": [
        {
            "label": "primary_table",
            "paths": [],
            "properties": {}
        },
        {
            "label": "secondary_table",
            "paths": [],
            "properties": {}
        },
        {
            "label": "primary_public_table",
            "paths": [],
            "properties": {}
        },
        {
            "label": "secondary_public_table",
            "paths": [],
            "properties": {}
        }
        ],
        "properties": {}
    },
    "properties": {},
    "type": {
        "name": "Struct",
        "properties": {},
        "struct": {
        "fields": [
            {
            "name": "sarus_data",
            "type": {
                "name": "Union",
                "properties": {
                "sarus_is_public": "False"
                },
                "union": {
                "fields": [
                    {
                    "name": "primary_table",
                    "type": {
                        "name": "Struct",
                        "properties": {
                        "merge_paths": "0",
                        "sarus_is_public": "False"
                        },
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
                                "possible_values": "[18, 79, 32, 54, 14, 81, 84, 36, 33, 24, 43, 48, 90, 99, 21, 70, 85, 51, 27, 63, 55, 49, 12, 39, 37, 44, 3, 26, 76, 65, 15, 42, 82, 4, 28, 9, 16, 62, 31, 68, 57, 87, 77, 6, 41, 22, 72, 25, 69, 7, 8, 96, 89, 38, 91, 66, 29, 5, 64, 71, 61, 20, 95, 88, 30, 50, 19, 78, 56, 0, 97, 58, 10, 98, 2, 47, 80, 35, 75, 73, 13, 83, 46, 17, 23, 74, 86, 67, 60, 1, 45, 53, 40, 11, 34, 52, 94, 59, 92, 93]",
                                "possible_values_length": "100"
                                }
                            }
                            },
                            {
                            "name": "float",
                            "type": {
                                "float": {
                                "base": "FLOAT32",
                                "max": 0.9992433190345764,
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
                                "max": "1980-01-31 18:51:02",
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
                                "min": "1980-01-23",
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
                                    "label": "sarus_data",
                                    "paths": [
                                    {
                                        "label": "primary_public_table",
                                        "paths": [
                                        {
                                            "label": "id",
                                            "paths": [],
                                            "properties": {}
                                        }
                                        ],
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
                        "properties": {
                        "merge_paths": "0",
                        "sarus_is_public": "False"
                        },
                        "struct": {
                        "fields": [
                            {
                            "name": "id",
                            "type": {
                                "id": {
                                "base": "INT32",
                                "reference": {
                                    "label": "sarus_data",
                                    "paths": [
                                    {
                                        "label": "primary_table",
                                        "paths": [
                                        {
                                            "label": "id",
                                            "paths": [],
                                            "properties": {}
                                        }
                                        ],
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
                                "possible_values": "[58, 72, 88, 37, 76, 26, 20, 0, 54, 93, 46, 28, 31, 3, 84, 30, 66, 17, 4, 27, 44, 94, 73, 9, 77, 69, 67, 15, 61, 56, 21, 42, 25, 16, 11, 49, 35, 90, 71, 81, 24, 53, 10, 74, 86, 62, 85, 13, 55, 48, 92, 83, 89, 59, 97, 5, 98, 50, 51, 6, 19, 96, 82, 41, 39, 7, 1, 2, 78, 79, 63, 47, 18, 43, 38, 57, 29, 8, 80, 70, 65, 68, 23, 33, 40, 60, 95, 99, 32, 87, 14, 36, 12, 34, 75, 22, 52, 64, 45, 91]",
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
                                    "label": "sarus_data",
                                    "paths": [
                                    {
                                        "label": "secondary_public_table",
                                        "paths": [
                                        {
                                            "label": "id",
                                            "paths": [],
                                            "properties": {}
                                        }
                                        ],
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
                        "properties": {
                        "merge_paths": "1",
                        "sarus_is_public": "True"
                        },
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
                                "possible_values": "[\\"JPA2Z33I\\"]",
                                "possible_values_length": "1",
                                "text_alphabet_name": "Simple",
                                "text_char_set": "[50, 51, 65, 73, 74, 80, 90]"
                                },
                                "text": {
                                "encoding": "UTF-8",
                                "possible_values": [
                                    "JPA2Z33I"
                                ]
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
                        "properties": {
                        "merge_paths": "1",
                        "sarus_is_public": "True"
                        },
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
                                "possible_values": "[\\"JPA2Z33I\\"]",
                                "possible_values_length": "1",
                                "text_alphabet_name": "Simple",
                                "text_char_set": "[50, 51, 65, 73, 74, 80, 90]"
                                },
                                "text": {
                                "encoding": "UTF-8",
                                "possible_values": [
                                    "JPA2Z33I"
                                ]
                                }
                            }
                            }
                        ]
                        }
                    }
                    }
                ]
                }
            }
            },
            {
            "name": "sarus_is_public",
            "type": {
                "boolean": {},
                "name": "Boolean",
                "properties": {}
            }
            },
            {
            "name": "sarus_privacy_unit",
            "type": {
                "name": "Optional",
                "optional": {
                "type": {
                    "id": {
                    "base": "STRING",
                    "unique": false
                    },
                    "name": "Id",
                    "properties": {}
                }
                },
                "properties": {}
            }
            },
            {
            "name": "sarus_weights",
            "type": {
                "float": {
                "base": "FLOAT64",
                "max": 1.7976931348623157e+308,
                "min": 0.0,
                "possible_values": []
                },
                "name": "Float64",
                "properties": {}
            }
            }
        ]
        }
    },
    "uuid": "0995b7557812707dbdd97d826ef34fa5"
    }
    """

    real_ds = Dataset.from_str(dataset, schema, "")
    
    query = 'SELECT COUNT(DISTINCT sarus_privacy_unit) FROM primary_table'
    rel = Relation.from_query(query, real_ds)
    # display_graph(rel.dot())
