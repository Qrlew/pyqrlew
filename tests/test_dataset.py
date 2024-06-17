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
    for _, rel in new_ds.relations():
        display_graph(rel.dot())
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
    display_graph(dprel.dot())

    new_rel = Relation.from_query(dprel.to_query(), dataset)
    display_graph(new_rel.dot())


def test_from_database():
    import sqlalchemy as sa
    url = "postgresql+psycopg2://postgres:pyqrlew-db@localhost:5433/test_db"
    engine = sa.create_engine(url)
    ds =  Dataset.from_database('ds_name', engine, 'st51_bicdlwoy')
    print(ds)
    print(ds.schema)
    paths_rels = ds.relations()
    for path, rel in paths_rels:
        print(path)
        display_graph(rel.dot())


# def test_from_str():
#     dataset = '{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "f31d342bc8284fa2b8f36fbfb869aa3a", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}'
#     schema = '''
#     {
#     "@type":"sarus_data_spec/sarus_data_spec.Schema",
#     "uuid":"f0e998eb7b904be9bcd656c4157357f6",
#     "dataset":"f31d342bc8284fa2b8f36fbfb869aa3a",
#     "name":"Transformed_schema",
#     "type":{
#         "name":"transformed_schema",
#         "struct":{
#             "fields":[
#                 {
#                 "name":"sarus_data",
#                 "type":{
#                     "name":"Union",
#                     "union":{
#                         "fields":[
#                             {
#                             "name":"st51_bicdlwoy",
#                             "type":{
#                                 "name":"Union",
#                                 "union":{
#                                     "fields":[
#                                         {
#                                         "name":"xwiromvh",
#                                         "type":{
#                                             "name":"Struct",
#                                             "struct":{
#                                                 "fields":[
#                                                     {
#                                                     "name":"id",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"text",
#                                                     "type":{
#                                                         "name":"Text UTF-8",
#                                                         "text":{
#                                                             "encoding":"UTF-8"
#                                                         }
#                                                     }
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                         },
#                                         {
#                                         "name":"qqnhlkqe",
#                                         "type":{
#                                             "name":"Struct",
#                                             "struct":{
#                                                 "fields":[
#                                                     {
#                                                     "name":"id",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"integer",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"float",
#                                                     "type":{
#                                                         "name":"Float64",
#                                                         "float":{
#                                                             "min":-1125899906842624.0,
#                                                             "max":1125899906842624.0
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"datetime",
#                                                     "type":{
#                                                         "name":"Datetime",
#                                                         "datetime":{
#                                                             "format":"%Y-%m-%d %H:%M:%S",
#                                                             "min":"01-01-01 00:00:00",
#                                                             "max":"9999-12-31 00:00:00"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"date",
#                                                     "type":{
#                                                         "name":"Datetime",
#                                                         "datetime":{
#                                                             "format":"%Y-%m-%d %H:%M:%S",
#                                                             "min":"01-01-01 00:00:00",
#                                                             "max":"9999-12-31 00:00:00"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"boolean",
#                                                     "type":{
#                                                         "name":"Boolean",
#                                                         "boolean":{
                                                            
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"text",
#                                                     "type":{
#                                                         "name":"Text UTF-8",
#                                                         "text":{
#                                                             "encoding":"UTF-8"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"public_fk",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                         },
#                                         {
#                                         "name":"bgpqlcws",
#                                         "type":{
#                                             "name":"Struct",
#                                             "struct":{
#                                                 "fields":[
#                                                     {
#                                                     "name":"id",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"integer",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"float",
#                                                     "type":{
#                                                         "name":"Float64",
#                                                         "float":{
#                                                             "min":-1125899906842624.0,
#                                                             "max":1125899906842624.0
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"datetime",
#                                                     "type":{
#                                                         "name":"Datetime",
#                                                         "datetime":{
#                                                             "format":"%Y-%m-%d %H:%M:%S",
#                                                             "min":"01-01-01 00:00:00",
#                                                             "max":"9999-12-31 00:00:00"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"date",
#                                                     "type":{
#                                                         "name":"Datetime",
#                                                         "datetime":{
#                                                             "format":"%Y-%m-%d %H:%M:%S",
#                                                             "min":"01-01-01 00:00:00",
#                                                             "max":"9999-12-31 00:00:00"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"boolean",
#                                                     "type":{
#                                                         "name":"Boolean",
#                                                         "boolean":{
                                                            
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"text",
#                                                     "type":{
#                                                         "name":"Text UTF-8",
#                                                         "text":{
#                                                             "encoding":"UTF-8"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"public_fk",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                         },
#                                         {
#                                         "name":"tyzgsphn",
#                                         "type":{
#                                             "name":"Struct",
#                                             "struct":{
#                                                 "fields":[
#                                                     {
#                                                     "name":"id",
#                                                     "type":{
#                                                         "name":"Integer",
#                                                         "integer":{
#                                                             "min":"-9223372036854775808",
#                                                             "max":"9223372036854775807"
#                                                         }
#                                                     }
#                                                     },
#                                                     {
#                                                     "name":"text",
#                                                     "type":{
#                                                         "name":"Text UTF-8",
#                                                         "text":{
#                                                             "encoding":"UTF-8"
#                                                         }
#                                                     }
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                         }
#                                     ]
#                                 },
#                                 "properties":{
#                                     "public_fields":"[]"
#                                 }
#                             }
#                             }
#                         ]
#                     },
#                     "properties":{
#                         "public_fields":"[]"
#                     }
#                 }
#                 },
#                 {
#                 "name":"sarus_weights",
#                 "type":{
#                     "name":"Integer",
#                     "integer":{
#                         "min":"-9223372036854775808",
#                         "max":"9223372036854775807"
#                     }
#                 }
#                 },
#                 {
#                 "name":"sarus_is_public",
#                 "type":{
#                     "name":"Boolean",
#                     "boolean":{
                        
#                     }
#                 }
#                 },
#                 {
#                     "name": "sarus_privacy_unit",
#                     "type": {
#                     "name": "Optional",
#                     "optional": {
#                         "type": {
#                         "id": {
#                             "base": "STRING",
#                             "unique": false
#                         },
#                         "name": "Id",
#                         "properties": {}
#                         }
#                     },
#                     "properties": {}
#                     }
#             },
#             ]
#         }
#     },
#     "protected":{
#         "label":"data"
#     },
#     "properties":{
#         "primary_keys":"",
#         "max_max_multiplicity":"1",
#         "foreign_keys":""
#     }
#     }
#     '''
#     real_ds = Dataset.from_str(dataset, schema, '')

    # # sarus dataset:
    # dataset = '{"@type": "sarus_data_spec/sarus_data_spec.Dataset", "uuid": "10ba9d15c46403eb7b1eb5b872868b69", "name": "Transformed", "spec": {"transformed": {"transform": "98f18c2b0beb406088193dab26e24552", "arguments": [], "named_arguments": {}}}, "properties": {}, "doc": "This ia a demo dataset for testing purpose"}'
    # schema = '{"@type": "sarus_data_spec/sarus_data_spec.Schema", "uuid": "72c83138118ecbea709ae57f9cdf8a2c", "dataset": "10ba9d15c46403eb7b1eb5b872868b69", "name": "Transformed_schema", "type": {"name": "Struct", "struct": {"fields": [{"name": "sarus_data", "type": {"name": "Union", "union": {"fields": [{"name": "primary_table", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Id", "id": {"base": "INT32", "unique": true}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"base": "INT32", "max": "99"}, "properties": {"possible_values": "[30, 89, 12, 65, 31, 57, 36, 27, 18, 93, 77, 22, 23, 94, 11, 28, 74, 88, 9, 15, 80, 71, 17, 46, 7, 75, 33, 84, 96, 44, 5, 4, 50, 54, 34, 6, 85, 92, 62, 79, 42, 97, 45, 40, 73, 37, 0, 3, 29, 16, 82, 14, 51, 53, 25, 48, 32, 81, 41, 90, 64, 38, 66, 67, 95, 99, 86, 21, 43, 8, 72, 78, 13, 87, 39, 91, 52, 76, 68, 70, 2, 58, 69, 1, 63, 83, 55, 20, 49, 60, 61, 19, 35, 10, 59, 98, 56, 26, 47, 24]", "possible_values_length": "100"}}}, {"name": "float", "type": {"name": "Float64", "float": {"base": "FLOAT32", "min": 0.00011310506670270115, "max": 0.9992868304252625}, "properties": {"possible_values_length": "301"}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "1980-01-01 01:04:49", "max": "1980-01-31 23:23:22"}, "properties": {"possible_values_length": "301"}}}, {"name": "date", "type": {"name": "Date", "date": {"format": "%Y-%m-%d", "min": "1980-01-03", "max": "2000-01-01"}, "properties": {"possible_values_length": "301"}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {"min_length": "8", "possible_values_length": "301", "text_alphabet_name": "Simple", "text_char_set": "[48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]", "max_length": "8"}}}, {"name": "public_fk", "type": {"name": "Id", "id": {"base": "INT32", "reference": {"label": "sarus_data", "paths": [{"label": "primary_public_table", "paths": [{"label": "id"}]}]}}}}]}, "properties": {"merge_paths": "0", "sarus_is_public": "False"}}}, {"name": "secondary_table", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Id", "id": {"base": "INT32", "reference": {"label": "sarus_data", "paths": [{"label": "primary_table", "paths": [{"label": "id"}]}]}}}}, {"name": "integer", "type": {"name": "Integer", "integer": {"base": "INT32", "max": "99"}, "properties": {"possible_values": "[30, 89, 12, 65, 31, 57, 36, 27, 18, 93, 77, 22, 23, 94, 11, 28, 74, 88, 9, 15, 80, 71, 17, 46, 7, 75, 33, 84, 96, 44, 5, 4, 50, 54, 34, 6, 85, 92, 62, 79, 42, 97, 45, 40, 73, 37, 0, 3, 29, 16, 82, 14, 51, 53, 25, 48, 32, 81, 41, 90, 64, 38, 66, 67, 95, 99, 86, 21, 43, 8, 72, 78, 13, 87, 39, 91, 52, 76, 68, 70, 2, 58, 69, 1, 63, 83, 55, 20, 49, 60, 61, 19, 35, 10, 59, 98, 56, 26, 47, 24]", "possible_values_length": "100"}}}, {"name": "float", "type": {"name": "Float64", "float": {"base": "FLOAT32", "min": 0.00011310506670270115, "max": 0.9992868304252625}, "properties": {"possible_values_length": "301"}}}, {"name": "datetime", "type": {"name": "Datetime", "datetime": {"format": "%Y-%m-%d %H:%M:%S", "min": "1980-01-01 01:04:49", "max": "1980-01-31 23:23:22"}, "properties": {"possible_values_length": "301"}}}, {"name": "date", "type": {"name": "Date", "date": {"format": "%Y-%m-%d", "min": "1980-01-03", "max": "2000-01-01"}, "properties": {"possible_values_length": "301"}}}, {"name": "boolean", "type": {"name": "Boolean", "boolean": {}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {"max_length": "8", "text_alphabet_name": "Simple", "min_length": "8", "text_char_set": "[48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]", "possible_values_length": "301"}}}, {"name": "public_fk", "type": {"name": "Id", "id": {"base": "INT32", "reference": {"label": "sarus_data", "paths": [{"label": "secondary_public_table", "paths": [{"label": "id"}]}]}}}}]}, "properties": {"sarus_is_public": "False", "fks_for_merging": "[\\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSEQoPc2Vjb25kYXJ5X3RhYmxl\\", \\"CiRzYXJ1c19kYXRhX3NwZWMvc2FydXNfZGF0YV9zcGVjLlBhdGgSDwoNcHJpbWFyeV90YWJsZQ==\\"]", "merge_paths": "2"}}}, {"name": "primary_public_table", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Id", "id": {"base": "INT32", "unique": true}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {"text_char_set": "[50, 51, 52, 55, 65, 70, 73, 74, 76, 78, 79, 80, 81, 87, 89, 90]", "max_length": "8", "possible_values": "[\\"JPA2Z33I\\", \\"JAQLY7IO\\", \\"NFNZN2W4\\"]", "text_alphabet_name": "Simple", "min_length": "8", "possible_values_length": "3"}}}]}, "properties": {"merge_paths": "1", "sarus_is_public": "True"}}}, {"name": "secondary_public_table", "type": {"name": "Struct", "struct": {"fields": [{"name": "id", "type": {"name": "Id", "id": {"base": "INT32", "unique": true}}}, {"name": "text", "type": {"name": "Text UTF-8", "text": {"encoding": "UTF-8"}, "properties": {"text_alphabet_name": "Simple", "text_char_set": "[50, 51, 52, 55, 65, 70, 73, 74, 76, 78, 79, 80, 81, 87, 89, 90]", "possible_values": "[\\"JPA2Z33I\\", \\"JAQLY7IO\\", \\"NFNZN2W4\\"]", "possible_values_length": "3", "max_length": "8", "min_length": "8"}}}]}, "properties": {"merge_paths": "1", "sarus_is_public": "True"}}}]}, "properties": {"sarus_is_public": "False"}}}, {"name": "sarus_is_public", "type": {"name": "Boolean", "boolean": {}}}, {"name": "sarus_privacy_unit", "type": {"name": "Optional", "optional": {"type": {"name": "Id", "id": {"base": "STRING"}}}}}, {"name": "sarus_weights", "type": {"name": "Float64", "float": {"max": 1.7976931348623157e308}}}]}}, "properties": {"max_max_multiplicity": "10"}}'

    # sarus_ds = Dataset.from_str(dataset, schema, size)

    # query = 'SELECT * FROM primary_table'
    # rel = Relation.from_query(query, sarus_ds)
    # display_graph(rel.dot())

    # tab_mapping = {
    #     ('Transformed_schema', 'primary_public_table'): ('Transformed_schema', 'st51_bicdlwoy', 'bgpqlcws'),
    #     ('Transformed_schema', 'primary_table'): ('Transformed_schema', 'st51_bicdlwoy', 'qqnhlkqe')
    # }
    # real_relations_dict = {tuple(path): rel for (path, rel) in real_ds.relations()}
    # # sarus_relations_dict = dict(sarus_ds.relations())
    # renamed_relations = [(sarus_path, real_relations_dict[real_path]) for sarus_path, real_path in tab_mapping.items()]

    # renamed = rel.compose(renamed_relations)
    # display_graph(renamed.dot())
