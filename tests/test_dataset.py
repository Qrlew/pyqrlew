import os
import numpy as np
import pandas as pd
from pyqrlew.io import PostgreSQL
from pyqrlew import Dialect, Dataset

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
