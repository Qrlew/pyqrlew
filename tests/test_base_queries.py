import os
import numpy as np
import pandas as pd
from pyqrlew.io import PostgreSQL
from termcolor import colored
from pyqrlew import Dialect

def are_dataframes_almost_equal(df1, df2, float_tolerance=1e-6):
    # Check if the shape is the same
    if df1.shape != df2.shape:
        print("Shapes are not equal")
        return False

    # Check if column names are the same
    if not df1.columns.equals(df2.columns):
        print("Columns are not the same")
        return False

    # Replace NaN values with a placeholder for comparison
    df1 = df1.fillna('nan_placeholder')
    df2 = df2.fillna('nan_placeholder')

    # Check if values are almost equal (considering float tolerance)
    numeric_columns = df2.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        if not np.all(np.isclose(
            df1[col].astype(float),
            df2[col].astype(float),
            rtol=float_tolerance
        )):
            print(f"In {col}, \n{list(df1[col])} \n!= \n{list(df2[col])}")
            return False

    # Check if non-numeric values are equal
    non_numeric_columns = df2.select_dtypes(exclude=[np.number]).columns
    for col in non_numeric_columns:
        if not df1[col].equals(df2[col]):
            print(f"In {col}, \n{list(df1[col])} \n!= \n{list(df2[col])}")
            print(f"type: {df1[col].dtype} {df2[col].dtype}")
            return False

    return True

def test_queries_consistency(queries):
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db

    for query in queries:
        print(f"\n{colored(query, 'blue')}")
        replaced_query = query.replace("census", "extract.census").replace("beacon", "extract.beacon")
        result = pd.read_sql(replaced_query, database.engine())            
        relation = dataset.relation(query)
        new_query = relation.to_query()
        print('===================')
        print(f"{colored(query, 'red')} -> {colored(new_query, 'green')}")
        try:
            result_after_rewriting = pd.read_sql(new_query, database.engine())
        except TypeError as e:
            print(f"Sending {new_query}\nfailed with error:\n{e}")
            return
        print(f'{result}')
        print(f'{result_after_rewriting}')
        print('===================')
        assert are_dataframes_almost_equal(result, result_after_rewriting)

def test_queries_differential_privacy(queries):
    """Test that we can rewrite the query into DP then execute it and check that the exact and DP results have the same type"""
    database = PostgreSQL()
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_"),
        ("beacon", [], "_PRIVACY_UNIT_ROW_"),
    ]
    budget = {"epsilon": 1.0, "delta": 5e-4}
    synthetic_data = [
        (["extract", "census"], ["extract", "census"]),
        (["extract", "beacon"], ["extract", "beacon"]),
    ]
    dataset = database.extract()
    for query in queries:
        print(f"{colored(query, 'blue')}")
        relation = dataset.relation(query)
        dp_relation = relation.rewrite_with_differential_privacy(
            dataset,
            privacy_unit,
            budget,
            None,
            None,
            synthetic_data,
        ).relation()
        results = pd.read_sql(relation.to_query(), database.engine())
        dp_results = pd.read_sql(dp_relation.to_query(), database.engine())
        if len(dp_results) != 0:
            assert (results.columns == dp_results.columns).all()


def test_simple_mssql_translation():
    database = PostgreSQL()
    dataset = database.extract()
    query = "SELECT * FROM census LIMIT 10;"
    relation = dataset.relation(query, Dialect.PostgreSql)
    translated = relation.to_query(Dialect.MsSql)
    assert "SELECT TOP (10) * FROM" in translated


def test_quoting():
    database = PostgreSQL()
    dataset = database.extract()
    query = """
    SELECT 
        age AS "my age"
    FROM census;"""
    relation = dataset.relation(query, Dialect.PostgreSql)
    translated = relation.to_query(Dialect.PostgreSql)
    assert translated.replace(' ', '')=='''
        WITH "map_p5al" ("my age") AS (
            SELECT "age" AS "my age" FROM "extract"."census"
        ) SELECT * FROM "map_p5al"
    '''.replace('\n', '').replace(' ', '')
    results = pd.read_sql(relation.to_query(), database.engine())

