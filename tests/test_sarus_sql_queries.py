import typing as t
import os
from qrlew_datasets.database import Database
from pyqrlew.io import PostgreSQL
from termcolor import colored
import pandas as pd
import numpy as np

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))

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

def _test_query_consistency(
        database: Database,
        query: str
    ):
    """Verify that the original query and the rewritten query (after `Relation` translation)
    produce the same result """
    result = pd.DataFrame(database.execute(query.replace("census", "extract.census").replace("beacon", "extract.beacon")))
    relation = database.extract().sql(query)
    new_query = relation.render()

    print(f'\n===================\n{query} -> {new_query}')

    try:
        result_after_rewriting = pd.DataFrame(database.execute(new_query))
    except TypeError as e:
        print(f"Sending {new_query}\nfailed with error:\n{e}")
        return
    print(f'{result}\n{result_after_rewriting}\n===================\n')
    assert are_dataframes_almost_equal(result, result_after_rewriting)

def _test_differential_privacy(
        database: Database,
        dataset: PostgreSQL,
        query: str,
        privacy_unit: t.List[t.Tuple[str, t.List[t.Tuple[str, str,str]], str]],
        budget: t.Dict[str, float],
        synthetic_data: t.List[t.Tuple[str]]
    ):
    """Verify that we know how to rewrite the query into DP and that these 2 queries produce results with the same type"""
    relation = dataset.sql(query)
    dp_relation = relation.rewrite_with_differential_privacy(
        dataset,
        synthetic_data,
        privacy_unit,
        budget
    ).relation()
    results = pd.DataFrame(database.eval(relation))
    dp_results = pd.DataFrame(database.eval(dp_relation))
    if len(dp_results) != 0:
        assert (results.columns == dp_results.columns).all()

def test_query_consistency():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    _ = database.extract() # load the db
    filename = os.path.join(DIRNAME, 'queries/sarus_sql_queries.sql')
    with open(filename, 'r') as f:
        for query in f:
            if not query.startswith('--'):
                colored_query = colored(query, 'blue')
                print('\n\n', colored_query)
                _test_query_consistency(database, query)

def test_differential_privacy():
    """Test that we can rewrite the query into DP then execute it and check that the exact and DP results have the same type"""
    database = PostgreSQL()
    filename = os.path.join(DIRNAME, 'queries/sarus_sql_queries.sql')
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_"),
        ("beacon", [], "_PRIVACY_UNIT_ROW_"),
    ]
    # Other arguments that will be explained later
    budget = {"epsilon": 1.0, "delta": 5e-4}
    synthetic_data = [
        (["extract", "census"], ["extract", "census"]),
        (["extract", "beacon"], ["extract", "beacon"]),
    ]
    with open(filename, 'r') as f:
        for query in f:
            if not query.startswith('--'):
                colored_query = colored(query, 'blue')
                print('\n\n', colored_query)
                _test_differential_privacy(
                    database=database,
                    dataset=database.extract(),
                    query=query,
                    privacy_unit=privacy_unit,
                    budget=budget,
                    synthetic_data= synthetic_data
                )

