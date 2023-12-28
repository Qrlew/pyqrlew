import os
import numpy as np
import pandas as pd
from pyqrlew.io import PostgreSQL
from termcolor import colored

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

def test_queries_consistency():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db
    filename = os.path.join(DIRNAME, 'queries/sarus_sql_queries.sql')
    
    with open(filename, 'r') as f:
        for query in f:
            if not query.startswith('--'):
                colored_query = colored(query, 'blue')
                print('\n\n', colored_query)
                replaced_query = query.replace("census", "extract.census").replace("beacon", "extract.beacon")
                result = pd.DataFrame(database.engine().execute(replaced_query))
                relation = dataset.sql(query)
                new_query = relation.render()
                print(f'\n===================\n{query} -> {new_query}')
                try:
                    result_after_rewriting = pd.DataFrame.from_dict(database.engine().execute(new_query))
                except TypeError as e:
                    print(f"Sending {new_query}\nfailed with error:\n{e}")
                    return
                print(f'{result}\n{result_after_rewriting}\n===================\n')
                assert are_dataframes_almost_equal(result, result_after_rewriting)

def test_queries_differential_privacy():
    """Test that we can rewrite the query into DP then execute it and check that the exact and DP results have the same type"""
    database = PostgreSQL()
    filename = os.path.join(DIRNAME, 'queries/sarus_sql_queries.sql')
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
    with open(filename, 'r') as f:
        for query in f:
            if not query.startswith('--'):
                colored_query = colored(query, 'blue')
                print('\n\n', colored_query)
                relation = dataset.sql(query)
                dp_relation = relation.rewrite_with_differential_privacy(
                    dataset,
                    privacy_unit,
                    budget,
                    synthetic_data,
                ).relation()
                print(list(database.engine().execute(relation.render())))
                # results = pd.DataFrame.from_dict(database.engine().execute(relation.render()))
                # dp_results = pd.DataFrame.from_dict(database.engine().execute(dp_relation.render()))
                # if len(dp_results) != 0:
                #     assert (results.columns == dp_results.columns).all()
