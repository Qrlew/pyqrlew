import os
from pyqrlew.io import PostgreSQL
from termcolor import colored

from utils import test_query_consistency as _test_query_consistency, test_differential_privacy as _test_differential_privacy

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))


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
                _test_query_consistency(
                    database,
                    query,
                    database.extract(),
                    [("census", "extract.census"), ("beacon", "extract.beacon")]
                )


def test_differential_privacy():
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

