import typing as t
import os
from qrlew_datasets.database import Database
from pyqrlew.io import PostgreSQL
from termcolor import colored
import pandas as pd

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))

def _test_query_consistency(
        database: Database,
        query: str
    ):
    """
    Verify that the original query and the rewritten query (after `Relation` translation)
    produce the same result.

    Args:
        database (Database): The database object.
        query (str): The original SQL query.

    Returns:
        None: The function does not return any value but raises an assertion error if the
        results of the original and rewritten queries are not equal.
    """
    result = database.execute(query.replace("census", "extract.census").replace("beacon", "extract.beacon"))
    relation = database.extract().sql(query)
    new_query = relation.render()

    print(f'\n===================\n')
    print(f"{query} -> {new_query}")

    try:
        result_after_rewriting = database.execute(new_query)
    except TypeError as e:
        print(f"Sending {new_query}\nfailed with error:\n{e}")
        return
    print(pd.DataFrame(result))
    print(pd.DataFrame(result_after_rewriting))
    print('\n===================\n')
    assert result == result_after_rewriting

def _test_differential_privacy(
        database: Database,
        dataset: PostgreSQL,
        query: str,
        privacy_unit: t.List[t.Tuple[str, t.List[t.Tuple[str, str,str]], str]],
        budget: t.Dict[str, float],
        synthetic_data: t.List[t.Tuple[str]]
    ):
    """TODO"""
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
    """TODO"""
    database = PostgreSQL()
    filename = os.path.join(DIRNAME, 'queries/dp_sarus_sql_queries.sql')
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

