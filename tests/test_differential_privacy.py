import os
from qrlew_datasets.database import Database
from pyqrlew.io import PostgreSQL
from termcolor import colored
import pandas as pd

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))

def _test_differential_privacy(
        database: Database,
        query: str
    ):
    """
    TODO:
    Verify that the original query and the rewritten query (after `Relation` translation)
    produce the same result.

    Args:
        database (Database): The database object.
        query (str): The original SQL query.

    Returns:
        None: The function does not return any value but raises an assertion error if the
        results of the original and rewritten queries are not equal.
    """
    dataset = database.financial()
    relation = dataset.sql(query)
    privacy_unit = [
        # account: public
        ("card", [("disp_id", "disp", "disp_id")], "client_id"),
        ("client", [], "client_id"),
        ("disp", [], "client_id"),
        ("district", [("district_id", "client", "district_id")], "client_id"),
        ("loan", [], "loan_id"),
        ("order", [], "order_id"),
        ("trans", [], "trans_id"),
    ]
    # Other arguments that will be explained later
    budget = {"epsilon": 1.0, "delta": 5e-4}
    synthetic_data = [
        (["financial", "account"], ["financial", "account"]),
        (["financial", "card"], ["financial", "card"]),
        (["financial", "client"], ["financial", "client"]),
        (["financial", "disp"], ["financial", "disp"]),
        (["financial", "district"], ["financial", "district"]),
        (["financial", "loan"], ["financial", "loan"]),
        (["financial", "order"], ["financial", "order"]),
        (["financial", "trans"], ["financial", "trans"]),
    ]
    dp_relation = relation.rewrite_with_differential_privacy(
        dataset,
        synthetic_data,
        privacy_unit,
        budget
    ).relation()
    results = pd.DataFrame(database.eval(relation))
    dp_results = pd.DataFrame(database.eval(dp_relation))
    assert (results.columns == dp_results.columns).all()

def test_differential_privacy():
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    filename = os.path.join(DIRNAME, 'queries/dp_queries.sql')
    with open(filename, 'r') as f:
        for query in f:
            if not query.startswith('--'):
                colored_query = colored(query, 'blue')
                print('\n\n', colored_query)
                _test_differential_privacy(database, query)

