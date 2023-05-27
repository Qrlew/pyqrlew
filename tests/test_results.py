import sqlalchemy
import typing as t
import os
from termcolor import colored

import pyqrlew as qrl

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))
FILENAME = os.path.join(DIRNAME, 'queries/valid_queries.sql')


def execute_query(url: str, query: str) -> t.List[t.Dict[str, t.Any]]:
    engine = sqlalchemy.create_engine(url + 'postgres')
    conn = engine.connect()
    str_query = sqlalchemy.text(query)
    res = conn.execute(str_query)
    return [dict(row) for row in res.fetchall()]


def check_query(dataset: qrl.Dataset, url: str, query: str) -> None:
    result = execute_query(url, query)
    rel = dataset.sql(query)
    new_query = rel.render()
    print(f"{query}->{new_query}")
    try:
        result2 = execute_query(url, new_query)
    except TypeError as e:
        print(
            f"Sending {new_query} \nfailed with error:\n{e}"
        )
        return
    assert result == result2
    return


def test_results(postgres_url, dataset):
    with open(FILENAME, 'r') as f:
        for query in f:
            if query.startswith('--'):
                continue
            print('\n\n', colored(query, 'blue'))
            check_query(dataset, postgres_url, query)
