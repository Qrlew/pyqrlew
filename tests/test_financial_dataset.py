from sqlalchemy import Engine, Connection, text
import typing as t
import os
from termcolor import colored

import pyqrlew as qrl

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))
FILENAME = os.path.join(DIRNAME, 'queries/valid_queries.sql')

def execute_query(
    connection: Connection,
    query: str
) -> t.List[t.Dict[str, t.Any]]:
    connection.execute(text("SET search_path TO extract"))
    str_query = text(query)
    res = connection.execute(str_query)
    names = res.keys()
    return [
        {name:col for name, col in zip(names, row)}
        for row in res
    ]


def check_query(
    dataset: qrl.Dataset,
    connection: Connection,
    query: str
) -> None:
    result = execute_query(connection, query)
    rel = dataset.sql(query)
    new_query = rel.render()
    print(f"{query}->{new_query}")
    try:
        result2 = execute_query(connection, new_query)
    except TypeError as e:
        print(
            f"Sending {new_query} \nfailed with error:\n{e}"
        )
        return
    assert result == result2
    return


def test_results(database):
    engine = database.engine()
    with engine.connect() as conn:
        with open(FILENAME, 'r') as f:
            for query in f:
                if not query.startswith('--'):
                    print('\n\n', colored(query, 'blue'))
                    check_query(database.extract(), conn, query)
