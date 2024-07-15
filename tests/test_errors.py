from pyqrlew.io.postgresql import PostgreSQL
from pyqrlew.wrappers import Relation
import pytest


@pytest.fixture
def extract_dataset():
    database = PostgreSQL()
    dataset = database.extract()
    return dataset


def test_wrong_query(extract_dataset):
    query = "SELECT age, COUNT(*) FROM not_existing_table GROUP BY age"
    with pytest.raises(RuntimeError) as e_info:
        _ = Relation.from_query(query, extract_dataset)

    query = "wrong query"
    with pytest.raises(RuntimeError) as e_info:
        _ = Relation.from_query(query, extract_dataset)

    # this panics, issue: https://github.com/Qrlew/qrlew/issues/287
    # query = "SELECT not_existing_col, COUNT(*) FROM  extract.census"
    # with pytest.raises(RuntimeError) as e_info:
    #     rel = Relation.from_query(query, extract_dataset)
    #     print(e_info)


