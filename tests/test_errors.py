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
        rel = Relation.from_query(query, extract_dataset)
        print(e_info)

    query = "wrong query"
    with pytest.raises(RuntimeError) as e_info:
        rel = Relation.from_query(query, extract_dataset)
        print(e_info)

    query = "SELECT not_existing_col, COUNT(*) FROM  extract.census"
    with pytest.raises(RuntimeError) as e_info:
        rel = Relation.from_query(query, extract_dataset)
        print(e_info)

def test_wrong_settings_for_pup_rewriting():
    pass

def test_wrong_settings_for_dp_rewriting():
    pass
