from pyqrlew.io import PostgreSQL
from pyqrlew import Relation, Dialect, Strategy
from pyqrlew.utils import display_graph
import pytest


@pytest.fixture
def extract_dataset():
    database = PostgreSQL()
    dataset = database.extract()
    return dataset


def test_from_query(extract_dataset):
    query = "SELECT * FROM extract.census"
    _ = Relation.from_query(query, extract_dataset)
    _ = Relation.from_query(query, extract_dataset, dialect=Dialect.PostgreSql)
    _ = Relation.from_query(query, extract_dataset, dialect=Dialect.BigQuery)
    _ = Relation.from_query(query, extract_dataset, dialect=Dialect.MsSql)
    

def test_rewrite_as_privacy_unit_preserving_soft(extract_dataset):
    query = "SELECT * FROM extract.census"
    rel = Relation.from_query(query, extract_dataset)
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_")
    ]
    epsilon_delta={"epsilon": 1.0, "delta": 1e-3}
    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )
    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Soft
    )

    privacy_unit = ([
        ("census", [], "age")
    ], False)

    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Soft
    )

    privacy_unit = ([
        ("census", [], "age", "education_num")
    ], False)

    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Soft
    )

def test_rewrite_as_privacy_unit_preserving_hard(extract_dataset):
    query = "SELECT age, COUNT(*) FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_")
    ]
    epsilon_delta={"epsilon": 1.0, "delta": 1e-3}
    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )
    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Hard
    )

    privacy_unit = ([
        ("census", [], "age")
    ], False)

    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Hard
    )

    privacy_unit = ([
        ("census", [], "age", "education_num")
    ], False)

    _ = rel.rewrite_as_privacy_unit_preserving(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
        strategy=Strategy.Hard
    )

    with pytest.raises(RuntimeError) as e_info:
        _ = rel.rewrite_as_privacy_unit_preserving(
            dataset=extract_dataset,
            synthetic_data=None,
            privacy_unit=privacy_unit,
            epsilon_delta=epsilon_delta,
            strategy=Strategy.Soft
        )
        assert e_info == "UnreachableProperty: privacy_unit_preserving is unreachable"


def rewrite_with_differential_privacy(extract_dataset):
    query = "SELECT age, COUNT(*) FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_")
    ]
    epsilon_delta={"epsilon": 1.0, "delta": 1e-3}
    _ = rel.rewrite_with_differential_privacy(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )

    privacy_unit = ([
        ("census", [], "age")
    ], False)

    _ = rel.rewrite_with_differential_privacy(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )

    privacy_unit = ([
        ("census", [], "age", "education_num")
    ], False)

    _ = rel.rewrite_with_differential_privacy(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )

def test_rename_fields(extract_dataset):
    query = "SELECT age AS age, COUNT(*) AS count_all FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    fields = [("age", "renamed_age"), ("count_all", "renamed_count_all")]
    rel = rel.rename_fields(fields)
    assert rel.schema() == '{renamed_age: int[20 90] (UNIQUE), renamed_count_all: int[0 199]}'
    # display_graph(rel.dot())


def test_compose(extract_dataset):
    queries = [
        (("dataset_name", "my_schema", "boomers",), "SELECT * FROM extract.census WHERE age >= 60"),
        (("dataset_name", "my_schema", "genx",), "SELECT * FROM extract.census WHERE age >= 40 AND age < 60"),
        (("dataset_name", "my_schema", "millenials",), "SELECT * FROM extract.census WHERE age >= 30 AND age < 40"),
        (("dataset_name", "my_schema", "genz",), "SELECT * FROM extract.census WHERE age < 30"),
    ]
    relations = [
        (path, Relation.from_query(query, extract_dataset)) for (path, query) in queries
    ]

    new_ds = extract_dataset.from_queries(queries)
    new_query = """SELECT age, marital_status, COUNT(*) AS count_all
        FROM genx
        GROUP BY age, marital_status
        ORDER BY age, marital_status 
    """
    rel = Relation.from_query(new_query, new_ds)

    composed = rel.compose(relations)
    assert composed.schema() == '{age: int[40 60], marital_status: str{Divorced, Married-civ-spouse, Married-spouse-absent, Never-married, None, Separated, Widowed}, count_all: int[0 199]}'
    # display_graph(composed.dot())


def test_with_filed(extract_dataset):
    query = "SELECT age AS age, COUNT(*) AS count_all FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    new_field_name = "custom_field"
    new_field_expr = "1.0"
    new_rel = rel.with_field(name=new_field_name, expr=new_field_expr)
    assert new_rel.schema() == '{custom_field: float{1}, age: int[20 90] (UNIQUE), count_all: int[0 199]}'
    # display_graph(new_rel.dot())

