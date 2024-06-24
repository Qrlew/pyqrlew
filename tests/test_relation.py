from pyqrlew.io import PostgreSQL
from pyqrlew import Relation, Dialect, Strategy
from pyqrlew.utils import display_graph
from pyqrlew.wrappers import Dataset
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


def test_rewrite_with_differential_privacy(extract_dataset):
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
    fields = [("age", "renamed_age")]
    rel = rel.rename_fields(fields)
    assert rel.schema() == '{renamed_age: int[20 90] (UNIQUE), count_all: int[0 199]}'
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
    # display_graph(rel.dot())
    composed = rel.compose(relations)
    assert composed.schema() == '{age: int[40 60], marital_status: str{Divorced, Married-civ-spouse, Married-spouse-absent, Never-married, None, Separated, Widowed}, count_all: int[0 199]}'
    # display_graph(composed.dot())

def test_with_filed(extract_dataset):
    query = "SELECT age AS age, COUNT(*) AS count_all FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    new_field_name = "custom_field"
    new_field_expr = "false"
    new_rel = rel.with_field(name=new_field_name, expr=new_field_expr)
    assert new_rel.schema() == '{custom_field: bool{false}, age: int[20 90] (UNIQUE), count_all: int[0 199]}'

    new_field_name = "custom_null_field"
    new_field_expr = "NULL"
    new_rel = new_rel.with_field(name=new_field_name, expr=new_field_expr)
    assert new_rel.schema() == '{custom_null_field: option(any), custom_field: bool{false}, age: int[20 90] (UNIQUE), count_all: int[0 199]}'
    # display_graph(new_rel.dot())

def test_extra_queries(tables, engine):
    ds = Dataset.from_database("my_db", engine, schema_name=None)
    print(ds.relations())
    QUERIES = [
        ('SELECT "integer" FROM primary_table, primary_public_table'),
        (
            'SELECT "integer" FROM primary_table '
            "JOIN (SELECT * FROM primary_public_table) AS subq USING(id)"
        ),
        (
            """WITH tab1 AS (
                SELECT "integer" FROM primary_table
                JOIN primary_public_table USING(id)
            ),
            tab2 AS (
                SELECT * FROM primary_table JOIN secondary_table USING(id)
            )
            SELECT * FROM tab1
            """
        ),
        "SELECT t1.id FROM primary_public_table AS t1 JOIN secondary_public_table AS t2 ON(t1.id=t2.id)",
        """
        WITH "join_nagp" ("field_ljc8", "field_xkq4", "field_rv0k", "field_xn3k", "field_3z4q", "field_o2t5", "field_xfrs", "field_pqun", "field_90w5", "field_9tue") AS (
            SELECT * FROM "primary_public_table" AS "_LEFT_" JOIN "secondary_public_table" AS "_RIGHT_" ON ("_LEFT_"."id") = ("_RIGHT_"."id")
        ), "map_w9s_" ("_PRIVACY_UNIT_WEIGHT_", "_PRIVACY_UNIT_", "sarus_is_public", "id") AS (
            SELECT CAST(1 AS INTEGER) AS "_PRIVACY_UNIT_WEIGHT_", CAST(CASE WHEN (1) > (2) THEN 1 ELSE NULL END AS TEXT) AS "_PRIVACY_UNIT_", CAST(0 AS BOOLEAN) AS "sarus_is_public", "field_ljc8" AS "id"
            FROM "join_nagp"
        ) SELECT * FROM "map_w9s_"
        """,
    ]

    for query in QUERIES:
        rel = Relation.from_query(query, ds)
        query = rel.to_query()
        print(query)
        # display_graph(rel.dot())


def test_writing_and_rewriting_many_times(tables, engine):
    import sqlalchemy as sa
    ds = Dataset.from_database("my_db", engine, schema_name=None)
    query = """SELECT * FROM primary_table t1 JOIN primary_public_table t2 ON (t1.id=t2.id)"""

    with engine.connect() as conn:
        conn.execute(sa.text(query))

    for i in range(2):
        rel = Relation.from_query(query, ds)
        query = rel.to_query()
        print(query)
        with engine.connect() as conn:
            conn.execute(sa.text(query))
        # display_graph(rel.dot())
