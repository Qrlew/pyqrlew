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
    for path, rel in extract_dataset.relations():
        display_graph(rel.dot())
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
    print(new_ds.schema)
    new_query = """SELECT age, marital_status, COUNT(*) AS count_all
        FROM genx
        GROUP BY age, marital_status
        ORDER BY age, marital_status 
    """
    rel = Relation.from_query(new_query, new_ds)
    display_graph(rel.dot())
    composed = rel.compose(relations)
    assert composed.schema() == '{age: int[40 60], marital_status: str{Divorced, Married-civ-spouse, Married-spouse-absent, Never-married, None, Separated, Widowed}, count_all: int[0 199]}'
    display_graph(composed.dot())


def test_renaming(extract_dataset):
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
    display_graph(rel.dot())
    composed = rel.compose(relations)
    assert composed.schema() == '{age: int[40 60], marital_status: str{Divorced, Married-civ-spouse, Married-spouse-absent, Never-married, None, Separated, Widowed}, count_all: int[0 199]}'
    display_graph(composed.dot())

def test_with_filed(extract_dataset):
    query = "SELECT age AS age, COUNT(*) AS count_all FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    new_field_name = "custom_field"
    new_field_expr = "cast(0 as boolean)"
    new_rel = rel.with_field(name=new_field_name, expr=new_field_expr)
    assert new_rel.schema() == '{custom_field: bool{false}, age: int[20 90] (UNIQUE), count_all: int[0 199]}'
    # display_graph(new_rel.dot())



def test_dp_event(extract_dataset):
    query = "SELECT age, COUNT(*) FROM extract.census GROUP BY age"
    rel = Relation.from_query(query, extract_dataset)
    privacy_unit = [
        ("census", [], "_PRIVACY_UNIT_ROW_")
    ]
    epsilon_delta={"epsilon": 1.0, "delta": 1e-3}
    relwithdp = rel.rewrite_with_differential_privacy(
        dataset=extract_dataset,
        synthetic_data=None,
        privacy_unit=privacy_unit,
        epsilon_delta=epsilon_delta,
    )

    dp_event = relwithdp.dp_event()
    _ = dp_event.to_dict()



def test_some_queries(tables, engine):
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
        display_graph(rel.dot())


def test_writing_and_rewriting_many_times(tables, engine):
    import sqlalchemy as sa
    ds = Dataset.from_database("my_db", engine, schema_name=None)
    query = """
WITH
  "map_6qs7" (
    "id",
    "integer",
    "float",
    "datetime",
    "date",
    "boolean",
    "text",
    "public_fk"
  ) AS (
    SELECT
      "id" AS "id",
      "integer" AS "integer",
      "float" AS "float",
      "datetime" AS "datetime",
      "date" AS "date",
      "boolean" AS "boolean",
      "text" AS "text",
      "public_fk" AS "public_fk"
    FROM
      "primary_table"
  ),
  "original_table" (
    "id",
    "integer",
    "float",
    "datetime",
    "date",
    "boolean",
    "text",
    "public_fk",
    "sarus_is_public",
    "sarus_privacy_unit",
    "sarus_weights"
  ) AS (
    SELECT
      "id" AS "id",
      "integer" AS "integer",
      "float" AS "float",
      "datetime" AS "datetime",
      "date" AS "date",
      "boolean" AS "boolean",
      "text" AS "text",
      "public_fk" AS "public_fk",
      0 AS "sarus_is_public",
      MD5(
        CONCAT(
          MD5(
            CONCAT(
              '3c7787272a91a5753234666ca398ba8d3cf6d9ff6dda137886ea9a77a3d19f7b',
              MD5(CAST(RANDOM() AS TEXT))
            )
          ),
          MD5(
            CONCAT(
              '0e8b99dba4369b3265b0b9e1337d3ae04d70881174bce77685e75fd9b46e23e0'
            )
          ),
          MD5(
            CONCAT(
              '17bbc4d13311f67df8bc1e832c9a5a082f61c28be77e933c7759c9d70b343b86'
            )
          ),
          MD5(
            CONCAT(
              '2f4f7d6eaa60d8aaa2b102efb6054a94e76aa4020776c63b6f5ab230eec43e1b'
            )
          )
        )
      ) AS "sarus_privacy_unit",
      1 AS "sarus_weights"
    FROM
      "map_6qs7"
  ),
  "map_j11u" ("sarus_privacy_unit") AS (
    SELECT
      "sarus_privacy_unit" AS "sarus_privacy_unit"
    FROM
      "original_table"
  ),
  "map_xtgv" ("field_cbf0") AS (
    SELECT
      "sarus_privacy_unit" AS "field_cbf0"
    FROM
      "map_j11u"
  ),
  "reduce_zcd_" ("field_dydc") AS (
    SELECT
      "field_cbf0" AS "field_dydc"
    FROM
      "map_xtgv"
    GROUP BY
      "field_cbf0"
  ),
  "map_dpy6" ("sarus_privacy_unit") AS (
    SELECT
      "field_dydc" AS "sarus_privacy_unit"
    FROM
      "reduce_zcd_"
  ),
  "map_oy_e" ("sarus_privacy_unit") AS (
    SELECT
      "sarus_privacy_unit" AS "sarus_privacy_unit"
    FROM
      "map_dpy6"
  ),
  "map_uyvg" ("sarus_privacy_unit") AS (
    SELECT
      "sarus_privacy_unit" AS "sarus_privacy_unit"
    FROM
      "map_oy_e"
  ),
  "sampled_table" ("sarus_privacy_unit") AS (
    SELECT
      "sarus_privacy_unit" AS "sarus_privacy_unit"
    FROM
      "map_uyvg"
    ORDER BY
      RANDOM() ASC
    LIMIT
      90
  )
    SELECT
      "_LEFT_"."id" AS "id",
      "_LEFT_"."integer" AS "integer",
      "_LEFT_"."float" AS "float",
      "_LEFT_"."datetime" AS "datetime",
      "_LEFT_"."date" AS "date",
      "_LEFT_"."boolean" AS "boolean",
      "_LEFT_"."text" AS "text",
      "_LEFT_"."public_fk" AS "public_fk",
      "_LEFT_"."sarus_is_public" AS "sarus_is_public",
      "_LEFT_"."sarus_privacy_unit" AS "sarus_privacy_unit",
      "_LEFT_"."sarus_weights" AS "sarus_weights"
    FROM
      "original_table" AS "_LEFT_"
      JOIN "sampled_table" AS "_RIGHT_" ON ("_LEFT_"."sarus_privacy_unit") = ("_RIGHT_"."sarus_privacy_unit")
"""

    with engine.connect() as conn:
        conn.execute(sa.text(query))

    for i in range(2):
        rel = Relation.from_query(query, ds)
        query = rel.to_query()
        print(query)
        with engine.connect() as conn:
            conn.execute(sa.text(query))
        display_graph(rel.dot())
