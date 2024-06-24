from pathlib import Path
import string
from typing import List
from pyqrlew.io.postgresql import PostgreSQL
import pytest
from sqlalchemy import (
    Column,
    ForeignKey,
    MetaData,
    Table,
    create_engine,
    types,
    text
)
import pandas as pd
import numpy as np

np.random.seed(10)
LENGTH_1 = 900
LENTGH_2 = 100
LENGTH_PUBLIC = 3

@pytest.fixture
def tests_path() -> Path:
    return Path(__file__).parent

@pytest.fixture
def queries_path(tests_path) -> Path:
    """Provide the path to test queries"""
    return tests_path / 'queries' / 'base_queries.sql'

@pytest.fixture
def queries(queries_path) -> List[str]:
    with open(queries_path, 'r') as f:
        return [query for query in f if not query.startswith('--')]

@pytest.fixture(scope="session", autouse=True)
def engine():
    database = PostgreSQL()
    return database.engine()


@pytest.fixture(scope="session", autouse=True)
def metadata():
    output = MetaData()
    return output


@pytest.fixture(scope="session", autouse=True)
def simple_tables(metadata, engine):
    tab_A = Table(
        "table_A",
        metadata,
        Column("a1", types.String, primary_key=True),
        Column("a2", types.Integer, nullable=True),
        Column("a3", types.Float, nullable=False),
        Column("a4", types.String, ForeignKey("table_C.c1", ondelete='CASCADE'), nullable=False),
    )
    tab_B = Table(
        "table_B",
        metadata,
        Column("b1", types.String, primary_key=True),
        Column("b2", types.String, ForeignKey("table_A.a1", ondelete='CASCADE'), nullable=True),
    )
    tab_D = Table(  # table to ignore
        "table_D",
        metadata,
        Column("d1", types.String, primary_key=True),
    )
    tab_C = Table(
        "table_C",
        metadata,
        Column("c1", types.String, primary_key=True),
        Column(
            "c2",
            types.String,
            ForeignKey("table_D.d1", ondelete='CASCADE'),
            nullable=True,
        ),
    )
    tab_E = Table(
        "table_E",
        metadata,
        Column("e1", types.Integer, primary_key=True),
    )

    tab_Abis = Table(
        "table_Abis",
        metadata,
        Column("a4", types.String, nullable=False),
        Column("a1", types.String),
        Column("a2", types.Integer, nullable=True),
        Column("a3", types.Float, nullable=False),
    )

    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "table_A", "table_B", "table_D", "table_C", "table_E", "table_Abis" CASCADE'))

    metadata.create_all(engine)
    df_d = pd.DataFrame(
        {
            "d1": ["Paris", "Rome", "Madrid"],
        }
    )
    df_d.to_sql(name="table_D", con=engine, if_exists="append", index=False)
    df_c = pd.DataFrame(
        {
            "c1": ["France", "Italy", "Spain"],
            "c2": ["Paris", "Rome", "Madrid"],
        }
    )
    df_c.to_sql(name="table_C", con=engine, if_exists="append", index=False)
    df_A = pd.DataFrame(
        {
            "a1": ["User1", "User2", "User3"],
            "a2": [3, 5, 9],
            "a3": [6.0, 10.0, 8.0],
            "a4": ["Spain", "Italy", "Italy"],
        }
    )
    df_A.to_sql(name="table_A", con=engine, if_exists="append", index=False)
    df_Abis = pd.DataFrame(
        {
            "a4": ["Spain", "Italy", "Italy", "Spain", "Spain"],
            "a1": ["User1", "User1", "User1", "User2", "User2"],
            "a2": [3, 5, 9, None, 10],
            "a3": [16.0, 110.0, 88.0, 99.0, 231.0],
        }
    )
    df_Abis.to_sql(
        name="table_Abis", con=engine, if_exists="append", index=False
    )
    df_B = pd.DataFrame(
        {
            "b1": ["Transaction1", "Transaction2"],
            "b2": ["User2", "User1"],
        }
    )
    df_B.to_sql(name="table_B", con=engine, if_exists="append", index=False)
    df_E = pd.DataFrame(
        {
            "e1": list(range(99)),
        }
    )
    df_E.to_sql(name="table_E", con=engine, if_exists="append", index=False)
    return tab_A, tab_B, tab_C, tab_D, tab_E, tab_Abis

@pytest.fixture(scope="session", autouse=True)
def tables(metadata, engine):

    _ = Table(
        "primary_public_table",
        metadata,
        Column("id", types.Integer(), primary_key=True),
        Column("text", types.String(), nullable=True),
    )
    _ = Table(
        "secondary_public_table",
        metadata,
        Column("id", types.Integer(), primary_key=True),
        Column("text", types.String(), nullable=True),
    )
    _ = Table(
        "primary_table",
        metadata,
        Column("id", types.Integer(), primary_key=True, nullable=True),
        Column("integer", types.Integer(), nullable=False),
        Column("float", types.Float(), nullable=False),
        Column("datetime", types.DateTime(), nullable=False),
        Column("date", types.Date(), nullable=False),
        Column("boolean", types.Boolean(), nullable=False),
        Column("text", types.String(), nullable=False),
        Column(
            "public_fk",
            types.Integer(),
            ForeignKey("primary_public_table.id", ondelete='CASCADE'),
            nullable=False,
        ),
    )
    _ = Table(
        "secondary_table",
        metadata,
        Column("id", types.Integer(), ForeignKey("primary_table.id", ondelete='CASCADE'), nullable=True),
        Column("integer", types.Integer(), nullable=True),
        Column("float", types.Float(), nullable=True),
        Column("datetime", types.DateTime(), nullable=True),
        Column("date", types.Date(), nullable=True),
        Column("boolean", types.Boolean(), nullable=True),
        Column("text", types.String(), nullable=True),
        Column(
            "public_fk",
            types.Integer(),
            ForeignKey("secondary_public_table.id", ondelete='CASCADE'),
            nullable=True,
        ),
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS primary_table, secondary_table, primary_public_table, secondary_public_table CASCADE"))

    metadata.create_all(engine)
    private, public = create_df(sql=True)
    public.to_sql(
        name="primary_public_table",
        con=engine,
        if_exists="append",
        index=False,
    )
    public.to_sql(
        name="secondary_public_table",
        con=engine,
        if_exists="append",
        index=False,
    )
    private.to_sql(
        name="primary_table", con=engine, if_exists="append", index=False
    )
    private.to_sql(
        name="secondary_table", con=engine, if_exists="append", index=False
    )

def create_df(sql: bool) -> pd.DataFrame:
    public = pd.DataFrame(
        {
            "id": np.linspace(1, LENGTH_PUBLIC, LENGTH_PUBLIC),
            "text": [
                "".join(
                    np.random.choice(
                        list(string.ascii_uppercase + string.digits), size=8
                    ).tolist()
                )
                for _ in range(LENGTH_PUBLIC)
            ],
        }
    )

    private = pd.DataFrame(
        {
            "id": np.linspace(1, LENGTH_1, LENGTH_1, dtype=int),
            "integer": np.random.randint(low=0, high=100, size=LENGTH_1),
            "float": np.random.random(size=LENGTH_1),
            "datetime": np.random.choice(
                pd.date_range("1980-01-01", "1980-02-01", freq="s"),
                LENGTH_1,
                replace=True,
            ),
            "text": [
                "".join(
                    np.random.choice(
                        list(string.ascii_uppercase + string.digits), size=8
                    ).tolist()
                )
                for _ in range(LENGTH_1)
            ],
            "date": np.random.choice(
                pd.date_range("1980-01-01", "2000-01-01", freq="D"),
                LENGTH_1,
                replace=True,
            ),
            "boolean": np.random.choice(
                [True, False], LENGTH_1, replace=True, p=[0.7, 0.3]
            ),
            "public_fk": np.random.choice(
                public["id"].values, LENGTH_1, replace=True
            ),
        }
    )

    if sql:
        return private, public

    private = private.drop("public_fk", axis=1)
    private["date"] = private["date"].astype(str)
    return private