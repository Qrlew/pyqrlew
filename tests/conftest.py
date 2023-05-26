import pytest
import os
import sqlalchemy
import pyqrlew as qrl
from ressources.names import (
    DATASET_FILENAME,
    SCHEMA_FILENAME,
    SIZE_FILENAME,
)

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))
FILENAME = os.path.join(DIRNAME, 'queries/valid_queries.sql')

@pytest.fixture(scope='session', autouse=True)
def postgres_url():
    url = "postgresql+psycopg2://postgres:1234@localhost:5432/"
    engine = sqlalchemy.engine.create_engine(url, echo=True, future=True)
    try:
        engine.connect()
    except sqlalchemy.exc.OperationalError:
        return "postgresql+psycopg2://postgres:pyqrlew-test@postgres:5432/"
    return "postgresql+psycopg2://postgres:pyqrlew-test@localhost:5432/"

@pytest.fixture(scope='session', autouse=True)
def dataset():
    with open(DATASET_FILENAME, "r") as f:
        dataset = f.read()

    with open(SCHEMA_FILENAME, "r") as f:
        schema = f.read()

    with open(SIZE_FILENAME, "r") as f:
        size = f.read()

    return qrl.Dataset(dataset, schema, size)

