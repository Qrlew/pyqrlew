import pytest
from pyqrlew.io import PostgreSQL

@pytest.fixture(scope='session', autouse=True)
def database():
    return PostgreSQL()
