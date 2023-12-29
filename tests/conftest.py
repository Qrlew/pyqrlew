from pathlib import Path
from typing import List
import pytest


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
