from pyqrlew.utils import tables_prefix
from pyqrlew import Dialect

def test_tables_prefix():
    query_str = """
    WITH mytab AS (SELECT * FROM A.b.c),
    mytab2 AS (SELECT * FROM (SELECT * FROM d.e.f) AS subtab )
    SELECT * FROM mytab JOIN mytab2 USING(id)
    """
    tables = tables_prefix(query_str, Dialect.PostgreSql)
    assert tables == ["A","d"]
