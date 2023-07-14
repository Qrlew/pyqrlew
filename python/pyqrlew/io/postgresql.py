import shutil
from pathlib import Path
import importlib.resources as pkg_resources
import logging
from sqlalchemy import Engine, exists, select, column, table, text
from datasets import sources
from datasets.databases import PostgreSQL as EmptyPostgreSQL

NAME: str = 'pyqrlew-db'
class PostgreSQL(EmptyPostgreSQL):
    def __init__(self) -> None:
        super().__init__(NAME)
    
    def with_resource(self, schema: str, src: Path) -> 'PostgreSQL':
        with self.engine().connect() as conn:
            res = list(conn.execute(select(column('schema_name')).select_from(table('schemata', schema='information_schema')).where(column('schema_name') == schema)))
            schema_exists = len(res)==1
        if not schema_exists:
            dst = Path('/tmp') / schema
            shutil.copyfile(src, dst)
            self.load(dst)
        return self
    
    def with_financial(self) -> 'PostgreSQL':
        return self.with_resource('financial', pkg_resources.files(sources) / 'financial' / 'financial.sql')

    def with_hepatitis(self) -> 'PostgreSQL':
        return self.with_resource('hepatitis_std', pkg_resources.files(sources) / 'hepatitis' / 'Hepatitis_std.sql')

    def with_imdb(self) -> 'PostgreSQL':
        return self.with_resource('imdb_ijs', pkg_resources.files(sources) / 'imdb' / 'imdb_ijs.sql')
