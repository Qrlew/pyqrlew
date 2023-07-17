import shutil
from pathlib import Path
import importlib.resources as pkg_resources
import json
import logging
from sqlalchemy import MetaData, Engine, exists, select, column, table, text
from datasets import sources
from datasets.databases import PostgreSQL as EmptyPostgreSQL
from pyqrlew.io.database import dataset_schema_size
import pyqrlew as qrl

NAME: str = 'pyqrlew-db'
class PostgreSQL(EmptyPostgreSQL):
    def __init__(self) -> None:
        super().__init__(NAME)
    
    def load_resource(self, schema: str, src: Path) -> 'PostgreSQL':
        with self.engine().connect() as conn:
            res = list(conn.execute(select(column('schema_name')).select_from(table('schemata', schema='information_schema')).where(column('schema_name') == schema)))
            schema_exists = len(res)==1
        if not schema_exists:
            dst = Path('/tmp') / schema
            shutil.copyfile(src, dst)
            self.load(dst)
        return self
    
    def load_financial(self) -> 'PostgreSQL':
        return self.load_resource('financial', pkg_resources.files(sources) / 'financial' / 'financial.sql')

    def load_hepatitis(self) -> 'PostgreSQL':
        return self.load_resource('hepatitis_std', pkg_resources.files(sources) / 'hepatitis' / 'Hepatitis_std.sql')

    def load_imdb(self) -> 'PostgreSQL':
        return self.load_resource('imdb_ijs', pkg_resources.files(sources) / 'imdb' / 'imdb_ijs.sql')

    def financial(self) -> qrl.Dataset:
        self.load_financial()
        metadata = MetaData()
        metadata.reflect(self.engine(), schema='financial')
        dataset, schema, size = dataset_schema_size(metadata)
        return qrl.Dataset(json.dumps(dataset), json.dumps(schema), '{}')
    
    def hepatitis(self) -> qrl.Dataset:
        self.load_hepatitis()
        metadata = MetaData()
        metadata.reflect(self.engine(), schema='hepatitis_std')
        dataset, schema, size = dataset_schema_size(metadata)
        return qrl.Dataset(json.dumps(dataset), json.dumps(schema), '{}')

    def imdb(self) -> qrl.Dataset:
        self.load_imdb()
        metadata = MetaData()
        metadata.reflect(self.engine(), schema='imdb_ijs')
        dataset, schema, size = dataset_schema_size(metadata)
        return qrl.Dataset(json.dumps(dataset), json.dumps(schema), '{}')
