import shutil
from pathlib import Path
import importlib.resources as pkg_resources
import json
import logging
from sqlalchemy import MetaData, Engine, exists, select, column, table, text, Result
from datasets import sources
from datasets.databases import PostgreSQL as EmptyPostgreSQL
from pyqrlew.io.database import dataset
import pyqrlew as qrl

NAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: str = 5433

class PostgreSQL(EmptyPostgreSQL):
    def __init__(self) -> None:
        super().__init__(NAME, USER, PASSWORD, PORT)
    
    def load_resource(self, schema: str, src: Path) -> 'PostgreSQL':
        with self.engine().connect() as conn:
            res = list(conn.execute(select(column('schema_name')).select_from(table('schemata', schema='information_schema')).where(column('schema_name') == schema)))
            schema_exists = len(res)==1
        if not schema_exists:
            dst = Path('/tmp') / schema
            shutil.copyfile(src, dst)
            self.load(dst)
        return self
    
    def load_extract(self) -> 'PostgreSQL':
        return self.load_resource('extract', pkg_resources.files(sources) / 'extract' / 'extract.sql')

    def load_financial(self) -> 'PostgreSQL':
        return self.load_resource('financial', pkg_resources.files(sources) / 'financial' / 'financial.sql')

    def load_hepatitis(self) -> 'PostgreSQL':
        return self.load_resource('hepatitis_std', pkg_resources.files(sources) / 'hepatitis' / 'Hepatitis_std.sql')

    def load_imdb(self) -> 'PostgreSQL':
        return self.load_resource('imdb_ijs', pkg_resources.files(sources) / 'imdb' / 'imdb_ijs.sql')
    
    def extract(self) -> qrl.Dataset:
        self.load_extract()
        return dataset('extract', self.engine())
    
    def financial(self) -> qrl.Dataset:
        self.load_financial()
        return dataset('financial', self.engine())
    
    def hepatitis(self) -> qrl.Dataset:
        self.load_hepatitis()
        return dataset('hepatitis_std', self.engine())

    def imdb(self) -> qrl.Dataset:
        self.load_imdb()
        return dataset('imdb_ijs', self.engine())

    def eval(self, relation: qrl.Relation) -> list:
        with self.engine().connect() as conn:
            result = conn.execute(text(relation.render())).all()
        return result