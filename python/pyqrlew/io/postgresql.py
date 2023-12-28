import shutil
from pathlib import Path
from sqlalchemy import select, column, table, text
from qrlew_datasets.files import SQL
from qrlew_datasets.databases import PostgreSQL as EmptyPostgreSQL
from pyqrlew.io.dataset import dataset
import pyqrlew as qrl

NAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: str = 5433

POSSIBLE_VALUES_THRESHOLD: int = 20

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
        return self.load_resource('extract', SQL('extract').local())

    def load_financial(self) -> 'PostgreSQL':
        return self.load_resource('financial', SQL('financial').local())

    def load_hepatitis(self) -> 'PostgreSQL':
        return self.load_resource('hepatitis_std', SQL('hepatitis').local())

    def load_imdb(self) -> 'PostgreSQL':
        return self.load_resource('imdb_ijs', SQL('imdb').local())

    def load_retail(self) -> 'PostgreSQL':
        return self.load_resource('retail', SQL('retail').local())

    def extract(self) -> qrl.Dataset:
        self.load_extract()
        return dataset('extract', self.engine(), 'extract', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def financial(self) -> qrl.Dataset:
        self.load_financial()
        return dataset('financial', self.engine(), 'financial', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def hepatitis(self) -> qrl.Dataset:
        self.load_hepatitis()
        return dataset('hepatitis_std', self.engine(), 'hepatitis_std', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def imdb(self) -> qrl.Dataset:
        self.load_imdb()
        return dataset('imdb_ijs', self.engine(), 'imdb_ijs', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def retail(self) -> qrl.Dataset:
        self.load_retail()
        return dataset('retail', self.engine(), 'retail', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)
