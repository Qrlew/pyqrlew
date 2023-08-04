import os
import sqlalchemy
from datasets.database import Database
import pyqrlew as qrl
from pyqrlew.io.database import dataset
import pandas as pd

dtype_to_sqlalchemy = {
    'object': sqlalchemy.String,
    'int64': sqlalchemy.Integer,
    'float64': sqlalchemy.Float,
    'bool': sqlalchemy.Boolean,
    'datetime64[ns]': sqlalchemy.DateTime,
    'timedelta64[ns]': sqlalchemy.Interval,
    'category': sqlalchemy.String
}


class SQLite(Database):
    def __init__(self, db_file:str) -> None:
        self.db_file = db_file

    def load_csv(self, table_name: str, csv_file: str) -> None:
        df = pd.read_csv(csv_file)
        self.load_pandas(table_name, df)

    def load_pandas(self, table_name: str, df: pd.DataFrame) -> None:
        column_types = {
            column: dtype_to_sqlalchemy[str(df[column].dtype)]
            for column in df.columns
        }
        metadata = sqlalchemy.MetaData()
        columns = [
            sqlalchemy.Column(col_name, col_type)
            for col_name, col_type in column_types.items()
        ]
        _ = sqlalchemy.Table(table_name, metadata, *columns)
        engine = self.engine()
        metadata.create_all(engine)
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    def url(self) -> str:
        return f'sqlite:///{self.db_file}'

    def remove(self) -> None:
        os.remove(self.db_file)

    def engine(self) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(self.url(), echo=True)

    def eval(self, relation: qrl.Relation) -> list:
        return self.execute(relation.render())

    def execute(self, query: str) -> list:
        with self.engine().connect() as conn:
            result = conn.execute(sqlalchemy.text(query)).all()
        return result

    def dataset(self) -> qrl.Dataset:
        return dataset(self.db_file, self.engine())

    def print_infos_metadata(self) -> None:
        metadata = sqlalchemy.MetaData()
        metadata.reflect(self.engine(), schema=None)
        for table_name, table in metadata.tables.items():
            print(f"\nTable: {table_name}")
            for column in table.c:
                print(f" - {column.name}, Type: {column.type}")
