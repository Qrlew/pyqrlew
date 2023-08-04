import typing as t
from .sqlite import SQLite
import pandas as pd

def from_csv(table_name: str, csv_file: str, db_name:str ="my_sqlite.db") -> 'SQLite':
    db = SQLite(db_name)
    db.load_csv(table_name, csv_file)
    return db

def from_csv_dict(csv_dict: t.Dict[str, str], db_name:str ="my_sqlite.db") -> 'SQLite':
    db = SQLite(db_name)
    _ = {
        db.load_csv(table_name, csv_file)
        for table_name, csv_file in csv_dict.items()
    }
    return db

def from_pandas(table_name: str, df: pd.DataFrame, db_name:str ="my_sqlite.db") -> 'SQLite':
    db = SQLite(db_name)
    db.load_pandas(table_name, df)
    return db

def from_pandas_dict(csv_dict: t.Dict[str, pd.DataFrame], db_name:str ="my_sqlite.db") -> 'SQLite':
    db = SQLite(db_name)
    _ = {
        db.load_pandas(table_name, df)
        for table_name, df in csv_dict.items()
    }
    return db