from typing import List
import numpy as np
import pandas as pd # type: ignore
from qrlew_datasets.databases import PostgreSQL as EmptyPostgreSQL # type: ignore
from pyqrlew import dataset_from_database
import pyqrlew as qrl
from sqlalchemy import text
from enum import Enum
import typing as t

class Distribution(Enum):
    LAPLACE = "laplace"
    UNIFORM = "uniform"
    GAUSSIAN = "gaussian"
    HALTON = "halton"

class ColumnSpec:
    def __init__(self, name: str, distribution: Distribution, nan_probability: float = 0.0, **kwargs: float) -> None:
        self.name = name
        self.distribution = distribution
        self.nan_probability = nan_probability
        self.kwargs = kwargs

class StochasticDatabase:
    def __init__(self, schema_name:str, dbname: str, user:str, password:str, port:int) -> None:
        db = EmptyPostgreSQL(dbname, user, password, port)
        self.schema_name = schema_name
        self.engine = db.engine()
        with self.engine.connect() as connection:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def create_table(self, name: str, size: int, column_specs: List[ColumnSpec], index_is_unique:bool=True) -> t.Optional[int]:
        data = RandomTableGenerator(size).create_table(column_specs)
        if index_is_unique:
            data["id"] = np.arange(size)
        else:
            data["id"] = [
                int(np.random.normal(loc = size / 2, scale = size / 4))
                for _ in range(size)
            ]
        return data.to_sql(name, schema=self.schema_name, con=self.engine, if_exists='replace', index=False)

    def dataset(self) -> qrl.Dataset:
        return dataset_from_database(self.schema_name, self.engine, self.schema_name)

    def eval(self, relation: qrl.Relation) -> list:
        return self.execute(relation.to_query(None))

    def execute(self, query: str) -> list:
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).all()
        return list(result)

class RandomTableGenerator:
    def __init__(self, size: int) -> None:
        self.size = size

    def create_table(self, column_specs: List[ColumnSpec]) -> pd.DataFrame:
        table_data = {
            spec.name: self._generate_numbers(spec)
            for spec in column_specs
        }
        return pd.DataFrame(table_data)

    def _generate_numbers(self, spec: ColumnSpec) -> np.ndarray:
        distribution = spec.distribution
        kwargs = spec.kwargs
        nan_probability = spec.nan_probability

        random_numbers: np.ndarray = np.zeros(self.size)
        if distribution == Distribution.LAPLACE:
            random_numbers =  np.random.laplace(**kwargs, size=self.size)
        elif distribution == Distribution.UNIFORM:
            random_numbers =  np.random.uniform(**kwargs, size=self.size)
        elif distribution == Distribution.GAUSSIAN:
            random_numbers =  np.random.normal(**kwargs, size=self.size)
        elif distribution == Distribution.HALTON:
            random_numbers =  self.halton_sequence(**t.cast(dict[str, int], kwargs))
        if  nan_probability > 0:
            random_numbers =  self.add_nan(random_numbers, nan_probability)
        return random_numbers

    def add_nan(self, random_numbers: np.ndarray, nan_probability: float=0.1) -> np.ndarray:
        nan_mask = random_numbers < nan_probability
        random_numbers[nan_mask] = np.nan
        return random_numbers

    def halton_sequence(self, base:int=2, low:int=0, high:int=1) -> np.ndarray:
        sequence = np.zeros(self.size)

        for i in range(self.size):
            f = 1.0
            r = 0.0
            index = i + 1

            while index > 0:
                f /= base
                r += f * (index % base)
                index = index // base

            sequence[i] = r

        return low + (high - low) * sequence
