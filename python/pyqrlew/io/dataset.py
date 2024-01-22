from dataclasses import dataclass
import logging
from uuid import uuid4 as generate_uuid
from typing import Optional, Tuple, List, Dict, Union
import json
from sqlalchemy import MetaData, Table, Column, types, select, func, literal, String, ARRAY, case, text
from sqlalchemy.engine import Engine
import pyqrlew as qrl

def dataset_from_database(
    name: str,
    engine: Engine,
    schema_name: Optional[str]=None,
    ranges: bool=False,
    possible_values_threshold: Optional[int]=None
) -> qrl.Dataset:
    """_summary_

    Args:
        name (str): _description_
        engine (Engine): _description_
        schema_name (Optional[str], optional): _description_. Defaults to None.
        ranges (bool, optional): _description_. Defaults to False.
        possible_values_threshold (Optional[int], optional): _description_. Defaults to None.

    Raises:
        NotImplementedError: _description_

    Returns:
        qrl.Dataset: _description_
    """
    metadata = MetaData()
    metadata.reflect(engine, schema=schema_name)

    def dataset_schema_size() -> Tuple[dict, dict, Optional[dict]]:
        """Return a (dataset, schema) pair or (dataset, schema, size) triplet """
        ds = dataset()
        return (
            ds,
            schema(ds),
            size(ds),
        )

    def dataset() -> dict:
        """Return a (dataset, schema) pair or (dataset, schema, size) triplet """
        return {
            '@type': 'sarus_data_spec/sarus_data_spec.Dataset',
            'uuid': generate_uuid().hex,
            'name': 'Transformed',
            'spec': {
                'transformed': {
                    'transform': generate_uuid().hex,
                    'arguments': [],
                    'named_arguments': {},
                },
            },
            'properties': {},
            'doc': 'This ia a demo dataset for testing purpose',
        }

    def schema(dataset: dict) -> dict:
        tables = {"fields": [table(metadata.tables[name]) for name in metadata.tables]}

        if schema_name is not None:
            tables = {
                "fields": [{
                    'name': schema_name,
                    'type': {
                        'name': 'Union',
                        'union': tables,
                        'properties': {
                            'public_fields': '[]'
                        },
                    }
                }],
            }

        return {
            '@type': 'sarus_data_spec/sarus_data_spec.Schema',
            'uuid': generate_uuid().hex,
            'dataset': dataset['uuid'],
            'name': name,
            'type': {
                # Slugname
                'name': name.lower(),
                'struct': {
                    'fields': [
                        {
                            'name': 'sarus_data',
                            'type': {
                                'name': 'Union',
                                'union': tables,
                                'properties': {
                                    'public_fields': '[]',
                                },
                            },
                        },
                        {
                            'name': 'sarus_weights',
                            'type': {
                                'name': 'Integer',
                                'integer': {
                                    'min': '-9223372036854775808',
                                    'max': '9223372036854775807',
                                    'base': 'INT64',
                                    'possible_values': []
                                },
                                'properties': {}
                            },
                        },
                        {
                            'name': 'sarus_is_public',
                            'type': {
                                'name': 'Boolean',
                                'boolean': {},
                                'properties': {},
                            },
                        },
                        {
                            'name': 'sarus_protected_entity',
                            'type': {
                                'name': 'Id',
                                'id': {
                                    'base': 'STRING',
                                    'unique': False,
                                },
                                'properties': {},
                            },
                        },
                    ],
                },
                'properties': {}
            },
            'protected': {
                'label': 'data',
                'paths': [],
                'properties': {},
            },
            'properties': {
                'max_max_multiplicity': '1',
                'foreign_keys': '',
                'primary_keys': '',
            }
        }

    def table(tab: Table) -> dict:
        min_max_possible_values = compute_min_max_possible_values(tab)
        return {
            'name': tab.name,
            'type': {
                'name': 'Struct',
                'struct': {
                    'fields': [column(col, **min_max_possible_values[col.name]) for col in tab.columns],
                },
                'properties': {},
            }
        }

    def compute_min_max_possible_values(tab: Table) -> Dict[str, Dict[str, Union[str, List[str]]]]:
        """Send 3 SQL queries for loading the bounds"""
        values = {col.name: {'min': None, 'max': None, 'possible_values': []} for col in tab.columns}
        intervals_types = [
            types.Integer, types.BigInteger,
            types.Float, types.Numeric,
            types.String, types.Text, types.Unicode, types.UnicodeText,
            types.Date, types.DateTime, types.Time
        ]
        interval_cols = [
            col for col in tab.columns
            if any(isinstance(col.type, t) for t in intervals_types)
        ]

        if ranges and len(interval_cols) != 0:
            min_query = select(*[func.cast(func.min(col), String).label(col.name) for col in interval_cols]).select_from(tab)
            max_query = select(*[func.cast(func.max(col), String).label(col.name) for col in interval_cols]).select_from(tab)

            with engine.connect() as conn:
                min_results = conn.execute(min_query).fetchone()
                max_results = conn.execute(max_query).fetchone()

            for col, min_val, max_val in zip(interval_cols, min_results, max_results):
                values[col.name]['min'] = min_val
                values[col.name]['max'] = max_val

        if possible_values_threshold is not None and len(interval_cols) != 0:
            tablename = f"\"{tab.name}\"" if tab.schema is None else f"\"{tab.schema}\".\"{tab.name}\""
            values_query = text(
                "SELECT " + ','.join([
                    f"CASE WHEN COUNT(DISTINCT \"{col.name}\") <= {possible_values_threshold} "
                    f"THEN array_agg(DISTINCT CAST(\"{col.name}\" AS Text)) ELSE ARRAY[]::VARCHAR[] "
                    f"END AS \"{col.name}\" "
                    for col in interval_cols
                ])
                + f"FROM {tablename}"
            ) # case very complicate to use with loop

            with engine.connect() as conn:
                values_results = conn.execute(values_query).fetchone()

            for col, possible_values in zip(interval_cols, values_results):
                values[col.name]['possible_values'] = [str(v) for v in possible_values]

        return values

    def column(col: Column, min:Optional[str]=None, max:Optional[str]=None, possible_values:List[str]=[]) -> dict:
        if isinstance(col.type, types.Integer) or isinstance(col.type, types.BigInteger):
            min = '-9223372036854775808' if min is None else min
            max = '9223372036854775807' if max is None else max
            return {
                'name': col.name,
                'type': {
                    'name': 'Integer',
                    'integer': {
                        'base': 'INT64',
                        'min': min,
                        'max': max,
                        'possible_values': possible_values,
                    },
                    'properties': {},
                },
            }
        elif isinstance(col.type, types.Float) or isinstance(col.type, types.Numeric):
            min = '-1.7976931348623157e+308' if min is None else min
            max = '1.7976931348623157e+308' if max is None else max
            return {
                'name': col.name,
                'type': {
                    'name': 'Float64',
                    'float': {
                        'base': 'FLOAT64',
                        'min': min,
                        'max': max,
                        'possible_values': [],
                    },
                    'properties': {},
                },
            }
        elif isinstance(col.type, types.String) or isinstance(col.type, types.Text) or isinstance(col.type, types.Unicode) or isinstance(col.type, types.UnicodeText):

            return {
                'name': col.name,
                'type': {
                    'name': 'Text UTF-8',
                    'text': {
                        'encoding': 'UTF-8',
                        'min': min,
                        'max': max,
                        'possible_values': possible_values,
                    },
                    'properties': {},
                },
            }
        elif isinstance(col.type, types.Boolean):
            return {
                'name': col.name,
                'type': {
                    'name': 'Boolean',
                    'boolean': {},
                    'properties': {},
                },
            }
        elif isinstance(col.type, types.Date) or isinstance(col.type, types.DateTime) or isinstance(col.type, types.Time):
            min = '01-01-01 00:00:00' if min is None else min
            max = '9999-12-31 00:00:00' if max is None else max
            return {
                'name': col.name,
                'type': {
                    'name': 'Datetime',
                    'datetime': {
                        'format': '%Y-%m-%d %H:%M:%S',
                        'min': min,
                        'max': max,
                    },
                    'properties': {},
                },
            }
        else:
            return {
                'name': col.name,
                'type': {
                    'name': 'Type',
                    'type': {},
                    'properties': {},
                },
            }

    def size(dataset: dict) -> dict:
        tables = {'fields': [table_size(metadata.tables[name]) for name in metadata.tables]}
        if schema_name is not None:
            tables = {
                'fields': [
                    {
                        'name': schema_name,
                        'statistics': {
                            'name': 'Union',
                            'union': tables,
                            'properties': {},
                        },
                        'properties': {},
                    },
                ],
            }

        return {
            '@type': 'sarus_data_spec/sarus_data_spec.Size',
            'uuid': generate_uuid().hex,
            'dataset': dataset['uuid'],
            'name': f'{name}_sizes',
            'statistics': {
                'name': 'Union',
                'union': tables,
                'properties': {},
            },
            'properties': {},
        }

    def table_size(tab: Table) -> dict:
        with engine.connect() as conn:
            result = conn.execute(select(func.count()).select_from(tab))
            size = result.scalar()
        multiplicity = 1.0
        return {
            'name': tab.name,
            'statistics': {
                'name': 'Struct',
                'struct': {
                    'fields': [column_size(col, size, multiplicity) for col in tab.columns],
                    'size': str(size),
                    'multiplicity': multiplicity,
                },
                'properties': {},
            }
        }

    def column_size(col: Column, size: int, multiplicity: float) -> dict:
        if isinstance(col.type, types.Integer) or isinstance(col.type, types.BigInteger):
            return {
                'name': col.name,
                'statistics': {
                    "integer": {
                        "distribution": {
                            "integer": {
                                "max": "9223372036854775807",
                                "min": "-9223372036854775808",
                                "points": []
                            },
                            "properties": {}
                            },
                        "multiplicity": multiplicity,
                        "size": size
                    },
                    "name": "Integer",
                    "properties": {}
                }
            }
        elif isinstance(col.type, types.Float) or isinstance(col.type, types.Numeric):
            return {
                'name': col.name,
                'statistics': {
                    "float": {
                        "distribution": {
                            "double": {
                                "max": '1.7976931348623157e+308',
                                "min": '-1.7976931348623157e+308',
                                "points": []
                            },
                            "properties": {}
                            },
                        "multiplicity": multiplicity,
                        "size": size
                    },
                    "name": "Float",
                    "properties": {}
                }
            }
        elif isinstance(col.type, types.String) or isinstance(col.type, types.Text) or isinstance(col.type, types.Unicode) or isinstance(col.type, types.UnicodeText):
            return {
                'name': col.name,
                'statistics': {
                    "text": {
                        "distribution": {
                            "integer": {
                                "max": "9223372036854775807",
                                "min": "-9223372036854775808",
                                "points": []
                            },
                            "properties": {}
                            },
                        "multiplicity": multiplicity,
                        "size": size
                    },
                    "name": "Text",
                    "properties": {}
                }
            }

        elif isinstance(col.type, types.Boolean):
            return {
                'name': col.name,
                'statistics': {
                    "boolean": {
                        "distribution": {
                            "integer": {
                                "max": "9223372036854775807",
                                "min": "-9223372036854775808",
                                "points": []
                            },
                            "properties": {}
                            },
                        "multiplicity": multiplicity,
                        "size": size
                    },
                    "name": "Boolean",
                    "properties": {}
                }
            }
        elif isinstance(col.type, types.Date) or isinstance(col.type, types.DateTime) or isinstance(col.type, types.Time):
            return {
                'name': col.name,
                'statistics': {
                    "datetime": {
                        "distribution": {
                            "integer": {
                                "max": "9223372036854775807",
                                "min": "-9223372036854775808",
                                "points": []
                            },
                            "properties": {}
                            },
                        "multiplicity": multiplicity,
                        "size": size
                    },
                    "name": "Datetime",
                    "properties": {}
                }
            }

        else: # TODO: support more types
            raise NotImplementedError(f"SQL -> Sarus Conversion not supported for {col.type} SQL type")

    # Gather protobufs
    dataset, schema, size = dataset_schema_size()
    # Display when debugging
    logging.debug(json.dumps(dataset))
    logging.debug(json.dumps(schema))
    logging.debug(json.dumps(size))
    # Return the result
    return qrl.Dataset(json.dumps(dataset), json.dumps(schema), json.dumps(size))

# Make it a builder
qrl.Dataset.from_database = dataset_from_database

# Add a useful const
qrl.Dataset.CONSTRAINT_UNIQUE: str = '_UNIQUE_'

# A method to get select a schema
def schema(dataset: qrl.Dataset, schema: str) -> 'Schema':
    return Schema(dataset, schema)

# Add the method
qrl.Dataset.__getattr__ = schema

@dataclass
class Schema:
    dataset: qrl.Dataset
    schema: str

    def __getattr__(self, table: str) -> 'Table':
        return Table(self.dataset, self.schema, table)

@dataclass
class Table:
    dataset: qrl.Dataset
    schema: str
    table: str

    def __getattr__(self, column: str) -> 'Column':
        return Column(self.dataset, self.schema, self.table, column)

    def relation(self) -> qrl.Relation:
        return next(rel for path, rel in self.dataset.relations() if path[1]==self.schema and path[2]==self.table)

@dataclass
class Column:
    dataset: qrl.Dataset
    schema: str
    table: str
    column: str

    def with_range(self, min:float, max:float) -> qrl.Dataset:
        return self.dataset.with_range(self.schema, self.table, self.column, min, max)
    
    def with_possible_values(self, with_possible_values: List[str]) -> qrl.Dataset:
        return self.dataset.with_possible_values(self.schema, self.table, self.column, with_possible_values)
    
    def with_constraint(self, constraint: str) -> qrl.Dataset:
        return self.dataset.with_constraint(self.schema, self.table, self.column, constraint)

    def with_unique_constraint(self) -> qrl.Dataset:
        return self.with_constraint(qrl.Dataset.CONSTRAINT_UNIQUE)
    
    def with_no_constraint(self) -> qrl.Dataset:
        return self.with_constraint(None)