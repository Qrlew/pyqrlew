import logging
from uuid import uuid4 as generate_uuid
from typing import Optional, Tuple
import json
from sqlalchemy import MetaData, Table, Column, types, select, func
from sqlalchemy.engine import Engine
import pyqrlew as qrl

def dataset(name: str, engine: Engine, schema_name: Optional[str]=None) -> qrl.Dataset:
    metadata = MetaData()
    metadata.reflect(engine, schema=schema_name)

    def _dataset_schema_size() -> Tuple[dict, dict, Optional[dict]]:
        """Return a (dataset, schema) pair or (dataset, schema, size) triplet """
        ds = _dataset()
        return (
            ds,
            _schema(ds),
            _size(ds),
        )

    def _dataset() -> dict:
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

    def _schema(dataset: dict) -> dict:
        tables = {"fields": [_table(metadata.tables[name]) for name in metadata.tables]}

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

    def _table(tab: Table) -> dict:
        return {
            'name': tab.name,
            'type': {
                'name': 'Struct',
                'struct': {
                    'fields': [_column(col) for col in tab.columns],
                },
                'properties': {},
            }
        }

    def _column(col: Column) -> dict:
        if isinstance(col.type, types.Integer) or isinstance(col.type, types.BigInteger):
            return {
                'name': col.name,
                'type': {
                    'name': 'Integer',
                    'integer': {
                        'base': 'INT64',
                        'min': '-9223372036854775808',
                        'max': '9223372036854775807',
                        'possible_values': [],
                    },
                    'properties': {},
                },
            }
        elif isinstance(col.type, types.Float) or isinstance(col.type, types.Numeric):
            return {
                'name': col.name,
                'type': {
                    'name': 'Float64',
                    'float': {
                        'base': 'FLOAT64',
                        'min': '-1.7976931348623157e+308',
                        'max': '1.7976931348623157e+308',
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
                        'encoding': 'UTF-8'
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
            return {
                'name': col.name,
                'type': {
                    'name': 'Datetime',
                    'datetime': {
                        'format': '%Y-%m-%d %H:%M:%S',
                        'min': '01-01-01 00:00:00',
                        'max': '9999-12-31 00:00:00',
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

    def _size(dataset: dict) -> dict:
        tables = {'fields': [_table_size(metadata.tables[name]) for name in metadata.tables]}
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

    def _table_size(tab: Table) -> dict:
        with engine.connect() as conn:
            result = conn.execute(select(func.count()).select_from(tab))
            size = result.scalar()
        multiplicity = 1.0
        return {
            'name': tab.name,
            'statistics': {
                'name': 'Struct',
                'struct': {
                    'fields': [_column_size(col, size, multiplicity) for col in tab.columns],
                    'size': str(size),
                    'multiplicity': multiplicity,
                },
                'properties': {},
            }
        }

    def _column_size(col: Column, size: int, multiplicity: float) -> dict:
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
    dataset, schema, size = _dataset_schema_size()
    # Display when debugging
    logging.debug(json.dumps(dataset))
    logging.debug(json.dumps(schema))
    logging.debug(json.dumps(size))
    # Return the result
    return qrl.Dataset(json.dumps(dataset), json.dumps(schema), json.dumps(size))