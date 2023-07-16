from uuid import uuid4 as generate_uuid
from typing import Optional
from sqlalchemy import Engine, MetaData, Table, Column
from sqlalchemy import types

def dataset_schema_size(metadata: MetaData) -> tuple[dict, dict, Optional[dict]]:
    """Return a (dataset, schema) pair or (dataset, schema, size) triplet """
    ds = dataset(metadata)
    return (
        ds,
        schema(metadata, ds),
        None
    )

def dataset(metadata: MetaData) -> dict:
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
            }
        },
        'properties': {},
        'doc': 'This ia a demo dataset for testing purpose',
    }

def schema(metadata: MetaData, dataset: dict) -> dict:
    return {
        '@type': 'sarus_data_spec/sarus_data_spec.Schema',
        'uuid': generate_uuid().hex,
        'dataset': dataset['uuid'],
        'name': metadata.schema,
        'type': {
            'name': '',
            'struct': {
                'fields': [
                    {
                        'name': 'data',
                        'type': {
                            'name': 'Union',
                            'union': {
                                "fields": [table(metadata.tables[name]) for name in metadata.tables]
                            },
                            'properties': {
                                'public_fields': '[]'
                            },
                        }
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
                        }
                    },
                    {
                        'name': 'sarus_is_public',
                        'type': {
                            'name': 'Boolean',
                            'boolean': {},
                            'properties': {}
                        }
                    },
                    {
                        'name': 'sarus_protected_entity',
                        'type': {
                            'name': 'Id',
                            'id': {
                                'base': 'STRING',
                                'unique': False
                            },
                            'properties': {}
                        }
                    },
                ],
            },
            'properties': {}
        },
        'protected': {
            'label': 'data',
            'paths': [],
            'properties': {}
        },
        'properties': {
            'max_max_multiplicity': '1',
            'foreign_keys': '',
            'primary_keys': '',
        }
    }

def table(tab: Table) -> dict:
    return {
        'name': tab.name,
        'type': {
            'name': 'Struct',
            'struct': {
                'fields': [column(col) for col in tab.columns]
            },
            'properties': {},
        }
    }

def column(col: Column) -> dict:
    print(col.type)
    if isinstance(col.type, types.Integer) or isinstance(col.type, types.BigInteger):
        return {
            'name': col.name,
            'integer': {
                'name': 'Integer',
                "integer": {
                    "base": "INT64",
                    "min": "-9223372036854775808",
                    "max": "9223372036854775807",
                    "possible_values": []
                },
                'properties': {},
            }
        }
    else:
        return {
            'name': col.name,
            'type': {
                'name': 'Type',
                'type': {},
                'properties': {},
            }
        }