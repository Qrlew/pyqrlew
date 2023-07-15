from uuid import uuid4 as generate_uuid
from typing import Optional
from sqlalchemy import Engine, MetaData, Table, Column

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
            'struct': {
                'fields': [
                    {
                        'name': 'data',
                        'type': {
                            'name': 'Union',
                            'properties': {
                                'public_fields': '[]'
                            },
                            'union': {
                                "fields": [table(metadata.tables[name]) for name in metadata.tables]
                            }
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
            'name': '',
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
            'properties': {},
            'struct': {
                'fields': []#[column(tab.columns[name]) for name in tab.columns]
            },
        }
    }

def column(col: Column) -> dict:
    return {
        'name': col.name,
        'type': {
            'name': 'Type',
            'properties': {},
            'type': {}
        }
    }