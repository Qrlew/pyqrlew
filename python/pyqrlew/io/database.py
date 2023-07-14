from uuid import uuid4 as generate_uuid
from sqlalchemy import Engine, MetaData

def dataset(metadata: MetaData) -> dict:
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
    print(dataset)
    return {
        '@type': 'sarus_data_spec/sarus_data_spec.Schema',
        'uuid': generate_uuid().hex,
        'dataset': dataset['uuid'],
        'name': metadata.schema,
        'type': {
            'struct': {
                'fields': [
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
    # for name in metadata.tables:
    #     print(metadata.tables[name])
    #     # print(f'{name} -> {table}')