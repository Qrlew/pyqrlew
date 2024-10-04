"""Module containing wrappers around rust objects and some utils"""
from collections import namedtuple
from pyqrlew.typing import PrivacyUnit, SyntheticData, DpEvent
from .pyqrlew import _Dataset, _Relation, _RelationWithDpEvent, Dialect, Strategy
import typing as t 
from sqlalchemy.engine import Engine

from dataclasses import dataclass
import logging
from uuid import uuid4 as generate_uuid
from typing import Optional, Tuple, List, Dict, Union
import json
from sqlalchemy import types
import sqlalchemy as sa
import typing as t


MAX_NUMERIC_RANGE = 2**50


class Dataset:
    """A Dataset is a set of SQL Tables.
    
    Examples
    ----------

    Creating a Dataset from an existing database with an sqlalchemy engine

        >>> import pyqrlew as qrl
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("postgresql+psycopg2://****/mydatabase") 
        >>> dataset = Dataset.from_database(name='extract', engine=DB.engine(), schema_name='extract', range=False, possible_values_threshold=None)

    Creating a Dataset from queries and a previous dataset. Here with WHERE statement we also determine the bounds on the age column.

        >>> queries = [
        >>>    (("ds_name", "new_schema", "tab1"), 'SELECT * FROM extract.census WHERE age < 18 AND age > 0'),
        >>>    (("ds_name", "new_schema", "tab2"), 'SELECT * FROM extract.census WHERE age >= 18  AND age < 120'),
        >>> ]
        >>> new_dataset = dataset.from_queries(queries)

    """

    CONSTRAINT_UNIQUE: str = '_UNIQUE_' 


    def __init__(self, dataset: _Dataset) -> None:
        self._dataset = dataset

    def __str__(self) -> str:
        return self._dataset.__str__()

    @staticmethod
    def from_str(dataset: str, schema: str, size: str) -> 'Dataset':
        """Factory method to create a Dataset from string representations
        compatible with protocol buffers defined `here. <https://github.com/Qrlew/sarus/tree/main/sarus_data_spec/protobuf>`_
        
        Args:
            dataset (str): string representation of a dataset
            schema (str): string representation of the dataset's schema
            size (str): string representation of the dataset's size
        """
        return Dataset(_Dataset(dataset, schema, size))

    @staticmethod
    def from_database(
        name: str,
        engine: Engine,
        schema_name: t.Optional[str]=None,
        ranges: bool=False,
        possible_values_threshold: t.Optional[int]=None
    ) -> 'Dataset':
        """Builds a `Dataset` from a sqlalchemy `Engine`.

        Args:
            name (str):
                Name of the Dataset
            engine (Engine):
                The sqlalchemy `Engine` to use
            schema_name (Optional[str], optional):
                The DB schema to use. Defaults to None.
            ranges (bool, optional):
                Use the actual min and max of the data as ranges. **This is unsafe from a privacy perspective**. Defaults to False.
                If False numeric values ranges will be considered between -2.0^50 to 2.0^50.
            possible_values_threshold (Optional[int], optional):
                Use the actual observed values as range. **This is unsafe from a privacy perspective**. Defaults to None.

        Returns:
            Dataset:
        """
        return dataset_from_database(name, engine, schema_name, ranges, possible_values_threshold)

    def __getattr__(dataset: 'Dataset', schema_or_table: str) -> t.Union['Schema', 'Table']:
        if has_schema_name(json.loads(dataset.schema), schema_or_table):
            return Schema(dataset, schema=schema_or_table)
        else:
            return Table(dataset, schema=None, table=schema_or_table)

    @property
    def schema(self) -> str:
        return self._dataset.schema

    @property
    def size(self) -> t.Optional[str]:
        return self._dataset.size
    
    def with_range(self, schema_name: t.Optional[str], table_name: str, field_name: str, min: float, max: float) -> 'Dataset':
        """Returns a new Dataset with a defined range for a given numeric column. Check out more `here! <https://qrlew.readthedocs.io/en/latest/tutorials/getting_started.html#optionally-declare-value-ranges-and-unique-constraints>`_

        Args:
            schema_name (str): schema
            table_name (str): table
            field_name (str): column
            min (float): min range
            max (float): max range
        Returns:
            Dataset:
        """
        return Dataset(self._dataset.with_range(schema_name, table_name, field_name, min, max))
    
    def with_possible_values(self, schema_name: t.Optional[str], table_name: str, field_name: str, possible_values: t.Iterable[str]) -> 'Dataset':
        """Returns a new Dataset with a defined possible values for a given text column. Check out more `here! <https://qrlew.readthedocs.io/en/latest/tutorials/getting_started.html#optionally-declare-value-ranges-and-unique-constraints>`_

        Args:
            schema_name (str): schema
            table_name (str): table
            field_name (str): column
            possible_values (Iterable[str]): a sequence with wanted possible values
        Returns:
            Dataset:
        """
        return Dataset(self._dataset.with_possible_values(schema_name, table_name, field_name, possible_values))
    
    def with_constraint(self, schema_name: t.Optional[str], table_name: str, field_name: str, constraint: t.Optional[str]) -> 'Dataset':
        """Returns a new Dataset with a constraint on given column. Check out more `here! <https://qrlew.readthedocs.io/en/latest/tutorials/getting_started.html#optionally-declare-value-ranges-and-unique-constraints>`_
        
        Args:
            schema_name (str): schema
            table_name (str): table
            field_name (str): column
            constraint (Optional[str]):  Unique or PrimaryKey
        Returns:
            Dataset:
        """
        return Dataset(self._dataset.with_constraint(schema_name, table_name, field_name, constraint))
    
    def relations(self) -> t.Iterable[t.Tuple[t.List[str], 'Relation']]:
        """Returns the Dataset's Relations and their corresponding path"""
        return [(path, Relation(rel)) for (path, rel) in self._dataset.relations()]

    def relation(self, query: str, dialect: t.Optional['Dialect']=None) -> 'Relation':
        """Returns a Relation from am SQL query.
        
        Args:
            query (str): SQL query used to build the Relation.
            dialect (Optional[Dialect]): query's dialect. If not provided, it is assumed to be PostgreSql.
        Returns:
            Relation:
        """
        return Relation(self._dataset.relation(query, dialect))

    def from_queries(self, queries: t.Iterable[t.Tuple[t.Iterable[str], str]], dialect: t.Optional['Dialect']=None) -> 'Dataset':
        """Returns a dataset from queries.
        
        Args:
            queries (Iterable[Tuple[Iterable[str], str]]): A sequence of (path, SQL query).
                The resulting Dataset will have a Relation for each query identified in the dataset
                by the corresponding path.
            dialect (Optional[Dialect]): queries dialect. If not provided, it is assumed to be PostgreSql.
        Returns:
            Dataset:
        """
        return Dataset(self._dataset.from_queries(queries, dialect))


class Relation:
    """A Relation is a Dataset transformed by a SQL query.
    
    Example
    ----------

    Create a relation from a dataset

    >>> from pyqrlew import Dialect
    >>> query = "SELECT AVG(age), sex FROM extract.census GROUP BY sex"
    >>> relation = dataset.relation(query, Dialect.PostgreSql)

    Or alternatively 

    >>> from pyqrlew import Relation
    >>> relation = Relation.from_query(query=query, dataset=dataset, dialect=Dialect.PostgreSql) 

    """
    
    def __init__(self, relation: _Relation) -> None:
        self._relation = relation

    def __str__(self) -> str:
        return self._relation.__str__()

    @staticmethod
    def from_query(query: str, dataset: Dataset, dialect: t.Optional['Dialect']=None) -> 'Relation':
        """Builds a `Relation` from a query and a dataset

        Args:
            query (str): sql query.
            dataset (Dataset): the Dataset.
            dialect (Optional[Dialect]): query's dialect.
                If not provided, it is assumed to be PostgreSql
        
        Returns:
            Relation:
        """
        return Relation(_Relation.from_query(query, dataset._dataset, dialect))

    def to_query(self, dialect: t.Optional[Dialect]=None) -> str:
        """Returns an SQL representation of the Relation.

        Args:
            dialect (Optional[Dialect]): dialect of generated sql query. If no dialect is provided,
                the query will be in PostgreSql. 

        Returns:
            str:
        """
        return self._relation.to_query(dialect)

    def rewrite_as_privacy_unit_preserving(
        self,
        dataset: Dataset,
        privacy_unit: PrivacyUnit,
        epsilon_delta: t.Dict[str, float],
        max_multiplicity: t.Optional[float]=None,
        max_multiplicity_share: t.Optional[float]=None,
        synthetic_data: t.Optional[SyntheticData]=None,
        strategy: t.Optional[Strategy]=None,
    ) -> 'RelationWithDpEvent':
        """Returns as RelationWithDpEvent where it's relation propagates the privacy unit
        through the query. Check out more `here! <https://qrlew.readthedocs.io/en/latest/tutorials/rewrite_with_dp.html#rewritting-with-dp>`_
        
        Args:
            dataset (Dataset):
                Dataset with needed relations
            privacy_unit (PrivacyUnit):
                Definition of privacy unit to be protected. Check out more `here <https://qrlew.readthedocs.io/en/latest/tutorials/rewrite_with_dp.html#the-privacy-unit>`_
            epsilon_delta (Mapping[str, float]): epsilon and delta budget
            max_multiplicity (Optional[float]): maximum number of rows per privacy unit in absolute terms
            max_multiplicity_share (Optional[float]): maximum number of rows per privacy unit in relative terms
                w.r.t. the dataset size. The actual max_multiplicity used to bound the PU contribution will be
                minimum(max_multiplicity, max_multiplicity_share*dataset.size).
            synthetic_data (Optional[Sequence[Tuple[Sequence[str],Sequence[str]]]]): Sequence of pairs 
                of original table path and its corresponding synthetic version. Each table must be specified.
                (e.g.: (["retail_schema", "features"], ["retail_schema", "features_synthetic"])).
            strategy (Optional[Strategy]): Strategy to follow during privacy tracking. If not provided the Hard Strategy will be followed
        Returns:
            RelationWithDpEvent: 
        """
        return RelationWithDpEvent(self._relation.rewrite_as_privacy_unit_preserving(
            dataset._dataset,
            privacy_unit,
            epsilon_delta,
            max_multiplicity,
            max_multiplicity_share,
            synthetic_data,
            strategy
        ))

    def rewrite_with_differential_privacy(
        self,
        dataset: Dataset,
        privacy_unit: PrivacyUnit,
        epsilon_delta: t.Dict[str, float],
        max_multiplicity: t.Optional[float]=None,
        max_multiplicity_share: t.Optional[float]=None,
        synthetic_data: t.Optional[SyntheticData]=None,
    ) -> 'RelationWithDpEvent':
        """It transforms a Relation into its differentially private equivalent. Check out more `here! <https://qrlew.readthedocs.io/en/latest/tutorials/rewrite_with_dp.html#rewritting-with-dp>`_

        Args:
            dataset (Dataset):
                Dataset with needed relations
            privacy_unit (PrivacyUnit):
                Definition of privacy unit to be protected. Check out more `here <https://qrlew.readthedocs.io/en/latest/tutorials/rewrite_with_dp.html#the-privacy-unit>`_
            epsilon_delta (Mapping[str, float]): epsilon and delta budget
            max_multiplicity (Optional[float]): maximum number of rows per privacy unit in absolute terms
            max_multiplicity_share (Optional[float]): maximum number of rows per privacy unit in relative terms
                w.r.t. the dataset size. The actual max_multiplicity used to bound the PU contribution will be
                minimum(max_multiplicity, max_multiplicity_share*dataset.size).
            synthetic_data (Optional[Sequence[Tuple[Sequence[str],Sequence[str]]]]): Sequence of pairs 
                of original table path and its corresponding synthetic version. Each table must be specified.
                (e.g.: (["retail_schema", "features"], ["retail_schema", "features_synthetic"])).

        Returns:
            RelationWithDpEvent: 
        """
        return RelationWithDpEvent(self._relation.rewrite_with_differential_privacy(
            dataset._dataset,
            privacy_unit,
            epsilon_delta,
            max_multiplicity,
            max_multiplicity_share,
            synthetic_data
        ))

    def compose(self, relations: t.Iterable[t.Tuple[t.Iterable[str], 'Relation']]) -> 'Relation':
        """It composes itself with other relations. It substitute its Tables with the corresponding relation in relations
            with the same path. Schemas in the relations to be composed should be compatible with
            the schema of the corresponding table otherwise an error is raised.

        Args:
            relations (t.Iterable[t.Tuple[t.Iterable[str], 'Relation']]): An iterable of pairs of a path and a Relation
                Tables in self will be substituted by the relation with the matching path.
        
        Returns:
            Relation: 
        """
        converted_relations = [(path, rel._relation) for (path, rel) in relations]
        return Relation(self._relation.compose(converted_relations))

    def rename_fields(self, fields: t.Iterable[t.Tuple[str, str]])  -> 'Relation':
        """It renames fields in the Relation
        
        Args:
            fields (t.Iterable[t.Tuple[str, str]]): An iterable of pairs with
                field name to be renamed and the new name
        Returns:
            Relation:
        """
        return Relation(self._relation.rename_fields(fields))

    def with_field(self, name: str, expr: str) -> 'Relation':
        """It creates a new Relation from self with a prepended new field
        
        Args:
            name (str): The field name
            expr (str): An SQL expression following the Generic dialect
        Returns:
            Relation:
        """
        return Relation(self._relation.with_field(name, expr))

    def schema(self) -> str:
        "Returns a string representation of the Relation's schema."
        return self._relation.schema()
    
    def type(self) -> str:
        "Returns a protobuf compatible string representation of the Relation's data type."
        return self._relation.type_()

    def dot(self) -> str:
        "GraphViz representation of the `Relation`"
        return self._relation.dot()

class RelationWithDpEvent:
    """Object containing a differentially private (DP) or privacy unit
    preserving (PUP) relation and the associated DpEvent."""

    def __init__(self, relation_with_dpevent: _RelationWithDpEvent) -> None:
        self.relation_with_dpevent = relation_with_dpevent

    def relation(self) -> Relation:
        """Returns the DP or PUP relation"""
        return Relation(self.relation_with_dpevent.relation())
    
    def dp_event(self) -> DpEvent:
        """Returns the DpEvent associated with the relation."""
        return self.relation_with_dpevent.dp_event()


def dataset_from_database(
    name: str,
    engine: Engine,
    schema_name: Optional[str]=None,
    ranges: bool=False,
    possible_values_threshold: Optional[int]=None
) -> Dataset:
    """Builds a `Dataset` from a sqlalchemy `Engine`

    Args:
        name (str):
            Name of the Dataset
        engine (Engine):
            The sqlalchemy `Engine` to use
        schema_name (Optional[str], optional):
            The DB schema to use. Defaults to None.
        ranges (bool, optional):
            Use the actual min and max of the data as ranges. **This is unsafe from a privacy perspective**. Defaults to False.
        possible_values_threshold (Optional[int], optional):
            Use the actual observed values as range. **This is unsafe from a privacy perspective**. Defaults to None.

    Returns:
        Dataset:
    """

    metadata = sa.MetaData()
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
        """Returns a Json representation compatible with [Protocol Buffers](https://protobuf.dev/reference/protobuf/google.protobuf/#google.protobuf.Struct)
        of the dataset
        """
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
        """Returns a Json representation compatible with [Protocol Buffers](https://protobuf.dev/reference/protobuf/google.protobuf/#google.protobuf.Struct)
        of the schema
        """

        tables_type = [table(metadata.tables[name]) for name in metadata.tables]
        has_admin = any([tab.has_admin for tab in tables_type])

        tables = {"fields": [tab.type_ for tab in tables_type]}

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

        sarus_is_public = {
            "name": "sarus_is_public",
            "type": {
                "boolean": {},
                "name": "Boolean",
                "properties": {}
            }
        }

        sarus_privacy_unit = {
            "name": "sarus_privacy_unit",
            "type": {
                "name": "Optional",
                "optional": {
                "type": {
                    "id": {
                    "base": "STRING",
                    "unique": False
                    },
                    "name": "Id",
                    "properties": {}
                }
                },
                "properties": {}
            }
        }

        sarus_weights = {
            "name": "sarus_weights",
            "type": {
                "float": {
                "base": "FLOAT64",
                "max": 1.7976931348623157e+308,
                "min": 0.0,
                "possible_values": []
                },
                "name": "Float64",
                "properties": {}
            }
        }
        if has_admin:
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
                            sarus_is_public,
                            sarus_privacy_unit,
                            sarus_weights,
                        ],
                    },
                    'properties': {}
                },
                'privacy_unit': {
                    'label': 'sarus_data',
                    'paths': [],
                    'properties': {},
                },
                'properties': {
                    'max_max_multiplicity': '1',
                    'foreign_keys': '',
                    'primary_keys': '',
                }
            }
        else:
            return {
                '@type': 'sarus_data_spec/sarus_data_spec.Schema',
                'uuid': generate_uuid().hex,
                'dataset': dataset['uuid'],
                'name': name,
                'type': {
                    'name': 'Union',
                    'union': tables,
                    'properties': {
                        'public_fields': '[]',
                    },
                },
            }


    def table(tab: sa.Table) -> TableType:
        min_max_possible_values = compute_min_max_possible_values(tab)
        admin_cols = ['sarus_weights', 'sarus_is_public', 'sarus_privacy_unit']
        col_names = [col.name for col in tab.columns]
        has_admin = [admin_col in col_names for admin_col in admin_cols]
        type_ = {
            'name': tab.name,
            'type': {
                'name': 'Struct',
                'struct': {
                    'fields': [
                        column(
                            col,
                            min=t.cast(t.Optional[str], min_max_possible_values[col.name]["min"]),
                            max=t.cast(t.Optional[str], min_max_possible_values[col.name]["max"]),
                            possible_values=t.cast(t.List[str], min_max_possible_values[col.name]["possible_values"])
                        ) for col in tab.columns
                        if col.name not in admin_cols
                    ],
                },
                'properties': {},
            }
        }
        return TableType(type_, all(has_admin))

    def compute_min_max_possible_values(tab: sa.Table) -> Dict[str, Dict[str, Union[t.Optional[str], List[str]]]]:
        """Send 3 SQL queries for loading the bounds"""
        values: Dict[str, Dict[str, Union[t.Optional[str], List[str]]]] = {
            col.name: {
                'min': None,
                'max': None,
                'possible_values': []
            } for col in tab.columns
        }
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
            min_query = sa.select(*[sa.func.cast(sa.func.min(col), sa.String).label(col.name) for col in interval_cols]).select_from(tab)
            max_query = sa.select(*[sa.func.cast(sa.func.max(col), sa.String).label(col.name) for col in interval_cols]).select_from(tab)

            with engine.connect() as conn:
                min_results = conn.execute(min_query).fetchone()
                max_results = conn.execute(max_query).fetchone()

            for col, min_val, max_val in zip(
                    interval_cols, t.cast(t.Iterable[t.Any], min_results), t.cast(t.Iterable[t.Any], max_results)
                ):
                values[col.name]['min'] = min_val
                values[col.name]['max'] = max_val

        if possible_values_threshold is not None and len(interval_cols) != 0:
            tablename = f"\"{tab.name}\"" if tab.schema is None else f"\"{tab.schema}\".\"{tab.name}\""
            values_query = sa.text(
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

            for col, possible_values in zip(interval_cols, t.cast(t.Iterable[t.Any], values_results)):
                values[col.name]['possible_values'] = [str(v) for v in possible_values]

        return values

    def column(col: sa.Column, min:Optional[str]=None, max:Optional[str]=None, possible_values:List[str]=[]) -> dict:
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
            min = f'{-MAX_NUMERIC_RANGE}' if min is None else min
            max = f'{MAX_NUMERIC_RANGE}' if max is None else max
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

    def table_size(tab: sa.Table) -> dict:
        with engine.connect() as conn:
            result = conn.execute(sa.select(sa.func.count()).select_from(tab))
            size = t.cast(int, result.scalar())
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

    def column_size(col: sa.Column, size: int, multiplicity: float) -> dict:
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
                                "max": f'{MAX_NUMERIC_RANGE}',
                                "min": f'{-MAX_NUMERIC_RANGE}',
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
    dataset_dict, schema_dict, size_dict = dataset_schema_size()
    # Display when debugging
    logging.debug(json.dumps(dataset_dict))
    logging.debug(json.dumps(schema_dict))
    logging.debug(json.dumps(size_dict))
    # Return the result
    return Dataset.from_str(json.dumps(dataset_dict), json.dumps(schema_dict), json.dumps(size_dict))


def schema_data_type(schema_dict: dict) -> dict:
    schema_type_ = schema_dict['type']
    if "stuct" in schema_type_:
        return schema_type_["struct"]["fields"][0]['type']
    return schema_type_

def has_schema_name(schema_dict: dict, schema_name: str) -> bool:
    """It checks if the dataset schema has an sql schema called as schema_name."""
    fields = schema_data_type(schema_dict)['union']['fields']
    for field in fields:
        if field["name"] == schema_name and "union" in field["type"]:
            return True
    return False


@dataclass
class TableType:
    type_: dict
    has_admin: bool


# A method to get select a schema
def schema(dataset: Dataset, schema: str) -> 'Schema':
    return Schema(dataset, schema)


@dataclass
class Schema:
    dataset: Dataset
    schema: str

    def __getattr__(self, table: str) -> 'Table':
        return Table(self.dataset, self.schema, table)

@dataclass
class Table:
    dataset: Dataset
    schema: t.Optional[str]
    table: str

    def __getattr__(self, column: str) -> 'Column':
        return Column(self.dataset, self.schema, self.table, column)

    def relation(self) -> Relation:
        return next(
            rel
            for (path, rel) in self.dataset.relations()
            if path[1:]==self.path()
        )
    
    def path(self) -> t.List[str]:
        if self.schema:
            return [self.schema, self.table]
        else:
            return [self.table]

@dataclass
class Column:
    dataset: Dataset
    schema: t.Optional[str]
    table: str
    column: str

    def with_range(self, min:float, max:float) -> Dataset:
        return self.dataset.with_range(self.schema, self.table, self.column, min, max)
    
    def with_possible_values(self, with_possible_values: List[str]) -> Dataset:
        return self.dataset.with_possible_values(self.schema, self.table, self.column, with_possible_values)
    
    def with_constraint(self, constraint: t.Optional[str]) -> Dataset:
        return self.dataset.with_constraint(self.schema, self.table, self.column, constraint)

    def with_unique_constraint(self) -> Dataset:
        return self.with_constraint(Dataset.CONSTRAINT_UNIQUE)
    
    def with_no_constraint(self) -> Dataset:
        return self.with_constraint(None)
    