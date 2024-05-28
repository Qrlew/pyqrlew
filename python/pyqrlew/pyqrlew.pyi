from __future__ import annotations
import typing as t
import enum

from pyqrlew.typing import PrivacyUnit, SyntheticData

class _Dataset:
    """Class that ...."""
    def __new__(self, dataset: str, schema: str, size: str) -> '_Dataset': ...
    @property
    def schema(self) -> str: ...
    @property
    def size(self) -> t.Optional[str]: ...
    def with_range(self, schema_name: str, table_name: str, field_name: str, min: float, max: float) -> '_Dataset': ...
    def with_possible_values(self, schema_name: str, table_name: str, field_name: str, possible_values: t.Iterable[str]) -> '_Dataset': ...
    def with_constraint(self, schema_name: str, table_name: str, field_name: str, constraint: t.Optional[str]) -> '_Dataset': ...
    def relations(self) -> t.Iterable[t.Tuple[t.List[str], '_Relation']]: ...
    def relation(self, query: str, dialect: t.Optional['Dialect']) -> '_Relation': ...
    def from_queries(self, queries: t.Iterable[t.Tuple[t.Iterable[str], str]], dialect: t.Optional['Dialect']) -> '_Dataset': ...
    def __str__(self) -> str: ...


class _Relation:
    """Class that... """
    @staticmethod
    def from_query(query: str, dataset: _Dataset, dialect: t.Optional['Dialect']) -> '_Relation': ...
    def __str__(self) -> str: ...
    def dot(self) -> str: ...
    def schema(self) -> str: ...
    def to_query(self, dialect: t.Optional['Dialect']=None) -> str: ...
    def rewrite_as_privacy_unit_preserving(
        self,
        dataset: _Dataset,
        privacy_unit: PrivacyUnit,
        epsilon_delta: t.Dict[str, float],
        max_multiplicity: t.Optional[float]=None,
        max_multiplicity_share: t.Optional[float]=None,
        synthetic_data: t.Optional[SyntheticData]=None,
        strategy: t.Optional['Strategy']=None
    ) -> 'RelationWithDpEvent': ...
    def rewrite_with_differential_privacy(
        self,
        dataset: _Dataset,
        privacy_unit: PrivacyUnit,
        epsilon_delta: t.Dict[str, float],
        max_multiplicity: t.Optional[float]=None,
        max_multiplicity_share: t.Optional[float]=None,
        synthetic_data: t.Optional[SyntheticData]=None
    ) -> 'RelationWithDpEvent': ...


class RelationWithDpEvent:
    def relation(self) -> _Relation: ...
    def dp_event(self) -> DpEvent: ...


class DpEvent:
    def to_dict(self) -> t.Mapping[str, t.Union[str, float]]: ...
    def to_named_tuple(self) -> t.NamedTuple: ...


class Dialect(enum.Enum):
    """Supported dialects"""
    PostgreSql=1
    MsSql=2
    BigQuery=3

class Strategy(enum.Enum):
    Soft=1
    Hard=2