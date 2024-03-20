"""Module with the definition of some rust objects that are not exposed to
python. This is only for documentation purposes."""
from .pyqrlew import _Relation
import typing as t

PrivacyUnit = t.Sequence[t.Tuple[str, t.Sequence[t.Tuple[str, str, str]], str]]
SyntheticData = t.Sequence[t.Tuple[t.Sequence[str], t.Sequence[str]]]


class DpEvent(t.Protocol):
    """Internal object containing a description of differentially private
    mechanisms such as Laplace and Gaussian, etc, and their composition.
    This is compatible with dp-accounting, part of the Google differential
    privacy library, containing tools for tracking differential privacy
    budgets.
    """

    def to_dict(self) -> t.Mapping[str, t.Union[str, float]]:
        """Returns a Dict representation of DP mechanisms."""
        ...

    def to_named_tuple(self) -> t.NamedTuple:
        """Returns NamedTuple of DP mechanisms compatible with `dp-accounting`
        """
        ...


class RelationWithDpEvent(t.Protocol):
    """Internal object containing a differentially private (DP) or privacy unit
    preserving (PUP) relation and the associated DpEvent."""

    def relation(self) -> _Relation:
        """Returns the DP or PUP relation.
        """
        ...


    def dp_event(self) -> DpEvent:
        """Returns the DpEvent associated with the relation."""
        ...

