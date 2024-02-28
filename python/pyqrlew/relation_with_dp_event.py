"""Module with the definition of some rust objects that are not exposed to
python. This is only for documentation purposes."""
import pyqrlew as qrl
import typing as t



class DpEvent:
    """Internal object containing a description of differentially private
    mechanisms such as Laplace and Gaussian, etc, and their composition.
    This is compatible with dp-accounting, part of the Google differential
    privacy library, containing tools for tracking differential privacy
    budgets.
    """

    def to_dict(self) -> t.Mapping[str, t.Union[str, float]]:
        """Returns a Dict representation of DP mechanisms."""
        pass

    def to_named_tuple(self) -> t.NamedTuple:
        """Returns NamedTuple of DP mechanisms compatible with `dp-accounting`
        """
        pass


class RelationWithDpEvent:
    """Internal object containing a differentially private (DP) or privacy unit
    preserving (PUP) relation and the associated DpEvent."""

    def relation(self) -> qrl.Relation:
        """Returns the DP or PUP relation.
        """
        pass


    def dp_event(self) -> DpEvent:
        """Returns the DpEvent associated with the relation."""
        pass

