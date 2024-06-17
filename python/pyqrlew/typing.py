"""Module with the definition of some rust objects that are not exposed to
python. This is only for documentation purposes."""
import typing as t

PrivacyUnit = t.Union[
    t.Sequence[t.Tuple[str, t.Sequence[t.Tuple[str, str, str]], str]],
    t.Tuple[t.Sequence[t.Tuple[str, t.Sequence[t.Tuple[str, str, str]], str]], bool],
    t.Tuple[t.Sequence[t.Tuple[str, t.Sequence[t.Tuple[str, str, str]], str, str]], bool]
]
SyntheticData = t.Sequence[t.Tuple[t.Sequence[str], t.Sequence[str]]]
DPEvent = t.Mapping[str, t.Union[str, float, t.Sequence['DPEvent']]]

class DpEvent(t.Protocol):
    """Internal object containing a description of differentially private
    mechanisms such as Laplace and Gaussian, etc, and their composition.
    This is compatible with dp-accounting, part of the Google differential
    privacy library, containing tools for tracking differential privacy
    budgets.
    """

    def to_dict(self) -> DPEvent:
        """Returns a Dict representation of DP mechanisms."""
        ...

    def to_named_tuple(self) -> t.NamedTuple:
        """Returns NamedTuple of DP mechanisms compatible with `dp-accounting`
        """
        ...
