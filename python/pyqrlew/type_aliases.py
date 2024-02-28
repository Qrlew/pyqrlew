"""Module containing type aliases"""
import typing as t

PrivacyUnit = t.Sequence[t.Tuple[str, t.Sequence[t.Tuple[str, str, str]], str]]
SyntheticData = t.Sequence[t.Tuple[t.Sequence[str], t.Sequence[str]]]
