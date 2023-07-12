from typing import Protocol
from collections.abc import Collection

class Database(Protocol):
    def name(self) -> str:
        """The name of the DB"""
        ...
    
    def tables(self) -> Collection[str]:
        """List the tables in the DB"""
        ...
    