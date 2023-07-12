from typing import Optional
from sqlalchemy import create_engine, Engine

DB: str = "qrlew-test"
PORT: int = 5432
USER: str = "postgres"
PASSWORD: str = "qrlew-test"

class Database:
    def __init__(self, name: str) -> None:
        pass
    
    @classmethod
    def try_get_existing(cls) -> Optional[Engine]:
        try:
            return create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@localhost:{PORT}/{DB}")
        except Exception:
            return None
