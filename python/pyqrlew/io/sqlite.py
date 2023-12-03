import os
import sqlalchemy
from qrlew_datasets.database import Database
import pyqrlew as qrl
from pyqrlew.io.database import dataset
import pandas as pd

dtype_to_sqlalchemy = {
    'object': sqlalchemy.String,
    'int64': sqlalchemy.Integer,
    'float64': sqlalchemy.Float,
    'bool': sqlalchemy.Boolean,
    'datetime64[ns]': sqlalchemy.DateTime,
    'timedelta64[ns]': sqlalchemy.Interval,
    'category': sqlalchemy.String
}


class SQLite(Database):
    """
    A class representing an SQLite database with PyQrlew integration.

    This class provides methods for interacting with an SQLite database using the
    PyQrlew library.

    Parameters
    ----------
        db_file: str
            The path to the SQLite database file.

    Attributes
    ----------
        db_file: str
            The path to the SQLite database file.

    Example
    ----------
        >>> import pandas as pd
        >>> qdb = SQLite("my_database.db")
        >>> df = pd.DataFrame([[1, "a", True], [2, "b", False]], columns=["col1", "col2", "col3"])
        >>> qdb.load_pandas("table1", df)
        >>> qdb.print_infos_metadata()
        Table: table1
        - col1, Type: BIGINT
        - col2, Type: TEXT
        - col3, Type: BOOLEAN
        >>> qdb.dataset().relations()
        [(['table1'], <Relation at 0x120f47120>)]
        >>> query = "SELECT SUM(col1) FROM table1"
        >>> qdb.execute(query)
        [(3,)]
        >>> relation = qdb.dataset().sql(query)
        >>> qdb.eval(relation)
        [(3,)]
    """

    def __init__(self, db_file:str) -> None:
        """
        Initialize an SQLite instance.

        Parameters
        ----------
            db_file: str
                The path to the SQLite database file.

        Returns
        ----------
            None
        """
        self.db_file = db_file

    def load_csv(self, table_name: str, csv_file: str) -> None:
        """
        Create a table in the SQLite database from data stored in a csv file.

        Parameters
        ----------
            table_name: str
                Name of the created table

            csv_file: str
                Path or URL to the CSV file containing the data.

        Returns
        ----------
            None

        """
        df = pd.read_csv(csv_file)
        self.load_pandas(table_name, df)

    def load_pandas(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Create a table in the SQLite database from data stored in a Pandas DataFrame.

        Parameters
        ----------
            table_name: str
                Name of the created table

            df: :class:`pandas.DataFrame`
                Data represented as a pandas DataFrame

        Returns
        ----------
            None

        """
        column_types = {
            column: dtype_to_sqlalchemy[str(df[column].dtype)]
            for column in df.columns
        }
        metadata = sqlalchemy.MetaData()
        columns = [
            sqlalchemy.Column(col_name, col_type)
            for col_name, col_type in column_types.items()
        ]
        _ = sqlalchemy.Table(table_name, metadata, *columns)
        engine = self.engine()
        metadata.create_all(engine)
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    def url(self) -> str:
        """Get the URL string of the SQLite database."""
        return f'sqlite:///{self.db_file}'

    def remove(self) -> None:
        """Remove the SQLite database file from the filesystem."""
        os.remove(self.db_file)

    def engine(self) -> sqlalchemy.engine.Engine:
        """Create and return an SQLAlchemy Engine instance for the SQLite database."""
        return sqlalchemy.create_engine(self.url(), echo=True)

    def eval(self, relation: qrl.Relation) -> list:
        """Convert a PyQrlew relation into a sql query string, send it to the database and return the result."""
        return self.execute(relation.render())

    def execute(self, query: str) -> list:
        """Execute a raw SQL query and return the result."""
        with self.engine().connect() as conn:
            result = conn.execute(sqlalchemy.text(query)).all()
        return result

    def dataset(self) -> qrl.Dataset:
        """Create and return a PyQrlew Dataset linked to the SQLite database."""
        return dataset(self.db_file, self.engine())

    def print_infos_metadata(self) -> None:
        """Print metadata information about tables and columns in the database."""
        metadata = sqlalchemy.MetaData()
        metadata.reflect(self.engine(), schema=None)
        for table_name, table in metadata.tables.items():
            print(f"\nTable: {table_name}")
            for column in table.c:
                print(f" - {column.name}, Type: {column.type}")
