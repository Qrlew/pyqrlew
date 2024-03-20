import shutil
from pathlib import Path
from sqlalchemy import select, column, table, text
from qrlew_datasets.files import SQL  # type: ignore  # module is installed, but missing library stubs or py.typed marker 
from qrlew_datasets.databases import PostgreSQL as EmptyPostgreSQL  # type: ignore  # module is installed, but missing library stubs or py.typed marker 
from pyqrlew import dataset_from_database, Dataset

NAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: int = 5433

POSSIBLE_VALUES_THRESHOLD: int = 20

class PostgreSQL(EmptyPostgreSQL):
    """
    A class representing a PostgreSQL database with PyQrlew integration.

    This class provides methods for interacting with an PostgreSQL database
    using the PyQrlew library.

    Example
    ----------
        >>> import pyqrlew as qrl
        >>> from pyqrlew.io import PostgreSQL
        >>> import pandas as pd
        >>> DB = PostgreSQL() # Setup a default database
        >>> DB.load_extract() # Insert a demo dataset
        >>> dataset = Dataset.from_database(name='extract', engine=DB.engine(), schema_name='extract')
        >>> dataset.relations()
        [(['extract', 'extract', 'beacon'], <Relation at 0x784b78176fd0>),
        (['extract', 'extract', 'census'], <Relation at 0x784b78177490>)]
        >>> dataset.extract.census.relation().schema()
        {age: int, workclass: str, fnlwgt: str, education: str, education_num: int, marital_status: str, occupation: str, relationship: str, race: str, sex: str, capital_gain: int, capital_loss: int, hours_per_week: int, native_country: str, income: str}
        >>> query = "SELECT AVG(age) AS avg_age FROM extract.census"
        >>> pd.read_sql(query, DB.engine())
        	avg_age
        0	48.005155
    """
    def __init__(self, name:str=NAME, user:str=USER, password:str=PASSWORD, port:int=PORT) -> None:
        """Initialize an SQLite instance.

        Args:
            name (str, optional): The name of the instance. Defaults to NAME.
            user (str, optional): user name in the database. Defaults to USER.
            password (str, optional): password used to connect to the service.
                Defaults to PASSWORD.
            port (int, optional): port number at which the database listens.
                Defaults to PORT.
        """
        super().__init__(name, user, password, port)

    def load_resource(self, schema: str, src: Path) -> 'PostgreSQL':
        """It loads sql data resource into the specified schema of the
        PostgreSQL database. It checks if the schema exists, and if not, copies
        the resource to a temporary location and then loads it into
        the database.

        Args:
            schema (str): The name of the database schema where
                the resource is to be loaded.
            src (Path): The file path of the resource to be
                loaded (an sql script) into the database.

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        with self.engine().connect() as conn:
            res = list(
                conn.execute(
                    select(column('schema_name'))
                    .select_from(table('schemata', schema='information_schema'))
                    .where(column('schema_name') == schema))
            )
            schema_exists = len(res)==1
        if not schema_exists:
            dst = Path('/tmp') / schema
            shutil.copyfile(src, dst)
            self.load(dst)
        return self

    def load_extract(self) -> 'PostgreSQL':
        """It loads the 'extract' dataset in the 'extract' schema.
        It internally calls the load_resource method 

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        return self.load_resource('extract', SQL('extract').local())

    def load_financial(self) -> 'PostgreSQL':
        """It loads the 'financial' dataset in the 'financial' schema.
        It internally calls the load_resource method 

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        return self.load_resource('financial', SQL('financial').local())

    def load_hepatitis(self) -> 'PostgreSQL':
        """It loads the 'hepatitis' dataset in the 'hepatitis_std' schema.
        It internally calls the load_resource method 

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        return self.load_resource('hepatitis_std', SQL('hepatitis').local())

    def load_imdb(self) -> 'PostgreSQL':
        """It loads the 'imdb' dataset in the 'imdb_ijs' schema.
        It internally calls the load_resource method 

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        return self.load_resource('imdb_ijs', SQL('imdb').local())

    def load_retail(self) -> 'PostgreSQL':
        """It loads the 'retail' dataset in the 'retail' schema.
        It internally calls the load_resource method 

        Returns:
            PostgreSQL: The instance of the PostgreSQL class,
            allowing for method chaining.
        """
        return self.load_resource('retail', SQL('retail').local())

    def extract(self) -> Dataset:
        """It loads the extract dataset in the database and it returns a qrlew
        Dataset with data ranges and categorical possible values if the unique
        column values are less than POSSIBLE_VALUES_THRESHOLD.
        """
        self.load_extract()
        return dataset_from_database('extract', self.engine(), 'extract', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def financial(self) -> Dataset:
        """It loads the financial dataset in the database and it returns a qrlew
        Dataset with data ranges and categorical possible values if the unique
        column values are less than POSSIBLE_VALUES_THRESHOLD.
        """
        self.load_financial()
        return dataset_from_database('financial', self.engine(), 'financial', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def hepatitis(self) -> Dataset:
        """It loads the hepatitis dataset in the database and it returns a qrlew
        Dataset with data ranges and categorical possible values if the unique
        column values are less than POSSIBLE_VALUES_THRESHOLD.
        """
        self.load_hepatitis()
        return dataset_from_database('hepatitis_std', self.engine(), 'hepatitis_std', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def imdb(self) -> Dataset:
        """It loads the imdb dataset in the database and it returns a qrlew
        Dataset with data ranges and categorical possible values if the unique
        column values are less than POSSIBLE_VALUES_THRESHOLD.
        """
        self.load_imdb()
        return dataset_from_database('imdb_ijs', self.engine(), 'imdb_ijs', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)

    def retail(self) -> Dataset:
        """It loads the retail dataset in the database and it returns a qrlew
        Dataset with data ranges and categorical possible values if the unique
        column values are less than POSSIBLE_VALUES_THRESHOLD.
        """
        self.load_retail()
        return dataset_from_database('retail', self.engine(), 'retail', ranges=True, possible_values_threshold=POSSIBLE_VALUES_THRESHOLD)
