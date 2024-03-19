import typing as t
from .sqlite import SQLite
import pandas as pd # type: ignore

def from_csv(table_name: str, csv_file: str, db_name:str ="my_sqlite.db", ranges: bool=False) -> 'SQLite':
    """
    Construct a PyQrlew Database from a csv file.
    Its uses SQLite for storing the data. The data is cloned into Sqlite.

    Parameters
    ----------
    table_name : str
        Name of the table that is created
    csv_file : str
        Any valid string path is acceptable. The string could be a URL.
    db_name : str, default "my_sqlite.db"
        Name of the file where the database is stored
    ranges: bool
        Query the database for fetching min and max of each column of type integer, float, date/time and string and use then as bounds

    Returns
    -------
    None

    Example
    --------
    >>> import pyqrlew as pq
    >>> qdb = pq.from_csv(
            table_name="heart_data",
            csv_file="https://raw.githubusercontent.com/vserret/datasets/main/heart_data.csv"
        )
    >>> qdb.print_infos_metadata()
        Table: heart_data
         - id, Type: BIGINT
         - age, Type: BIGINT
         - gender, Type: BIGINT
         - height, Type: BIGINT
         - weight, Type: FLOAT
         - ap_hi, Type: BIGINT
         - ap_lo, Type: BIGINT
         - cholesterol, Type: BIGINT
         - gluc, Type: BIGINT
         - smoke, Type: BIGINT
         - alco, Type: BIGINT
         - active, Type: BIGINT
         - cardio, Type: BIGINT
    """
    db = SQLite(db_name, ranges)
    db.load_csv(table_name, csv_file)
    return db

def from_csv_dict(csv_dict: t.Dict[str, str], db_name:str ="my_sqlite.db", ranges: bool=False) -> 'SQLite':
    """
    Construct a PyQrlew Database from a dictionary whose keys are the table names and values the
    corresponding data stored in csv files.
    Its uses SQLite for storing the data. The data is cloned into Sqlite.

    Parameters
    ----------
    data_dict : Dict[str, str]
        For each item, the key is the name of the table that is created
        and the value is the name (path or url) of the csv file where the data is stored
    db_name : str, default "my_sqlite.db"
        Name of the file where the database is stored
    ranges: bool
        Query the database for fetching min and max of each column of type integer, float, date/time and string and use then as bounds

    Returns
    -------
    None

    Example
    --------
    >>> import pyqrlew as pq
    >>> import pandas as pd
    >>> df = pd.DataFrame([[1, "a", True], [2, "b", False]], columns=["col1", "col2", "col3"])
    >>> df.to_csv("table2.csv")
    >>> qdb = pq.from_csv_dict(
        {
            "heart_data": "https://raw.githubusercontent.com/vserret/datasets/main/heart_data.csv",
            "table2": "table2.csv"
        }
    )
    >>> qdb.print_infos_metadata()
        Table: heart_data
        - id, Type: BIGINT
        - age, Type: BIGINT
        - gender, Type: BIGINT
        - height, Type: BIGINT
        - weight, Type: FLOAT
        - ap_hi, Type: BIGINT
        - ap_lo, Type: BIGINT
        - cholesterol, Type: BIGINT
        - gluc, Type: BIGINT
        - smoke, Type: BIGINT
        - alco, Type: BIGINT
        - active, Type: BIGINT
        - cardio, Type: BIGINT

        Table: table2
        - Unnamed: 0, Type: BIGINT
        - col1, Type: BIGINT
        - col2, Type: TEXT
        - col3, Type: BOOLEAN
    """
    db = SQLite(db_name, ranges)
    
    for table_name, csv_file in csv_dict.items():
        db.load_csv(table_name, csv_file)

    return db

def from_pandas(table_name: str, data: pd.DataFrame, db_name:str ="my_sqlite.db", ranges: bool=False) -> 'SQLite':
    """
    Construct a PyQrlew Database from a pandas DataFrame.
    Its uses SQLite for storing the data. The data is cloned into Sqlite.

    Parameters
    ----------
    table_name : str
        Name of the table that is created
    data : :class:`pandas.DataFrame`
        Data represented as a pandas DataFrame
    db_name : str, default "my_sqlite.db"
        Name of the file where the database is stored
    ranges: bool
        Query the database for fetching min and max of each column of type integer, float, date/time and string and use then as bounds

    Returns
    -------
    None

    Example
    --------
    >>> import pyqrlew as pq
    >>> import pandas as pd
    >>> df = pd.DataFrame([[1, "a", True], [2, "b", False]], columns=["col1", "col2", "col3"])
    >>> qdb = pq.from_pandas("my_table", df)
    >>> qdb.print_infos_metadata()
        Table: my_table
         - col1, Type: BIGINT
         - col2, Type: TEXT
         - col3, Type: BOOLEAN

    """
    db = SQLite(db_name, ranges)
    db.load_pandas(table_name, data)
    return db

def from_pandas_dict(data_dict: t.Dict[str, pd.DataFrame], db_name:str ="my_sqlite.db", ranges: bool=False) -> 'SQLite':
    """
    Construct a PyQrlew Database from a dictionary whose keys are the table names and values the
    corresponding data stored in a pandas DataFrame.
    Its uses SQLite for storing the data. The data is cloned into Sqlite.

    Parameters
    ----------
    data_dict : Dict[str, :class:`pandas.DataFrame`]
        For each item, the key is the name of the table that is created
        and the value is the data represented as a pandas DataFrame
    db_name : str, default "my_sqlite.db"
        Name of the file where the database is stored
    ranges: bool
        Query the database for fetching min and max of each column of type integer, float, date/time and string and use then as bounds

    Returns
    -------
    None

    Example
    --------
    >>> import pyqrlew as pq
    >>> import pandas as pd
    >>> df1 = pd.DataFrame([[1, "a", True], [2, "b", False]], columns=["col1", "col2", "col3"])
    >>> df2 = pd.DataFrame([[1, 2], [3, 4], [5, 6]], columns=["colA", "colB"])
    >>> qdb = pq.from_pandas_dict({"table1": df1, "table2": df2})
    >>> qdb.print_infos_metadata()
        Table: table1
        - col1, Type: BIGINT
        - col2, Type: TEXT
        - col3, Type: BOOLEAN

        Table: table2
        - colA, Type: BIGINT
        - colB, Type: BIGINT
    """
    db = SQLite(db_name, ranges)
    for table_name, df in data_dict.items():
        db.load_pandas(table_name, df)
    return db
