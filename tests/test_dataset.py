import os
import numpy as np
import pandas as pd
from pyqrlew.io import PostgreSQL
from pyqrlew import Dialect

def test_queries_consistency(queries):
    """Test the consistency of results for queries stored in a file."""
    database = PostgreSQL()
    dataset = database.extract() # load the db
    dataset.w