import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL
from pyqrlew import dataset_from_database

logging.basicConfig(level=logging.INFO)

db = PostgreSQL()
ds = db.extract()