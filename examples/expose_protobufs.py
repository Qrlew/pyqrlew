import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL
from pyqrlew.io.dataset import dataset_from_database

logging.basicConfig(level=logging.DEBUG)

db = PostgreSQL()
ds = db.extract()