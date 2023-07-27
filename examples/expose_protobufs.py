import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL
from pyqrlew.io.database import dataset

logging.basicConfig(level=logging.DEBUG)

db = PostgreSQL()
ds = db.extract()