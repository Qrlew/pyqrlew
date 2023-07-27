import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL

logging.basicConfig(level=logging.INFO)

db = PostgreSQL()

ds = db.extract()
# ds = db.financial()
# ds = db.imdb()
# ds = db.hepatitis()

print(ds.relations())
print(ds.sql("select * from census").dot())
