from importlib.resources import files
import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL

logging.basicConfig(level=logging.DEBUG)

db = PostgreSQL()

ds = db.financial()
# ds = db.imdb()
# ds = db.hepatitis()

print(ds)
print(ds.relations())

# t = qrl.Dataset(dataset, schema, size)

# print(t.relations())

# print(t.sql("select * from campaign_descriptions").dot())
