from importlib.resources import files
import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL

logging.basicConfig(level=logging.DEBUG)

db = PostgreSQL()
db.financial()

# with files(qrl).joinpath('data/retail_demo/dataset.json').open('r') as f:
#     dataset = f.read()

# with files(qrl).joinpath('data/retail_demo/schema.json').open('r') as f:
#     schema = f.read()

# with files(qrl).joinpath('data/retail_demo/size.json').open('r') as f:
#     size = f.read()

# t = qrl.Dataset(dataset, schema, size)

# print(t.relations())

# print(t.sql("select * from campaign_descriptions").dot())
