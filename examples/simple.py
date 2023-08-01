import logging
import pyqrlew as qrl
from pyqrlew.io import PostgreSQL

logging.basicConfig(level=logging.INFO)

db = PostgreSQL()

ds = db.extract()
# ds = db.financial()
# ds = db.imdb()
# ds = db.hepatitis()

for path, relation in ds.relations():
    print(relation.render())
    for row in db.eval(relation):
        print(row)

print(ds.sql("select * from census").render())
