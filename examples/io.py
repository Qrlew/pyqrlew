import pyqrlew as qrl
from pyqrlew.io import Database
from pyqrlew.io.postgresql import Database as PostgreSQL

database: Database = PostgreSQL()

print(database)

print(PostgreSQL.try_get_existing())