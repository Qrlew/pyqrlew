import types
import sys
from pyqrlew.io.postgresql import PostgreSQL, dataset_from_database
from pyqrlew.io.sqlite import SQLite

from pyqrlew.io.utils import *

# This is for compatibility with existing notebooks where we do:
# from pyqrlew.io.dataset import dataset_from_database
dataset = types.ModuleType('pyqrlew.io.dataset')
# Populate the module with the desired function
dataset.dataset_from_database = dataset_from_database # type: ignore
# Add the module to sys.modules
sys.modules['pyqrlew.io.dataset'] = dataset
