import os

URI = "postgresql+psycopg2://postgres:1234@localhost:5432/test_database"

DIRNAME = os.path.join(os.getcwd(), os.path.dirname(__file__))
DATASET_FILENAME = os.path.join(DIRNAME, 'dataset.json')
SCHEMA_FILENAME = os.path.join(DIRNAME, 'schema.json')
BOUNDS_FILENAME = os.path.join(DIRNAME, 'bounds.json')
SIZE_FILENAME = os.path.join(DIRNAME, 'size.json')
