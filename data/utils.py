'''Utils for interacting with DB.'''

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
import yaml

DB_DRIVER_NAME = 'postgresql'
DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'

def create_db_engine(keyfile_path: str = DB_KEYFILE_PATH) -> Engine:
    engine = None
    with open(keyfile_path, 'r') as file:
        try:
            db_config = yaml.safe_load(file)
            db_url = URL.create(
                drivername=DB_DRIVER_NAME,
                username=db_config['user'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['db']
            )
            engine = create_engine(db_url)
        except Exception as e:
            raise RuntimeError(f'Error connecting to postgres') from e

    return engine