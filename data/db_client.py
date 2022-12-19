'''Client for interacting with the DB.'''

from typing import List, Dict, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL, Connection
from sqlalchemy.orm import sessionmaker, Session
import yaml

DB_DRIVER_NAME = 'postgresql'
DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'

class DbClient:
    '''Client for interacting with the DB.'''

    def __init__(self, keyfile_path: str = DB_KEYFILE_PATH):
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
                self.engine = create_engine(db_url)
            except Exception as e:
                raise RuntimeError(f'Error connecting to postgres') from e


    def session(self) -> Session:
        return sessionmaker(bind=self.engine)()


    def query(self, query: str, params: Dict[str, Any]) -> List[Dict]:
        '''Run a query on the DB.'''
        connection = Connection(self.engine)
        return connection.execute(text(query), params).all()