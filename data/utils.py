'''Utils for interacting with DB.'''

from typing import List

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL, Connection
import yaml

from housing.data.address import Address
from housing.data.schema import Listing, Unit

DB_DRIVER_NAME = 'postgresql'
DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'

GET_ALL_LISTINGS_QUERY = '''
    select
        units.address_str,
        units.bedrooms,
        listings.created_at as listing_created_at,
        listings.source,
        listings.price,
        listings.url
    from housing_units units join housing_listings listings on units.id = listings.unit_id 
'''

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


def all_listings_data(engine: Engine) -> List[Listing]:
    '''Fetch all listings in DB join with their unit data.'''
    connection = Connection(engine)
    rows = connection.execute(GET_ALL_LISTINGS_QUERY).all()
    results = []
    for row in rows:
        address = Address.from_string(row['address_str'])
        unit = Unit(
            address_str=row['address_str'],
            address=address,
            bedrooms=row['bedrooms']
        )
        listing = Listing(
            unit=unit,
            price=row['price'],
            source=row['source'],
            url=row['url']
        )
        results.append(listing)
    return results