'''Database schema.'''

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict

import glog
from sqlalchemy import Column, engine_from_config, ForeignKey, JSON, Integer, String, Table
from sqlalchemy.orm import registry, relationship

from housing.configs import config
from housing.data.address import Address
from housing.data import utils

mapper_registry = registry()

DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'


@mapper_registry.mapped
@dataclass
class Unit:
    '''All data intrinstic to physical unit itself.

    Can be shared by multiple listings.
    '''

    __table__ = Table(
        'housing_units',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('address_str', String(100), nullable=False, unique=True, index=True),
        Column('bedrooms', Integer, nullable=False),
        Column('other_info', JSON),
    )

    id: int = field(init=False)
    address_str: str  # For storing in the DB.
    address: Address  # For working with in python
    bedrooms: config.BedroomCount
    other_info: Optional[Dict] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@mapper_registry.mapped
@dataclass
class Listing:
    '''Single listing of a given unit.'''

    __table__ = Table(
        'housing_listings',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('unit_id', Integer, ForeignKey('housing_units.id'), nullable=False, index=True),
        Column('price', Integer, nullable=False),
        Column('source', String[50], nullable=False)
    )

    id: int = field(init=False)
    unit: Unit
    price: int
    source: str

    def to_dict(self) -> Dict:
        return asdict(self)


def main():
    '''If this file is run directly, setup schema in DB.'''
    engine = utils.create_db_engine()
    mapper_registry.metadata.create_all(engine)
    glog.info('Created schema!')


if __name__ == '__main__':
    main()