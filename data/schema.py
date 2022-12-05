'''Database schema.'''

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict

import glog
from sqlalchemy import Column, engine_from_config, ForeignKey, JSON, Integer, String, Table, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import registry, relationship

from housing.configs import config
from housing.data.address import Address
from housing.data.db_client import DbClient

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
        Column('created_at', DateTime(timezone=True), nullable=False, server_default=func.now()),
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
        result = asdict(self)
        result['address'] = self.address.to_dict()
        return result


@mapper_registry.mapped
@dataclass
class Listing:
    '''Single listing of a given unit.'''

    __table__ = Table(
        'housing_listings',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('unit_id', Integer, ForeignKey('housing_units.id'), nullable=False, index=True),
        Column('created_at', DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column('source', String[50], nullable=False),
        Column('price', Integer, nullable=False),
        Column('url', String[100], nullable=False),
    )

    id: int = field(init=False)
    unit: Unit
    price: int
    source: str
    url: str

    def to_dict(self) -> Dict:
        result = asdict(self)
        result['unit'] = self.unit.to_dict()
        return result


def main():
    '''If this file is run directly, setup schema in DB.'''
    client = DbClient()
    mapper_registry.metadata.create_all(client.engine)
    glog.info('Created schema!')


if __name__ == '__main__':
    main()