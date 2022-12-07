'''Database schema.'''

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict

import requests
import glog
from sqlalchemy import Column, engine_from_config, ForeignKey, JSON, Integer, String, Table, DateTime, Boolean
from sqlalchemy.sql import func, expression
from sqlalchemy.orm import registry, relationship

from housing.configs import config
from housing.data.address import Address
from housing.data.db_client import DbClient

mapper_registry = registry()

DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'
IP_SERVICE_URL = 'https://api.ipify.org'


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


@mapper_registry.mapped
@dataclass
class IpAddress:
    '''An IP from which tracked requests are made.'''

    __table__ = Table(
        'housing_ips',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ip', String[50], unique=True, nullable=False, index=True),
        Column('created_at', DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column('description', String[50], nullable=False),
        Column('is_valid', Boolean, nullable=False, server_default=expression.text('true')),
    )

    id: int = field(init=False)
    ip: str
    description: str
    is_valid: bool = True


    @classmethod
    def my_ip(cls) -> str:
        '''Returns current machine IP.'''
        ip_service_response = requests.get(IP_SERVICE_URL)
        ip_service_response.raise_for_status()
        return ip_service_response.text



@mapper_registry.mapped
@dataclass
class Request:
    '''Tracked request.'''

    __table__ = Table(
        'housing_requests',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ip_id', ForeignKey('housing_ips.id'), nullable=False, index=True),
        Column('created_at', DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column('finished_at', DateTime(timezone=True)),
        Column('domain', String[50], nullable=False),
        Column('endpoint', String[50], nullable=False),
        Column('environment', String[50], nullable=False),
        Column('status_code', Integer),
    )

    id: int = field(init=False)
    ip: IpAddress
    domain: str
    endpoint: str
    environment: str
    status_code: Optional[int] = None


def main():
    '''If this file is run directly, setup schema in DB.'''
    client = DbClient()
    mapper_registry.metadata.create_all(client.engine)
    glog.info('Created schema!')


if __name__ == '__main__':
    main()