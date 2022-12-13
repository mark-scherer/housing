'''Database schema.'''

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict

import requests
import glog
from sqlalchemy import Column, engine_from_config, ForeignKey, JSON, Integer, String, Table, DateTime, Boolean, Float
from sqlalchemy.sql import func, expression
from sqlalchemy.orm import registry, relationship
from sqlalchemy.ext.mutable import MutableDict
from retrying import retry

from housing.configs import config
from housing.data.address import Address
from housing.data.db_client import DbClient

mapper_registry = registry()

DB_KEYFILE_PATH = '/etc/keys/postgres.yaml'
IP_SERVICE_URL = 'https://api.ipify.org'

IP_SERVICE_RETRYABLE_STATUS_CODES = [502]
def _ip_service_retryable_error(exception: Exception) -> bool:
    return isinstance(exception, requests.HttpError) and \
        exception.response.status_code in IP_SERVICE_RETRYABLE_STATUS_CODES

IP_SERVICE_RETRY = retry(
    retry_on_exception=_ip_service_retryable_error,
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
)

@mapper_registry.mapped
@dataclass
class Unit:
    '''All data intrinstic to physical unit itself.

    Can be shared by multiple listings.
    '''

    # Other info known keys
    OTHER_INFO_SQFT_KEY = 'sqft'
    OTHER_INFO_PETS_ALLOWED_KEY = 'pets_allowed'
    OTHER_INFO_PARKING_AVAILABLE_KEY = 'parking_available'

    __table__ = Table(
        'housing_units',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('created_at', DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column('address_str', String(100), nullable=False, unique=True, index=True),
        Column('zipcode',  String(10), nullable=False),  
        Column('bedrooms', Integer, nullable=False),
        Column('bathrooms', Float),
        Column('other_info', MutableDict.as_mutable(JSON)),
    )

    id: int = field(init=False)
    address_str: str  # For storing in the DB.
    address: Address  # For working with in python
    zipcode: str      # For specific queries across zipcode.
    bedrooms: config.BedroomCount
    bathrooms: float
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
    @IP_SERVICE_RETRY
    def my_ip(cls) -> str:
        '''Returns current machine IP.'''
        glog.info(f'Trying to find own IP...')
        ip_service_response = requests.get(IP_SERVICE_URL)
        ip_service_response.raise_for_status()
        ip_str = ip_service_response.text
        glog.info(f'Got own ip: {ip_str}')
        return ip_str


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
        Column('method', String[10], nullable=False),
        Column('endpoint', String[50], nullable=False),
        Column('environment', String[50], nullable=False),
        Column('request_info', MutableDict.as_mutable(JSON)),
        Column('status_code', Integer),
        Column('response_info', MutableDict.as_mutable(JSON)),
    )

    id: int = field(init=False)
    ip: IpAddress
    domain: str
    method: str
    endpoint: str
    environment: str
    request_info: Dict = field(default_factory=dict) # Additional request info.
    status_code: Optional[int] = None
    response_info: Dict = field(default_factory=dict)  # Various request-specific response info


def main():
    '''If this file is run directly, setup schema in DB.'''
    client = DbClient()
    mapper_registry.metadata.create_all(client.engine)
    glog.info('Created schema!')


if __name__ == '__main__':
    main()