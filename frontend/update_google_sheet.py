'''Fetch data from DB and update a google sheet'''

from typing import List, Dict, NamedTuple
import json
from datetime import datetime

import glog

from housing.configs.config import Config, BedroomCount
from housing.data.db_client import DbClient
from housing.data.address import Address
from housing.frontend.google_sheets_client import GoogleSheetsClient


UNIT_LISTINGS_QUERY = '''
    with latest_prices as (
        select distinct on (unit_id)
            unit_id,
            price
        from housing_listings
        order by unit_id, created_at desc
    ) select
        units.address_str,
        units.bedrooms,
        min(listings.created_at) as first_found,
        max(listings.created_at) as last_found,
        array_agg(distinct source) as sources,
        latest_prices.price as current_price
    from housing_units units
        join housing_listings listings on units.id = listings.unit_id
        join latest_prices on units.id = latest_prices.unit_id
    group by units.address_str, units.bedrooms, latest_prices.price
'''
LISTING_TS_FORMAT = '%m/%d/%y'
SORT_HEADER = 'score'
UPDATED_AT_TS_FORMAT = '%a, %m/%d/%y %I:%M%p %Z'


class UnitListing(NamedTuple):
    '''Data contained in a sheet row: a unit with the latest listing info.
    
    The field names here should be the sanitized version of sheet headers to update!
    Field values should be exactly as they should appear in the sheet - i.e. should convert to readable strings.
    '''
    PRIMARY_KEYS = ['unit', 'address', 'zipcode']

    unit: int
    address: str  # Just the short_address portion
    zipcode: str
    current_price: int
    bedrooms: str
    sources: List[str]
    first_found: datetime
    last_found: datetime

    @classmethod
    def from_dict(cls, input: Dict) -> 'UnitListing':
        data = {key: value for key, value in input.items()}
        
        # Parse & split address.
        address = Address.from_string(data['address_str'])
        data['unit'] = address.unit_num
        data['address'] = address.short_address
        data['zipcode'] = address.zipcode
        del data['address_str']

        # Convert other fields to readable strings.
        data['bedrooms'] = str(data['bedrooms']) if data['bedrooms'] > 0 else 'studio'
        data['sources'] = ', '.join(sorted(data['sources']))
        data['first_found'] = data['first_found'].strftime(LISTING_TS_FORMAT)
        data['last_found'] = data['last_found'].strftime(LISTING_TS_FORMAT)

        return UnitListing(**data)

    def to_dict(self) -> Dict:
        '''Custom dict conversion method in case any fields need special handling.'''
        result = self._asdict()
        return result


def _bedrooms_str(config: Config) -> str:
    result = None
    scraping_params = config.scraping_params
    min_bedrooms = scraping_params.min_bedrooms
    max_bedrooms = scraping_params.max_bedrooms
    if min_bedrooms == min_bedrooms:
        result = f'{min_bedrooms} BR'
    else:
        result = f'{min_bedrooms}-{max_bedrooms} BR'
    return result


def _price_str(config: Config) -> str:
    result = None
    scraping_params = config.scraping_params
    min_price = scraping_params.min_price
    max_price = scraping_params.max_price
    if min_price == max_price:
        result = f'${min_price}'
    else:
        result = f'${min_price}-{max_price}'
    return result


def _zipcodes_str(config: Config) -> str:
    result = None
    scraping_params = config.scraping_params
    zipcodes = scraping_params.zipcodes
    return f'zipcodes: {", ".join(zipcodes)}'


def update_google_sheet(config: Config) -> None:
    '''Fetches DB data for specified config and updates its google sheet.'''

    db_client = DbClient()
    possible_headers = UnitListing._fields
    sheets_client = GoogleSheetsClient(config.sheet_id, possible_headers=possible_headers)

    # Fetch DB data
    query_data = db_client.query(UNIT_LISTINGS_QUERY)
    results = [UnitListing.from_dict(db_row) for db_row in query_data]
    glog.info(f'Fetched {len(results)} unit listings from DB for config: {config.name}')

    # Find necessary updates.
    new_sheet_data = [sheet_row.to_dict() for sheet_row in results]
    # glog.info(f'Attempting to make {len(new_sheet_data)} updates to the sheet: {json.dumps(new_sheet_data)}')
    
    # Update sheet.
    sheets_client.smart_update(_values=new_sheet_data, primary_keys=UnitListing.PRIMARY_KEYS, sort_key=SORT_HEADER)
    # sheets_client.smart_update(_values=new_sheet_data, primary_keys=UnitListing.PRIMARY_KEYS)
    glog.info(f'..updated sheet with new db data.')
    
    