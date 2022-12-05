'''Fetch data from DB and update a google sheet'''

from typing import List, Dict, NamedTuple
import json
from datetime import datetime

import glog

from housing.configs.config import Config, BedroomCount
from housing.data.db_client import DbClient
from housing.data.address import Address
from housing.frontend.google_sheets_client import GoogleSheetsClient, SheetData


UNIT_LISTINGS_QUERY = '''
    with latest_listings as (
        select distinct on (unit_id)
            unit_id,
            price
        from housing_listings
        order by unit_id, created_at desc
    ), latest_listings_by_source as (
        select distinct on (unit_id, source)
            unit_id,
            source,
            url
        from housing_listings
        order by unit_id, source, created_at desc
    )
    select
        units.address_str,
        units.bedrooms,
        min(listings.created_at) as first_found,
        max(listings.created_at) as last_found,
        array_agg(distinct listings.source) as sources,
        latest_listings.price as current_price,
        array_agg(latest_listings_by_source.url) as listing_urls
    from housing_units units
        join housing_listings listings on units.id = listings.unit_id
        join latest_listings on units.id = latest_listings.unit_id
        join latest_listings_by_source on units.id = latest_listings_by_source.unit_id
    group by units.address_str, units.bedrooms, latest_listings.price
'''
LISTING_TS_FORMAT = '%m/%d/%y'
SORT_HEADER = 'sort_value'
UPDATED_AT_TS_FORMAT = '%a, %m/%d/%y at %I:%M%p'


class UnitListing(NamedTuple):
    '''Data contained in a sheet row: a unit with the latest listing info.
    
    The field names here should be the sanitized version of sheet headers to update!
    Field values should be exactly as they should appear in the sheet - i.e. should convert to readable strings.
    '''
    PRIMARY_KEYS = ['unit', 'address', 'zipcode']
    LOCATION_TEXT = 'maps link'

    unit: int
    address: str  # Just the short_address portion
    zipcode: str
    location: str  # separate location link, necessary b/c can't use a link in a primary key column.
    current_price: int
    bedrooms: str
    sources: List[str]
    first_found: datetime
    last_found: datetime

    @classmethod
    def from_dict(cls, input: Dict) -> 'UnitListing':
        data = {key: value for key, value in input.items()}

        def _linked_cell(url: str, text: str) -> str:
            '''Helper for making hyperlinked cells - note the sheets api makes cells with multiple links very hard.'''
            return f'=HYPERLINK("{url}", "{text}")'
        
        # Parse & split address.
        address = Address.from_string(data['address_str'])
        data['unit'] = address.unit_num
        data['address'] = address.short_address
        data['zipcode'] = address.zipcode
        data['location'] = _linked_cell(url=address.to_google_maps_url(), text=cls.LOCATION_TEXT)
        del data['address_str']

        # Convert other fields to readable strings.
        data['bedrooms'] = str(data['bedrooms']) if data['bedrooms'] > 0 else 'studio'
        data['first_found'] = data['first_found'].strftime(LISTING_TS_FORMAT)
        data['last_found'] = data['last_found'].strftime(LISTING_TS_FORMAT)

        # Handle linked sources column.
        urls_by_source = dict(zip(data['sources'], data['listing_urls']))
        sorted_sources = sorted(data['sources'])
        source_cell_data = ','.join(sorted_sources)
        # Right now we just hyperlink to the first source - google sheets API makes inserting multiple links very hard.
        data['sources'] = _linked_cell(url=urls_by_source[sorted_sources[0]], text=source_cell_data)
        del data['listing_urls']

        return UnitListing(**data)

    def to_dict(self) -> Dict:
        '''Custom dict conversion method in case any fields need special handling.'''
        result = self._asdict()
        return result


def _sheet_metadata(config: Config) -> SheetData:
    '''Generate sheet metadata above the header row.'''
    scraping_params = config.scraping_params

    def _bedrooms_str() -> str:
        result = None
        min_bedrooms = scraping_params.min_bedrooms
        max_bedrooms = scraping_params.max_bedrooms
        if min_bedrooms == min_bedrooms:
            result = f'{min_bedrooms} BR'
        else:
            result = f'{min_bedrooms}-{max_bedrooms} BR'
        return result

    def _price_str() -> str:
        result = None
        min_price = scraping_params.min_price
        max_price = scraping_params.max_price
        if min_price == max_price:
            result = f'${min_price}'
        else:
            result = f'${min_price}-{max_price}'
        return result

    def _zipcodes_str() -> str:
        return ", ".join(scraping_params.zipcodes)

    def _scrapers_str() -> str:
        return ", ".join(scraping_params.scrapers)

    # Add generation metadata.
    generation_metadata = f'Rows are added programtically by apt-bot. Last updated: {datetime.now().strftime(UPDATED_AT_TS_FORMAT)}'

    # Add config overview.
    config_info = f'Scraping {_scrapers_str()} for: {_bedrooms_str()}, {_price_str()} in zipcodes: {_zipcodes_str()}'

    return [
        [generation_metadata],
        [config_info],
    ]



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
    glog.info(f'..updated sheet with new db data.')

    # Fill in sheet metadata.
    metadata = _sheet_metadata(config)
    metadata_top_row_num = sheets_client.header_row_num - len(metadata)
    assert metadata_top_row_num >= 0, f'Not enough room for {len(metadata)} rows with current header position: row {sheets_client.header_row_num}'
    metadata_tl = GoogleSheetsClient.row_col_num_to_A1(row_num=metadata_top_row_num, col_num=0)
    sheets_client.update(_range=metadata_tl, _values=metadata)
    
    