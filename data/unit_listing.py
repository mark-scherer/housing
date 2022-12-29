'''Pairing of a unit with a single scraped listing.'''

from typing import List, Optional, Dict, TypeVar, Any
from dataclasses import dataclass, fields, asdict, field
from datetime import datetime
import json

import glog

from housing.configs.config import ScrapingParams
from housing.data.address import Address
from housing.data.db_client import DbClient

LISTING_TS_FORMAT = '%m/%d/%y'

UNIT_LISTINGS_QUERY = '''
    with latest_listings as (
        select distinct on (unit_id)
            id,
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
        units.bathrooms,
        (units.other_info->>'sqft')::integer as sqft,
        units.other_info->>'pets_allowed' as pets_allowed,
        units.other_info->>'parking_available' as parking_available,
        min(listings.created_at) as first_found,
        max(listings.created_at) as last_found,
        array_agg(distinct listings.source) as sources,
        latest_listings.price as current_price,
        array_agg(latest_listings_by_source.url) as listing_urls,
        units.id as unit_id,
        latest_listings.id as latest_listing_id
    from housing_units units
        join housing_listings listings on units.id = listings.unit_id
        join latest_listings on units.id = latest_listings.unit_id
        join latest_listings_by_source on units.id = latest_listings_by_source.unit_id
    where
        units.zipcode = any(:zipcodes) and
        units.bedrooms >= :min_bedrooms and units.bedrooms <= :max_bedrooms and
        latest_listings.price >= :min_price and latest_listings.price <= :max_price
    group by
        units.address_str,
        units.bedrooms,
        units.bathrooms,
        (units.other_info->>'sqft')::integer,
        units.other_info->>'pets_allowed',
        units.other_info->>'parking_available',
        latest_listings.price,
        units.id,
        latest_listings.id
'''

@dataclass
class UnitListing:
    '''Data contained in a sheet row: a unit with the latest listing info.
    
    The field names here should be the sanitized version of sheet headers to update!
    Field values should be exactly as they should appear in the sheet - i.e. should convert to readable strings.
    '''
    TRUE_STRINGS = ['true', 't']
    FALSE_STRINGS = ['false', 'f']

    PRIMARY_KEYS = ['unit', 'address', 'zipcode']
    LOCATION_TEXT = 'maps link'
    USER_UNIT_SCORE_HEADER_SUFFIX = '_unit_score'
    USER_LOCATION_SCORE_HEADER_SUFFIX = '_location_score'

    # Data model listed here matches google sheet format - includes all non-calculated columns.
    # Order only changed when necessary due to default values.
    
    # Required fields
    address: str  # Just the short_address portion
    zipcode: str
    bedrooms: int
    bathrooms: float
    current_price: int

    # Other location details.
    unit: Optional[str] = None
    # Separate location link, necessary b/c can't use a link in the primary key address columns.
    location: Optional[str] = None
    
    # Other unit basics.
    sqft: Optional[int] = None
    pets_allowed: Optional[bool] = None
    parking_available: Optional[bool] = None
    
    # Other listings basics.
    sources: List[str] = field(default_factory=list)

    # Sorting & scoring values.
    predicted_score: Optional[float] = None
    user_unit_scores: Dict[str, float] = field(default_factory=dict)
    user_location_scores: Dict[str, float] = field(default_factory=dict)
    location_veto: Optional[bool] = None
    not_available_veto: Optional[bool] = None

    # Misc backend metadata.
    first_found: Optional[datetime] = None
    last_found: Optional[datetime] = None
    unit_id: Optional[int] = None
    latest_listing_id: Optional[int] = None

    @classmethod
    def get_all_unit_listings(cls, scraping_params: ScrapingParams, db_client: Optional[DbClient] = None) -> List['UnitListing']:
        '''Query all UnitListings from the DB for the specified scraping parms.'''
        if db_client is None:
            db_client = DbClient()

        query_params = scraping_params.to_dict()
        query_params['zipcodes'] = list(scraping_params.zipcodes)
        query_data = db_client.query(UNIT_LISTINGS_QUERY, query_params)
        results = []
        for db_row in query_data:
            parsed_db_row = {}
            try:
                parsed_db_row = UnitListing.from_db_row(db_row)
                results.append(parsed_db_row)
            except Exception as e:
                raise RuntimeError(f'Error parsing db row: {db_row} (parsed into {json.dumps(parsed_db_row)})') from e

        return results

    @classmethod
    def from_db_row(cls, row: Dict) -> 'UnitListing':
        '''Converts DB row queired via UNIT_LISTINGS_QUERY into UnitListing object.'''
        data = {key: value for key, value in row.items()}

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
        # Convert source urls to google sheet hyperlinks
        data['sources'] = [_linked_cell(url=urls_by_source[source], text=source) for source in sorted_sources]
        del data['listing_urls']

        return UnitListing.from_dict(data)

    @classmethod
    def _strToBool(cls, input: str) -> bool:
        '''Helper for parsing stringifed postgres bools back into actual bools'''
        result = None
        if input:
            if input.lower() in cls.TRUE_STRINGS:
                result = True
            elif input.lower() in cls.FALSE_STRINGS:
                result = False
            else:
                raise ValueError(f'Could not parse bool from string: {input}')

        return result

    @classmethod
    def from_dict(cls, data) -> 'UnitListing':
        '''Converts generic dict into UnitListing object.'''

        def _cast_value(data: Dict, key: str, type: TypeVar) -> Any:
            if key in data and data[key] is not None:
                if type == bool:
                    data[key] = cls._strToBool(data[key])
                else:
                    data[key] = type(data[key])
            return data
        
        result = None
        try:
            # Type convert certain fields.
            data = _cast_value(data, 'bedrooms', int)
            data = _cast_value(data, 'bathrooms', float)
            data = _cast_value(data, 'sqft', int)
            data = _cast_value(data, 'pets_allowed', bool)
            data = _cast_value(data, 'parking_available', bool)
            data = _cast_value(data, 'predicted_score', float)
            data = _cast_value(data, 'location_veto', bool)
            data = _cast_value(data, 'not_available_veto', bool)
            data = _cast_value(data, 'unit_id', int)
            data = _cast_value(data, 'latest_listing_id', int)

            # More involved field conversion
            data['current_price'] = str(data['current_price']).replace('$', '').replace(',', '')
            data = _cast_value(data, 'current_price', int)
            
            if 'sources' in data and data['sources'] is not None:
                # Only split into list if not already.
                if isinstance(data['sources'], str):
                    data['sources'] = data['sources'].split(',')
            
            for user_unit_score_key in [k for k in data.keys() if k.endswith(cls.USER_UNIT_SCORE_HEADER_SUFFIX)]:
                user = user_unit_score_key.replace(cls.USER_UNIT_SCORE_HEADER_SUFFIX, '').strip()
                value =  data[user_unit_score_key]
                score = None
                if value is not None and value != '':
                    try:
                        score = float(value)
                    except Exception as e:
                        raise ValueError(f'Error parsing unit score for user {user}') from e
                
                if 'user_unit_scores' not in data:
                    data['user_unit_scores'] = {}
                data['user_unit_scores'][user] = score
            
            for user_location_score_key in [k for k in data.keys() if k.endswith(cls.USER_LOCATION_SCORE_HEADER_SUFFIX)]:
                user = user_location_score_key.replace(cls.USER_LOCATION_SCORE_HEADER_SUFFIX, '').strip()
                value = data[user_location_score_key].strip()
                score = None
                if value is not None and value != '':
                    try:
                        score = float(value)
                    except Exception as e:
                        raise ValueError(f'Error parsing location score for user {user}: {value}') from e

                if 'user_location_scores' not in data:
                    data['user_location_scores'] = {}    
                data['user_location_scores'][user] = score

            # NOTE: not parsing first_found, last_found into datetimes, will remain strings.
            # This isn't that hard but just isn't needed yet.

            # Trim input dict to just UnitListing fields.
            trimmed_data = {}
            ul_fields = UnitListing.fields()
            for key, value in data.items():
                if key in ul_fields:
                    trimmed_data[key] = value

            # Parse into UnitListing object.
            result = UnitListing(**trimmed_data)
        except Exception as e:
            raise ValueError(f'Error parsing Unitlisting from dict: {json.dumps(data)}') from e
        
        return result

    @classmethod
    def fields(cls) -> List[str]:
        '''Helper to fetch all properties of the UnitListing class.'''
        return [f.name for f in fields(UnitListing)]

    def to_dict(self) -> Dict:
        '''Custom dict conversion method in case any fields need special handling.'''
        result = asdict(self)
        return result

    def to_sheet_update(self) -> Dict:
        '''Dump UnitListing as dict with type conversions made to support google sheet update.'''
        update_data = self.to_dict()

        # Handle multiple sources - google sheets makes it basically impossible to have
        # multiple hyperlinks per cell so just take the first one.
        if update_data['sources']:
            update_data['sources'] = update_data['sources'][0]
        
        return update_data