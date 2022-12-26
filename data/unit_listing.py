'''Pairing of a unit with a single scraped listing.'''

from typing import List, Optional, Dict
from dataclasses import dataclass, fields, asdict
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
    PRIMARY_KEYS = ['unit', 'address', 'zipcode']
    LOCATION_TEXT = 'maps link'

    # Scoring class vars
    TRUE_STRINGS = ['true', 't']
    FALSE_STRINGS = ['false', 'f']
    MIN_SCORED_PRICE = 1000
    MAX_SCORED_PRICE = 5000
    MAX_PRICE_SCORE = 50
    MIN_PRICE_SCORE = 0
    SCORE_PER_SQFT = 25 / 1000  # 25 pts per 1000 sqft
    SCORE_BY_BEDROOM = [0, 40, 20]
    SCORE_PER_BATHROOM = 20
    PETS_ALLOWED_SCORE = 20
    PARKING_AVAILABLE_SCORE = 20

    unit: int
    address: str  # Just the short_address portion
    zipcode: str
    location: str  # separate location link, necessary b/c can't use a link in a primary key column.
    current_price: int
    bedrooms: int
    bathrooms: int
    sqft: int
    pets_allowed: bool
    parking_available: bool
    sources: List[str]
    first_found: datetime
    last_found: datetime
    unit_id: int
    latest_listing_id: int
    predicted_score: Optional[float] = None

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
                parsed_db_row = UnitListing.from_dict(db_row)
                parsed_db_row.score()
                results.append(parsed_db_row)
            except Exception as e:
                raise RuntimeError(f'Error parsing db row: {db_row} (parsed into {json.dumps(parsed_db_row)})') from e

        return results

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

    def score(self) -> float:
        '''Fill in & return predicted score for UnitListing.
        
        Score is not bounded and doesn't realy represent anything specific, but is deterministic for a UnitListing.
        '''
        score = 0

        # Start score by weighing price.
        saturated_price = max(min(self.MAX_SCORED_PRICE, self.current_price), self.MIN_SCORED_PRICE)
        price_score_fraction = (saturated_price - self.MIN_PRICE_SCORE) / (self.MAX_SCORED_PRICE - self.MIN_SCORED_PRICE)
        price_score = (price_score_fraction * (self.MAX_PRICE_SCORE - self.MIN_PRICE_SCORE)) + self.MIN_PRICE_SCORE
        score += price_score

        # Factor in sqft.
        if self.sqft:
            sqft_score = self.sqft * self.SCORE_PER_SQFT
            score += sqft_score

        # Factor in bedrooms, bathrooms.
        bedroom_score = 0
        for i in range(int(self.bedrooms)):
            bedroom_score += self.SCORE_BY_BEDROOM[i]
        bathrooms_score = int(self.bathrooms) * self.SCORE_PER_BATHROOM
        score += bedroom_score + bathrooms_score

        # Factor in pets allowed, parking availability.
        pets = self._strToBool(self.pets_allowed)
        if pets is not None:
            pets_score = int(pets) * self.PETS_ALLOWED_SCORE
            score += pets_score
        parking = self._strToBool(self.parking_available)
        if parking is not None:
            parking_score = int(parking) * self.PARKING_AVAILABLE_SCORE
            score += parking_score

        self.predicted_score = score
        return score

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
        result = asdict(self)
        return result