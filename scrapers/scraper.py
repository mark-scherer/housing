'''Universal scraping interface.'''

from functools import lru_cache
from typing import NamedTuple, FrozenSet, List, Dict, Optional, Union

import requests
from bs4 import BeautifulSoup
import glog

import uszipcode
import usaddress


# Custom type to handle Studios.
# Studios represented as 0 Bedrooms
BedroomCount = int


class Address(NamedTuple):
    short_address: str
    city: str
    state: str
    zipcode: str
    unit: Optional[str] = None


    def id(self) -> str:
        '''Unique ID for Addrress.'''
        id_elements = [
            self.unit,
            self.short_address,
            self.zipcode,
        ]
        id = '-'.join([element for element in id_elements if element is not None])
        id.replace(' ', '-').lower()
        return id


    @staticmethod
    def from_full_address(full_address: str) -> 'Address':
        '''Parse Address from full address string.'''

        def _validate_parsed_address_info(address_info: Dict, full_address: str) -> None:
            '''Validate parsed usaddress included all needed info.'''
            REQUIRED_FIELDS = ['AddressNumber', 'StreetName', 'StreetNamePostType', 'PlaceName', 'StateName', 'ZipCode']
            for field in REQUIRED_FIELDS:
                assert field in address_info, f'address string missing required element ({field}): {full_address} {address_info}'


        address_info, _ = usaddress.tag(full_address)
        _validate_parsed_address_info(address_info, full_address)
        
        short_address_elements = [
            address_info.get('AddressNumber'),
            address_info.get('StreetNamePreDirectional'),
            address_info.get('StreetName'),
            address_info.get('StreetNamePostType')
        ]
        short_address_elements = [element for element in short_address_elements if element is not None]
        short_address = ' '.join(short_address_elements)

        unit = address_info.get('OccupancyIdentifier')

        return Address(
            short_address=short_address,
            city=address_info['PlaceName'],
            state=address_info['StateName'],
            zipcode=address_info['ZipCode'],
            unit=unit
        )


class SearchResult(NamedTuple):
    '''Incomplete listing output from initial search that must be augmented with a specific search to convert to a Listing.'''
    id: str         # Site-specific unique ID for search result
    url: str        # Url to allow full scraping of the search result.
    address: Address


class Listing(NamedTuple):
    '''A complete listing.'''

    # Metadata
    id: str
    name: str  # human readable ID

    # Unit details
    bedrooms: int

    # Location details
    zipcode: str

    # Listing details
    price: int
    source: str


# This should probably stay here
class ScrapingParams(NamedTuple):
    '''Params for defining a single scrape.
    
    Notes:
    - min values are inclusive, max values are exclusive
    '''

    # Unit params
    min_bedrooms: int
    max_bedrooms: int

    # Location params
    zipcodes: FrozenSet[str]

    # Listing parms
    min_price: int
    max_price: int


class Scraper:
    '''Universal scraping interface.'''

    zipcode_client = uszipcode.SearchEngine()

    @classmethod
    def scrape_search_results(cls, ScrapingParams) -> List[SearchResult]:
        '''Scrape partial listings from a search page.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    def scrape_listing(cls, SearchResult) -> Listing:
        '''Fully scrape a partial listing.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    @lru_cache(maxsize=100)
    def get_zipcode_info(cls, zipcode: str) -> uszipcode.model.SimpleZipcode:
        '''Get city, state info from a zipcode.'''
        return cls.zipcode_client.by_zipcode(zipcode)

    @classmethod
    def get_url(cls, url: str, method: str = 'GET', headers: Dict = None) -> BeautifulSoup:
        '''Download data from url.'''
        
        if headers is None:
            headers = {}
        
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup