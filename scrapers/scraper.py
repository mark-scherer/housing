'''Universal scraping interface.'''

from functools import lru_cache
from typing import NamedTuple, List, Dict, Optional, Union

import requests
from bs4 import BeautifulSoup
import glog

import uszipcode
import usaddress

from housing.configs import config


class Address(NamedTuple):
    short_address: str
    city: str
    state: str
    zipcode: str
    unit_num: Optional[str] = None


    def id(self) -> str:
        '''Unique ID for Addrress.'''
        id_elements = [
            self.unit_num,
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

        unit_num = address_info.get('OccupancyIdentifier')

        return Address(
            short_address=short_address,
            city=address_info['PlaceName'],
            state=address_info['StateName'],
            zipcode=address_info['ZipCode'],
            unit_num=unit_num
        )


class SearchResult(NamedTuple):
    '''Incomplete listing output from initial search that must be augmented with a specific search to convert to a Listing.'''
    id: str         # Site-specific unique ID for search result
    url: str        # Url to allow full scraping of the search result.
    address: Address


class Unit(NamedTuple):
    '''Unit includes all data intrinstic to physical unit itself and can be shared by multiple listings.'''
    address: Address
    bedrooms: config.BedroomCount

    def to_dict(self) -> Dict:
        '''_asdict() does not serialize properly and can't be overriden for NamedTuple, so must make our own.'''
        result = self._asdict()
        result['address'] = self.address._asdict()  # Must call this on nested NamedTuple manually
        return result


class Listing(NamedTuple):
    '''A complete listing.'''

    # Unit details
    unit: Unit

    # Listing details
    price: int
    source: str

    def to_dict(self) -> Dict:
        '''_asdict() does not serialize properly and can't be overriden for NamedTuple, so must make our own.'''
        result = self._asdict()
        result['unit'] = self.unit.to_dict()  # Must call this on nested NamedTuple manually
        return result


class Scraper:
    '''Universal scraping interface.'''

    zipcode_client = uszipcode.SearchEngine()

    @classmethod
    def scrape_search_results(cls, params: config.ScrapingParams) -> List[SearchResult]:
        '''Scrape search results (format is source-specific) for a given ScrapingParams.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    def scrape_listings(cls, search_result: SearchResult, scraping_params: config.ScrapingParams) -> List[Listing]:
        '''Fully scrape a single search result. Can return multiple listings.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    def search_and_scrape(cls, params: config.ScrapingParams) -> List[Listing]:
        '''Search given ScrapingParams and then fully scrape listings from each result.'''
        search_results = cls.scrape_search_results(params=params)
        glog.info(f'{cls.__name__} scraper gathered {len(search_results)} search results, now scraping listings from each..')

        listings = []
        for i, result in enumerate(search_results):
            result_listings = cls.scrape_listings(search_result=result, scraping_params=params)
            listings += result_listings
            glog.info(f'..scraped result {i} / {len(search_results)}, found {len(result_listings)} new listings - now {len(listings)} total')
        
        glog.info(f'..{cls.__name__} scraper finished scraping all {len(search_results)} search results, found {len(listings)} listings.')
        return listings


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

    @classmethod
    def is_valid_listing(cls, listing: Listing, params: config.ScrapingParams) -> bool:
        '''Check if listing meets all scraping params.'''
        return  params.min_price <= listing.price <= params.max_price and \
            params.min_bedrooms <= listing.unit.bedrooms <= params.max_bedrooms and \
            listing.unit.address.zipcode in params.zipcodes