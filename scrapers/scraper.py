'''Universal scraping interface.'''

from functools import lru_cache
from typing import NamedTuple, List, Dict

import requests
from bs4 import BeautifulSoup
import glog

import uszipcode

from housing.configs import config
from housing.data.address import Address
from housing.data.schema import Listing


class SearchResult(NamedTuple):
    '''Incomplete listing output from initial search that must be augmented with a specific search to convert to a Listing.'''
    id: str         # Site-specific unique ID for search result
    url: str        # Url to allow full scraping of the search result.
    address: Address


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