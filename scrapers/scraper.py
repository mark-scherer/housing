'''Universal scraping interface.'''

from functools import lru_cache
from typing import NamedTuple, List, Dict, Tuple, Optional
import argparse
from urllib.parse import urlparse
from datetime import datetime
import random

import requests
from bs4 import BeautifulSoup
import glog

import uszipcode

from housing.configs import config
from housing.data.address import Address
from housing.data.schema import Listing, IpAddress, Request
from housing.data.db_client import DbClient

parser = argparse.ArgumentParser()
parser.add_argument('--max_search_results', default=10, required=True, type=int,
    help='Result count at which to cutoff search with whatever has already been found.')
parser.add_argument('--max_scraped_search_results', default=None, type=int,
    help='Hard limit to the number of search results fully scraped.')
parser.add_argument('--ip_description', default=None, 
    help='description of IP address, needed if IP hasn\'t been logged before.')
parser.add_argument('--env', default=None, required=True, help='Env for logging requests')
FLAGS = parser.parse_args()


class KnownParsingError(Exception):
    '''Custom exception for known parsing errors that should be minimally logged.'''
    pass

class SearchResult(NamedTuple):
    '''Incomplete listing output from initial search that must be augmented with a specific search to convert to a Listing.'''
    id: str         # Site-specific unique ID for search result
    url: str        # Url to allow full scraping of the search result.
    address: Address


class Scraper:
    '''Universal scraping interface.'''

    SEARCH_REQUEST_PAGE_NUM_KEY = 'search_page_num'
    SEARCH_REQUEST_NUM_RESULTS_KEY = 'search_num_results'

    MAX_SEARCH_RESULTS: int = FLAGS.max_search_results
    MAX_SCRAPED_SEARCH_RESULTS: Optional[int] = FLAGS.max_scraped_search_results
    
    zipcode_client = uszipcode.SearchEngine()
    db_client = DbClient()
    _db_session = None
    my_ip = None

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
        if cls.MAX_SCRAPED_SEARCH_RESULTS:
            random.shuffle(search_results)
            search_results = search_results[0:cls.MAX_SCRAPED_SEARCH_RESULTS]
        glog.info(f'{cls.__name__} scraper gathered {len(search_results)} search results, now scraping listings from each..')

        listings = []
        for i, result in enumerate(search_results):
            try:
                result_listings = cls.scrape_listings(search_result=result, scraping_params=params)
                listings += result_listings
                glog.info(f'..scraped result {i} / {len(search_results)}, found {len(result_listings)} new listings - now {len(listings)} total')
            except KnownParsingError as e:
                glog.warning(f'Known error scraping listing for search result: {e}')
        
        glog.info(f'..{cls.__name__} scraper finished scraping all {len(search_results)} search results, found {len(listings)} listings.')
        return listings


    @classmethod
    @lru_cache(maxsize=100)
    def get_zipcode_info(cls, zipcode: str) -> uszipcode.model.SimpleZipcode:
        '''Get city, state info from a zipcode.'''
        return cls.zipcode_client.by_zipcode(zipcode)

    @classmethod
    def get_url(cls, url: str, method: str = 'GET', headers: Dict = None) -> Tuple[BeautifulSoup, Request]:
        '''Download data from url.
        
        Return:
        - soup
        - logged Request object (to enable updating response_info downstream)
        '''
        
        if headers is None:
            headers = {}

        # Log request
        cls._db_session = cls.db_client.session()  # Start a new session for this request.
        if cls.my_ip is None:
            cls.my_ip = IpAddress.my_ip()
        ip_address_str = cls.my_ip
        
        ip_address = cls._db_session.query(IpAddress).filter(IpAddress.ip == ip_address_str).first()
        if not ip_address:
            if not FLAGS.ip_description:
                raise ValueError(f'must provide --ip_description for unknown IP: {ip_address_str}')
            ip_address = IpAddress(
                ip=ip_address_str,
                description=FLAGS.ip_description
            )
            cls._db_session.add(ip_address)
            cls._db_session.flush()  # Need to flush to have id assigned
        
        parsed_url = urlparse(url)
        env = FLAGS.env.lower()
        request_info = {
            'headers': headers
        }
        logged_request = Request(
            ip=ip_address.id,
            domain=parsed_url.hostname,
            method=method,
            endpoint=parsed_url.path,
            environment=env,
            request_info=request_info,
        )
        logged_request.ip_id = ip_address.id
        cls._db_session.add(logged_request)
        cls._db_session.commit()
        
        response = requests.request(method, url, headers=headers)

        logged_request.response_info = {}
        logged_request.finished_at = datetime.utcnow()
        logged_request.status_code = response.status_code
        cls._db_session.commit()

        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup, logged_request

    @classmethod
    def is_valid_listing(cls, listing: Listing, params: config.ScrapingParams) -> bool:
        '''Check if listing meets all scraping params.'''
        return  params.min_price <= listing.price <= params.max_price and \
            params.min_bedrooms <= listing.unit.bedrooms <= params.max_bedrooms and \
            listing.unit.address.zipcode in params.zipcodes