'''Apartments.com scraper

TODO:
- implement pagination
'''

from os import path
from functools import lru_cache
from typing import Any, List, Dict
import json
import re

from bs4 import BeautifulSoup, Tag
import glog

from housing.scrapers import scraper, schema_dot_org

BASE_URL = 'https://www.apartments.com/'


class ApartmentsDotComSearchResult(scraper.SearchResult):
    '''Output from Apartments.com search results page.
    
    Apartments.com aggregates units by building and returns the range of options available.
    '''

    # Range of prices found at building.
    min_price: int
    max_price: int

    # Range of bedrooms found at building
    min_bedrooms: scraper.BedroomCount
    max_bedrooms: scraper.BedroomCount


    def __new__(cls, id, url, address, min_price, max_price, min_bedrooms, max_bedrooms):
        '''Cannot use NamedTuple in chained inheritance unless you override __new__() in the children.'''
        self = super(ApartmentsDotComSearchResult, cls).__new__(cls, id, url, address)
        self.min_price = min_price
        self.max_price = max_price
        self.min_bedrooms = min_bedrooms
        self.max_bedrooms = max_bedrooms
        return self


    def _asdict(self) -> Dict:
        '''Due to NamedTuple inheritance complexities also have to override this.'''
        result = super()._asdict()
        result['min_price'] = self.min_price
        result['max_price'] = self.max_price
        result['min_bedrooms'] = self.min_bedrooms
        result['max_bedrooms'] = self.max_bedrooms
        return result


class ApartmentsDotCom(scraper.Scraper):
    '''Apartments.com scraper.'''

    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    DATA_BLOCK_TYPE = 'application/ld+json'
    SEARCH_RESULT_ELEMENT_TYPE = 'article'
    SEARCH_RESULT_ID_ATTRIBUTE = 'data-listingid'
    SEARCH_RESULT_URL_ATTRIBUTE = 'data-url'
    SEARCH_RESULT_ADDRESS_CLASSES = ['property-address']
    SEARCH_RESULT_ADDRESS_CLASSES_FALLBACK = ['property-title', 'property-address']
    SEARCH_RESULT_PRICING_CLASSES = ['property-pricing', 'property-rents', 'price-range']
    SEARCH_RESULT_BEDROOMS_CLASSES = ['property-beds', 'bed-range']


    @classmethod
    @lru_cache(maxsize=100)
    def _get_search_url(cls, params: scraper.ScrapingParams, zipcode: str) -> str:
        '''Generate search url from specified scraping params.
        
        Must specify zipcode b/c ScrapingParams has multiple.
        '''
        
        # Get location clause.
        zipcode_info = cls.get_zipcode_info(zipcode)
        location_str = f'{zipcode_info.major_city}-{zipcode_info.state}-{zipcode}'
        location_str = location_str.replace(' ', '-').lower()

        # Get bedrooms clause.
        bedrooms_clause = f'{params.min_bedrooms}-bedrooms' \
            if params.min_bedrooms == params.max_bedrooms \
            else f'{params.min_bedrooms}-to-{params.max_bedrooms}-bedrooms'

        # Get price clause.
        price_clause = f'{params.min_price}-to-{params.max_price}'

        return path.join(BASE_URL, location_str, f'{bedrooms_clause}-{price_clause}') + '/'


    @classmethod
    def get_url(cls, url: str, method: str = 'GET') -> BeautifulSoup:
        '''Customize request to get past server scraping filters.'''
        headers = {'user-agent': cls.USER_AGENT}
        return super().get_url(url, method, headers=headers)


    @classmethod
    def _parse_apartment_complex_data_block(cls, apartment_complexes: List[Dict]) -> List[scraper.SearchResult]:
        '''Parse data block of schema.org ApartmentComplexes loaded as dicts.'''
        results = []
        for complex_data in apartment_complexes:
            partial_search_result = schema_dot_org.parse_apartment_complex(complex_data)
            source_id = path.basename(path.normpath(partial_search_result.url))  # Override default address-based ID w/ source-specific ID
            search_result = scraper.SearchResult(
                id=source_id,
                url=partial_search_result.id,
                address=partial_search_result.address
            )
            results.append(search_result)
        return results


    @classmethod
    def _parse_address_from_elements(cls, address_elements: List[Tag]) -> scraper.Address:
        '''Helper for parsing address from the text combination from multiple elements.'''
        assert len(address_elements) > 0, 'could not find address element'
        full_address_str = ' '.join([element.text for element in address_elements])
        address = scraper.Address.from_full_address(full_address_str)
        return address


    @classmethod
    def _parse_search_results_html(cls, search_result_elements: List[Tag]) -> List[ApartmentsDotComSearchResult]:
        '''Parse search result html.'''
        search_results = []
        for i, result_element in enumerate(search_result_elements):
            id = result_element[cls.SEARCH_RESULT_ID_ATTRIBUTE]
            url = result_element[cls.SEARCH_RESULT_URL_ATTRIBUTE]
            
            try:
                
                # Parse address.
                # First try string combination of text from all primary address classes.
                address = None
                try:
                    address_elements = result_element.find_all(class_=cls.SEARCH_RESULT_ADDRESS_CLASSES)
                    address = cls._parse_address_from_elements(address_elements)
                except AssertionError:
                    # Try fallback classes.
                    address_elements = result_element.find_all(class_=cls.SEARCH_RESULT_ADDRESS_CLASSES_FALLBACK)
                    address = cls._parse_address_from_elements(address_elements)

                # Parse price data.
                min_price = None
                max_price = None
                pricing_element = result_element.find(class_=cls.SEARCH_RESULT_PRICING_CLASSES)
                assert pricing_element is not None, 'could not find pricing element'

                pricing_str = pricing_element.text.replace('$', '').replace(',', '').replace(' ', '')
                pricing_str = pricing_str.replace('/mo', '')
                if '-' in pricing_str:
                    min_price_str, max_price_str = pricing_str.split('-')
                    min_price = int(min_price_str)
                    max_price = int(max_price_str)
                else:
                    min_price = max_price = int(pricing_str)

                # Parse bedrooms.
                min_bedrooms = None
                max_bedroomss = None
                bedrooms_element = result_element.find(class_=cls.SEARCH_RESULT_BEDROOMS_CLASSES)
                assert bedrooms_element is not None, 'could not find bedrooms element'

                bedrooms_str = re.sub('bed(s)?', '', bedrooms_element.text.lower()).replace(' ', '')
                bedrooms_str = bedrooms_str.split(',')[0]
                bedrooms_str = bedrooms_str.replace('studio', '0')
                if '-' in bedrooms_str:
                    min_bedrooms_str, max_bedrooms_str = bedrooms_str.split('-')
                    min_bedrooms = int(min_bedrooms_str)
                    max_bedrooms = int(max_bedrooms_str)
                else:
                    min_bedrooms = max_bedrooms = int(bedrooms_str)

                result = ApartmentsDotComSearchResult(
                    id=id,
                    url=url,
                    address=address,
                    min_price=min_price,
                    max_price=max_price,
                    min_bedrooms=min_bedrooms,
                    max_bedrooms=max_bedrooms
                )
                search_results.append(result)
            
            except AssertionError as e:
                glog.warning(f'Error parsing search result {id} ({i}), skipping: {e}')
                continue
        
        return search_results



    @classmethod
    def scrape_search_results(cls, params: scraper.ScrapingParams) -> List[ApartmentsDotComSearchResult]:
        '''Scrape search results from the search page.'''
        assert len(params.zipcodes) == 1, 'Do not yet support multiple zipcodes.'
        zipcode, = params.zipcodes
        
        search_url = cls._get_search_url(params, zipcode)
        soup = cls.get_url(search_url)

        # Parse LD-JSON data blocks.
        # Note: this provides no info over scraping the html so skipping.
        # data_blocks = [json.loads(db.string) for db in soup.find_all('script', type=cls.DATA_BLOCK_TYPE)]
        # data_block_search_results = cls._parse_apartment_complex_data_block(data_blocks[0]['about'])
        # _ = data_blocks[1]  # Info about virtual tours, not useful.

        # Parse info available in html.
        is_search_result_element = lambda tag: tag.name == cls.SEARCH_RESULT_ELEMENT_TYPE and tag.has_attr(cls.SEARCH_RESULT_ID_ATTRIBUTE)
        search_result_elements = soup.find_all(is_search_result_element)
        search_results = cls._parse_search_results_html(search_result_elements)

        # Filter out non-matches included in results.
        filtered_searched_results = [
            result for result in search_results
            if result.min_price <= params.max_price and result.max_price >= params.min_price and \
                result.min_bedrooms <= params.max_bedrooms and result.max_bedrooms >= params.min_bedrooms and \
                result.address.zipcode == zipcode
        ]

        search_results_obj = [pl._asdict() for pl in filtered_searched_results]
        glog.info(f'parsed {len(filtered_searched_results)} search results: {json.dumps(search_results_obj)}')


