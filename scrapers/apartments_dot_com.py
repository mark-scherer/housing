'''Apartments.com scraper

TODO:
- implement pagination
'''

from os import path
from functools import lru_cache
from typing import List, Dict, Optional
import json
import re
import traceback

from bs4 import BeautifulSoup, Tag
import glog

from housing.configs import config
from housing.data.address import Address
from housing.data.schema import Unit, Listing
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
    min_bedrooms: config.BedroomCount
    max_bedrooms: config.BedroomCount


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

    SOURCE = 'apartments.com'
    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'

    # Search result scraping params.
    DATA_BLOCK_TYPE = 'application/ld+json'
    SEARCH_RESULT_ELEMENT_TYPE = 'article'
    SEARCH_RESULT_ID_ATTRIBUTE = 'data-listingid'
    SEARCH_RESULT_URL_ATTRIBUTE = 'data-url'
    SEARCH_RESULT_ADDRESS_CLASSES = ['property-address']
    SEARCH_RESULT_ADDRESS_CLASSES_FALLBACK = ['property-title', 'property-address']
    SEARCH_RESULT_PRICING_CLASSES = ['property-pricing', 'property-rents', 'price-range']
    SEARCH_RESULT_BEDROOMS_CLASSES = ['property-beds', 'bed-range']
    PAGE_COUNT_CLASS = 'pageRange'
    DEFAULT_NUM_PAGES = 1

    # Listing scraping params: multi-listing search results.
    ALL_UNITS_TAB_ATTRIBUTE_NAME = 'data-tab-content-id'
    ALL_UNITS_TAB_ATTRIBUTE_VALUE = 'all'
    UNIT_TYPE_CLASS = 'hasUnitGrid'
    UNIT_TYPE_METADATA_CONTAINER_CLASS = 'priceGridModelWrapper'
    UNIT_TYPE_ID_ATTRIBUTE = 'data-rentalkey'
    UNIT_TYPE_METADATA_CLASS = 'detailsTextWrapper'
    UNIT_TYPE_LISTINGS_CLASS = 'unitContainer'

    # Listing scraping params: single-listing search result.
    LISTING_ADDRESS_CLASSES = ['propertyNameRow', 'propertyAddressRow']
    LISTING_NEIGHBORHOOD_CLASS = 'neighborhoodAddress'
    LISTING_DETAILS_CELL_CLASS = 'priceBedRangeInfoInnerContainer'
    LISTING_DETAILS_CELL_LABEL_PRICE = 'Monthly Rent'
    LISTING_DETAILS_CELL_LABEL_BEDROOMS = 'Bedrooms'


    # Generic listing scraping params.
    LISTING_UNIT_NUM_CLASS = 'unitColumn'
    LISTING_PRICE_CLASS = 'pricingColumn'



    @classmethod
    @lru_cache(maxsize=100)
    def _get_search_url(
        cls, 
        params: config.ScrapingParams, 
        zipcode: str,
        page: int
    ) -> str:
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
        price_clause = f'{params.min_price}-to-{params.max_price}' \
            if params.min_price > 0 \
            else f'under-{params.max_price}'

        # Get pagination clause
        pagination_clause = str(page) if page > 1 else ''

        url = path.join(BASE_URL, location_str, f'{bedrooms_clause}-{price_clause}', pagination_clause)
        if url[-1] != '/':
            url += '/'

        return url


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
    def _sanitize_string(cls, input: str) -> str:
        '''Remove any newlines, consecutive spaces, etc.'''
        result = input
        result = re.sub('\n|\r', ' ', result)  # Cleanup address newlines, tabs.
        result = re.sub(' {2,}', ' ', result).strip()  # Cleanup spaces.
        return result


    @classmethod
    def _parse_address_from_elements(cls, address_elements: List[Tag]) -> Address:
        '''Helper for parsing address from the text combination from multiple elements.'''
        assert len(address_elements) > 0, 'could not find address element'
        full_address_str = ' '.join([element.text for element in address_elements])
        address = Address.from_full_address(full_address_str)
        return address

    
    @classmethod
    def _parse_bedrooms(cls, bedrooms_str: str) -> config.BedroomCount:
        """Parse the number of bedrooms from a formatted string."""
        
        assert '-' not in bedrooms_str, 'Found "-", must split string before passing to _parse_bedrooms()'
        
        bedrooms_str = bedrooms_str.lower()
        bedrooms_str = re.sub('b(e)?d(s)?', '', bedrooms_str)
        bedrooms_str = bedrooms_str.replace(' ', '')
        bedrooms_str = bedrooms_str.split(',')[0]
        bedrooms_str = bedrooms_str.replace('studio', '0')
        
        return int(bedrooms_str)


    @classmethod
    def _parse_price(cls, price_str: str) -> int:
        """Parse price from a formmatted string."""

        assert '-' not in price_str, 'Found "-", must split string before passing to _parse_price()'

        price_str = price_str.replace('price', '')
        price_str = price_str.replace('$', '')
        price_str = price_str.replace(',', '')
        price_str = price_str.replace(' ', '')
        price_str = price_str.replace('/mo', '')
        return int(price_str)


    @classmethod
    def _parse_unit_num(cls, unit_num_str: str) -> str:
        """Parse unit_num from a formmated string."""

        unit_num_str = unit_num_str.lower()
        unit_num_str = unit_num_str.replace('unit', '')
        unit_num_str = unit_num_str.lstrip().rstrip()
        return unit_num_str


    @classmethod
    def _parse_search_result_element(cls, result_element: Tag) -> ApartmentsDotComSearchResult:
        '''Parse search result html.'''
        id = result_element[cls.SEARCH_RESULT_ID_ATTRIBUTE]
        url = result_element[cls.SEARCH_RESULT_URL_ATTRIBUTE]
        
        result = None
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
            pricing_str = pricing_element.text
            if '-' in pricing_str:
                min_price_str, max_price_str = pricing_str.split('-')
                min_price = cls._parse_price(min_price_str)
                max_price = cls._parse_price(max_price_str)
            else:
                min_price = max_price = cls._parse_price(pricing_str)

            # Parse bedrooms.
            min_bedrooms = None
            max_bedroomss = None
            bedrooms_element = result_element.find(class_=cls.SEARCH_RESULT_BEDROOMS_CLASSES)
            assert bedrooms_element is not None, 'could not find bedrooms element'
            bedrooms_str = bedrooms_element.text
            if '-' in bedrooms_str:
                min_bedrooms_str, max_bedrooms_str = bedrooms_str.split('-')
                min_bedrooms = cls._parse_bedrooms(min_bedrooms_str)
                max_bedrooms = cls._parse_bedrooms(max_bedrooms_str)
            else:
                min_bedrooms = max_bedrooms = cls._parse_bedrooms(bedrooms_str)

            result = ApartmentsDotComSearchResult(
                id=id,
                url=url,
                address=address,
                min_price=min_price,
                max_price=max_price,
                min_bedrooms=min_bedrooms,
                max_bedrooms=max_bedrooms
            )
        
        except AssertionError as e:
            exception_type = type(e)
            raise exception_type(f'Error parsing search result {id}: {e}') from e
        
        return result


    @classmethod
    def _parse_num_result_pages(cls, soup: BeautifulSoup) -> Optional[int]:
        '''Return the total number of pages if parseable.'''
        num_pages = None
        page_count_element = soup.find(class_=cls.PAGE_COUNT_CLASS)
        if page_count_element is not None:
            page_count_str = page_count_element.text
            if 'of' in page_count_str:
                num_pages_str = page_count_str.split('of')[1]
                num_pages = int(num_pages_str)
        
        return num_pages


    @classmethod
    def scrape_search_results(cls, params: config.ScrapingParams) -> List[ApartmentsDotComSearchResult]:
        '''Scrape search results from the search page.'''
        assert len(params.zipcodes) == 1, 'Do not yet support multiple zipcodes.'
        zipcode, = params.zipcodes

        current_page = 0  # Uses 1-based page numbers.
        num_pages = None  # Will be set in loop.
        continue_loop = True
        search_results = []
        while continue_loop:
            current_page += 1
            search_url = cls._get_search_url(params=params, zipcode=zipcode, page=current_page)
            soup = cls.get_url(search_url)

            # Parse LD-JSON data blocks.
            # Note: this provides no info over scraping the html so skipping.
            # data_blocks = [json.loads(db.string) for db in soup.find_all('script', type=cls.DATA_BLOCK_TYPE)]
            # data_block_search_results = cls._parse_apartment_complex_data_block(data_blocks[0]['about'])
            # _ = data_blocks[1]  # Info about virtual tours, not useful.

            # Parse info available in html.
            is_search_result_element = lambda tag: tag.name == cls.SEARCH_RESULT_ELEMENT_TYPE and tag.has_attr(cls.SEARCH_RESULT_ID_ATTRIBUTE)
            search_result_elements = soup.find_all(is_search_result_element)
            new_results = []
            for i, result_element in enumerate(search_result_elements):
                try:
                    parsed_result = cls._parse_search_result_element(result_element)
                    new_results.append(parsed_result)
                except Exception as e:
                    glog.warning(f'error parsing search result element (page: {current_page}, element: {i}), skipping: {traceback.format_exc()}')
                
            if num_pages is None:
                num_pages = cls._parse_num_result_pages(soup) or cls.DEFAULT_NUM_PAGES
            
            # Parse number of search result pages if haven't already.
            search_results += new_results
            glog.info(f'Parsed {len(new_results)} new results from page {current_page} / {num_pages}, now {len(search_results)} total..')
            
            # Determine if search pagination loop should continue.
            continue_loop = True
            if len(new_results) == 0:
                continue_loop = False
                glog.warning(f'found 0 new results, ending search.')
            elif len(search_results) > params.max_results:
                continue_loop = False
                glog.warning(f'found {len(search_results)} search results, more than max_results: {params.max_results}... ending search.')
            elif num_pages is not None and current_page >= num_pages:
                continue_loop = False
                glog.info(f'parsed all {current_page} / {num_pages} pages, ending search.')

        # Filter out non-matches included in results.
        filtered_searched_results = [
            result for result in search_results
            if result.min_price <= params.max_price and result.max_price >= params.min_price and \
                result.min_bedrooms <= params.max_bedrooms and result.max_bedrooms >= params.min_bedrooms and \
                result.address.zipcode == zipcode
        ]

        return filtered_searched_results


    @classmethod
    def _parse_unit_type_html(cls, unit_type_html: Tag, building_address: Address) -> List[Listing]:
        """Parse listings grid for given unit type.
        
        'Unit Type' is single result box with fixed floor plan.
        Each search result can have multiple, and each can have multiple listings at different prices.

        Currently ignores available info that's not stored in Listing:
        - images
        - floorplan
        - sq footage
        - bathrooms
        - available date
        """

        listings = []
        unit_type_id = None
        try:
            unit_type_metadata_element = unit_type_html.find(class_=cls.UNIT_TYPE_METADATA_CONTAINER_CLASS)
            unit_type_id = unit_type_metadata_element[cls.UNIT_TYPE_ID_ATTRIBUTE]

            # Parse unit type metadata.
            unit_type_metadata_str = unit_type_metadata_element.find(class_=cls.UNIT_TYPE_METADATA_CLASS).text
            
            # Ignore bathrooms, sq footage data for now.
            bedrooms_str, bathrooms_str, *sq_footage_strs = unit_type_metadata_str.split(',')
            bedrooms = cls._parse_bedrooms(bedrooms_str)

            # Parse listings.
            listing_elements = unit_type_html.find_all(class_=cls.UNIT_TYPE_LISTINGS_CLASS)
            for element in listing_elements:
                unit_num_str = element.find(class_=cls.LISTING_UNIT_NUM_CLASS).text
                unit_num = cls._parse_unit_num(unit_num_str)
                price_str = element.find(class_=cls.LISTING_PRICE_CLASS).text
                price = cls._parse_price(price_str)

                address = Address(
                    short_address=building_address.short_address,
                    city=building_address.city,
                    state=building_address.state,
                    zipcode=building_address.zipcode,
                    unit_num=unit_num
                )
                unit = Unit(
                    address=address,
                    bedrooms=bedrooms,
                )
                listing = Listing(
                    unit=unit,
                    price=price,
                    source= cls.SOURCE
                )
                listings.append(listing)

        except Exception as e:
            exception_type = type(e)
            raise exception_type(f'Error parsing listings from unit type {unit_type_id}: {e}')

        return listings


    @classmethod
    def _parse_multi_listing_search_result(cls, page_soup: BeautifulSoup, building_address: scraper.Address) -> List[Listing]:
        '''Parse all listings from a multi-listing search result page.'''
        
        all_results_tab_element = page_soup.find(attrs={cls.ALL_UNITS_TAB_ATTRIBUTE_NAME: cls.ALL_UNITS_TAB_ATTRIBUTE_VALUE})
        unit_type_elements = all_results_tab_element.find_all(class_=cls.UNIT_TYPE_CLASS)

        listings = []
        for unit_type in unit_type_elements:
            unit_type_listings = cls._parse_unit_type_html(unit_type_html=unit_type, building_address=building_address)
            listings += unit_type_listings

        return listings


    @classmethod
    def _parse_single_listing_search_result(cls, page_soup: BeautifulSoup) -> Listing:
        '''Parse Listing from single-listing search result page.'''
        
        # Parse address.
        address_lines = [element.text for element in page_soup.find_all(class_=cls.LISTING_ADDRESS_CLASSES)]
        neighborhood_element = page_soup.find(class_=cls.LISTING_NEIGHBORHOOD_CLASS)
        neighborhood_str = neighborhood_element.text if neighborhood_element else ''
        address_str = ' '.join(address_lines).replace(neighborhood_str, '')  # Join address lines and remove neighboorhood string.
        address_str = cls._sanitize_string(address_str)
        address = Address.from_full_address(address_str)
        
        listing_detail_elements = page_soup.find_all(class_=cls.LISTING_DETAILS_CELL_CLASS)

        # Parse bedrooms.
        bedrooms_detail_cell_element = next(filter(lambda element: cls.LISTING_DETAILS_CELL_LABEL_BEDROOMS in element.text, listing_detail_elements))
        bedrooms_str = bedrooms_detail_cell_element.text.replace(cls.LISTING_DETAILS_CELL_LABEL_BEDROOMS, '')
        bedrooms_str = cls._sanitize_string(bedrooms_str)
        bedrooms = cls._parse_bedrooms(bedrooms_str)

        # Parse price.
        price_detail_cell_element = next(filter(lambda element: cls.LISTING_DETAILS_CELL_LABEL_PRICE in element.text, listing_detail_elements))
        price_str = price_detail_cell_element.text.replace(cls.LISTING_DETAILS_CELL_LABEL_PRICE, '')
        price_str = cls._sanitize_string(price_str)
        price = cls._parse_price(price_str)

        unit = Unit(
            address=address,
            bedrooms=bedrooms
        )
        return Listing(
            unit=unit,
            price=price,
            source=cls.SOURCE
        )


    @classmethod
    def scrape_listings(cls, search_result: ApartmentsDotComSearchResult, scraping_params: config.ScrapingParams) -> List[Listing]:
        '''Fully scrape an apartments.com search result.'''
        
        listings = []
        try:
            soup = cls.get_url(search_result.url)

            all_results_tab_element = soup.find(attrs={cls.ALL_UNITS_TAB_ATTRIBUTE_NAME: cls.ALL_UNITS_TAB_ATTRIBUTE_VALUE})
            multi_unit = all_results_tab_element is not None

            # Parse search results with multiple units.
            if multi_unit:
                listings += cls._parse_multi_listing_search_result(page_soup=soup, building_address=search_result.address)

            # Parse simple, single-listing search results.
            else:
                listings += [cls._parse_single_listing_search_result(page_soup=soup)]

            # Filter to only valid listings.
            listings = [l for l in listings if cls.is_valid_listing(listing=l, params=scraping_params)]

        except Exception as e:
            exception_type = type(e)
            raise exception_type(f'Error fully scraping searh result {search_result.id} ({search_result.url}): {e}') from e
        
        return listings
