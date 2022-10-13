'''Universal scraping interface.'''

from typing import NamedTuple, List

# We'll move these out to data/ or similar eventually
class PartiaListing(NamedTuple):
    '''An incomplete listing that can be scraped from search results.'''


class Listing(NamedTuple):
    '''A complete listing.'''

    # Unit details
    bedrooms: int

    # Location details
    zipcode: int

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
    zipcode: int

    # Listing parms
    min_price: int
    max_price: int


class Scraper:
    '''Universal scraping interface.
    
    No need for any instance methods/vars, just class methods/vars.
    '''

    @classmethod
    def scrape_partial_listings(cls, ScrapingParams) -> List[PartiaListing]:
        '''Scrape partial listings from a search page.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    def scrape_full_listing(cls, PartiaListing) -> Listing:
        '''Fully scrape a partial listing.'''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')
