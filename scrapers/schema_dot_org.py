'''Utils for parsing schema.org objects.'''

from typing import Dict

from housing.data.address import Address
from housing.scrapers import scraper


def parse_postal_address(postal_address: Dict) -> Address:
    '''Parse schema.org PostalAddress into a Address'''
    short_address = postal_address['streetAddress']
    city = postal_address['addressLocality']
    state = postal_address['addressRegion']
    zipcode = postal_address['postalCode']

    return Address(
        short_address=short_address,
        city=city,
        state=state,
        zipcode=zipcode
    )


def parse_apartment_complex(apartment_complex: Dict) -> scraper.SearchResult:
    '''Parse schema.org ApartmentComplex into a PartialListing'''
    address = parse_postal_address(apartment_complex['Address'])
    url = apartment_complex['url']
    id = address.id()
    return scraper.SearchResult(
        id=id,
        url=url,
        address=address
    )