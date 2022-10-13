'''Various scraper testing.'''

import sys

import glog

from housing.scrapers import scraper
from housing.scrapers.apartments_dot_com import ApartmentsDotCom

def main():
    test_scraper = ApartmentsDotCom()
    test_params = scraper.ScrapingParams(
        min_bedrooms=2,
        max_bedrooms=3,
        zipcodes=[94158],
        min_price=1000,
        max_price=3000
    )
    
    test_scraper.scrape_partial_listings(test_params)


main()