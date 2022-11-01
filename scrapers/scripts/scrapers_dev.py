'''Various scraper testing.'''

import sys
import json

import glog

from housing.scrapers import scraper
from housing.scrapers.apartments_dot_com import ApartmentsDotCom

def main():
    test_scraper = ApartmentsDotCom()
    test_params = scraper.ScrapingParams(
        min_bedrooms=0,
        max_bedrooms=3,
        zipcodes=frozenset(['94158']),
        min_price=1000,
        max_price=10000,
        max_results=30
    )

    test_results = test_scraper.scrape_search_results(test_params)
    results_obj = [pl._asdict() for pl in test_results]
    glog.info(f'Results: {json.dumps(results_obj)}')


main()