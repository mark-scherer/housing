'''Various scraper testing.'''

import sys
import json

import glog

from housing.scrapers import scraper
from housing.scrapers.apartments_dot_com import ApartmentsDotCom

RESULTS_TO_FULLY_SCRAPE = 1

def main():
    test_scraper = ApartmentsDotCom()
    test_params = scraper.ScrapingParams(
        min_bedrooms=1,
        max_bedrooms=2,
        zipcodes=frozenset(['94158']),
        min_price=0,
        max_price=4000,
        max_results=10
    )

    test_results = test_scraper.scrape_search_results(test_params)
    glog.info(f'scraped {len(test_results)} search results, now attempting to fully scrape {RESULTS_TO_FULLY_SCRAPE}..')
    # results_obj = [pl._asdict() for pl in test_results]
    # glog.info(f'Results: {json.dumps(results_obj)}')

    for i, result in enumerate(test_results[0:RESULTS_TO_FULLY_SCRAPE]):
        glog.info(f'attempting to scrape result {i}: {result.id}..')

        scraped_listings = test_scraper.scrape_listings(search_result=result, scraping_params=test_params)

        listings_obj = [listing.to_dict() for listing in scraped_listings]
        glog.info(f'..finished scraping search result {i}: {result.id}. Found {len(scraped_listings)} listings: {json.dumps(listings_obj)}')


main()