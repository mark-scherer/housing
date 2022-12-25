'''Scrape specified scraper_configs and store the results in the DB.

TODO
- load test apartments.com scraper
- respect config.scrapers field
- expand apartments.com to all areas of interest and fix any new bugs
- implement zillow scraper

python scrapers/scripts/scrape_and_record.py \
    --env=dev \
    --max_search_results=500 \
    --max_scraped_search_results=500

To see count of recent requests:
    select domain, count(*)
    from housing_requests
    where created_at > now () - interval '1 day'
    group by domain order by count(*) desc;
'''

from os import path
import json
from typing import NamedTuple, Dict, List

import glog
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session

from housing.configs.config import Config
from housing.data.db_client import DbClient
from housing.data.schema import Unit
from housing.scrapers.apartments_dot_com import ApartmentsDotCom
from housing.scrapers.scraper import Scraper

# Move these to args!
CONFIG_PATHS = [
    # '/Users/mark/Documents/housing/configs/dev.yaml'
    # '/Users/mark/Documents/housing/configs/scraper_load_test.yaml'
    '/Users/mark/Documents/housing/configs/seattle.yaml'
]

SCRAPERS = [
    ApartmentsDotCom
]


class SingleScrapeResult(NamedTuple):
    '''For storing metadata about a single scrape result.'''
    units: int
    listings: int


class FullScrapeResult(NamedTuple):
    '''Metadata about an entire scraping run'''
    # Counts of listings scraped by config then scraper
    listing_counts: Dict[str, Dict[str, int]]

    def to_table(self) -> str:
        '''Formats as a table ready for printing.'''
        listings_summary_df = pd.DataFrame.from_dict(self.listing_counts)
        return listings_summary_df


def scrape_and_record_one(config: Config, scraper: Scraper, db_session: Session) -> SingleScrapeResult:
    '''Scrape and record results in the DB for a given config/scraper combo.
    
    Return: metadata about successfully recorded results.
    '''
    scraping_params = config.scraping_params
    scraped_listings = scraper.search_and_scrape(params=scraping_params)

    new_units = 0
    for i, listing in enumerate(scraped_listings):
        try:
            unit = listing.unit

            # Check if unit already in db.
            found_unit = db_session.query(Unit).filter(Unit.address_str == unit.address_str).first()
            if found_unit:
                unit = found_unit

            # Otherwise add it.
            else:
                db_session.add(unit)
                db_session.flush()  # Need to flush to have unit.id assigned
                new_units += 1
            
            listing.unit_id = unit.id
            db_session.add(listing)
        except Exception as e:
            raise RuntimeError(f'error recording scraped listing {i}/{len(scraped_listings)}: {json.dumps(listing.to_dict())}: {e}') from e
    
    db_session.commit()
    return SingleScrapeResult(units=new_units, listings=len(scraped_listings))


def scrape_and_record_all(configs: List[Config], scrapers: List[Scraper], db_session: Session) -> FullScrapeResult:
    glog.info(f'Attempting to scrape {len(configs)} scraper_configs across {len(scrapers)} sources...')
    listing_counts = {}
    for i, config in enumerate(configs):
        listing_counts[config.name] = {}
        
        for j, scraper in enumerate(scrapers):
            scrape_metadata = {"config": config.name, "scraper": scraper.__name__}
            scrape_progress = f'config {i}/{len(configs)}, scraper {j}/{len(scrapers)}'
            glog.info(f'Attempting scrape {scrape_progress}: {json.dumps(scrape_metadata)}')
            
            try:
                scrape_result = scrape_and_record_one(config=config, scraper=scraper, db_session=db_session)
                
                listing_counts[config.name][scraper.__name__] = scrape_result.listings
                glog.info(f'..finished scrape {scrape_progress}: {json.dumps(scrape_metadata)}: {json.dumps(scrape_result._asdict())}')
            except Exception as e:
                raise RuntimeError(f'Error with scrape {scrape_progress}: {json.dumps(scrape_metadata)}: {e}') from e
    
    return FullScrapeResult(listing_counts=listing_counts)


def main():
    db_client = DbClient()
    db_session = db_client.session()
    
    configs = [Config.load_from_file(config_path) for config_path in CONFIG_PATHS]

    full_scrape_results = scrape_and_record_all(configs=configs, scrapers=SCRAPERS, db_session=db_session)
    glog.info(f'..finished scraping {len(CONFIG_PATHS)} scraper_configs across {len(SCRAPERS)} sources - scraped listings:\n{full_scrape_results.to_table()}')


if __name__ == '__main__':
    main()