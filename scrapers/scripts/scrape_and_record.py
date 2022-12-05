'''Scrape specified scraper_configs and store the results in the DB.

TODO
- respect config.scrapers field
- add (possibly scraped) bathrooms, pets allowed fields
- fix apartments.com scraping issues and load test
- implement zillow scraper
'''

from os import path
import json
from typing import NamedTuple

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
    '/Users/mark/Documents/housing/configs/dev.yaml'
]

SCRAPERS = [
    ApartmentsDotCom
]


class ScrapeResult(NamedTuple):
    '''For storing metadata about scrape result.'''
    units: int
    listings: int


def scrape_and_record(config: Config, scraper: Scraper, db_session: Session) -> ScrapeResult:
    '''Scrape and record results in the DB.
    
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
    return ScrapeResult(units=new_units, listings=len(scraped_listings))



def main():
    db_client = DbClient()
    db_session = db_client.session()
    
    glog.info(f'Attempting to scrape {len(CONFIG_PATHS)} scraper_configs across {len(SCRAPERS)} sources...')
    scraped_listings_summary = {}
    for i, config_path in enumerate(CONFIG_PATHS):
        config = Config.load_from_file(config_path)
        scraped_listings_summary[config.name] = {}
        
        for j, scraper in enumerate(SCRAPERS):
            scrape_metadata = {"config_path": config_path, "scraper": scraper.__name__}
            scrape_progress = f'config {i}/{len(CONFIG_PATHS)}, scraper {j}/{len(SCRAPERS)}'
            glog.info(f'Attempting scrape {scrape_progress}: {json.dumps(scrape_metadata)}')
            
            try:
                scrape_result = scrape_and_record(config=config, scraper=scraper, db_session=db_session)
                
                scraped_listings_summary[config.name][scraper.__name__] = scrape_result.listings
                glog.info(f'..finished scrape {scrape_progress}: {json.dumps(scrape_metadata)}: {json.dumps(scrape_result._asdict())}')
            except Exception as e:
                raise RuntimeError(f'Error with scrape {scrape_progress}: {json.dumps(scrape_metadata)}: {e}') from e
    
    summary_df = pd.DataFrame.from_dict(scraped_listings_summary)
    glog.info(f'..finished scraping {len(CONFIG_PATHS)} scraper_configs across {len(SCRAPERS)} sources - scraped listings:\n{summary_df}')


if __name__ == '__main__':
    main()