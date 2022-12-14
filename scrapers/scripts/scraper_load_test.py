'''Hit scraper as hard as possible until failure.

python -u scrapers/scripts/scraper_load_test.py \
    --env=load_test \
    --max_search_results=1000 \
    --max_scraped_search_results=500 \
    --ip_description="madrone starbucks wifi"
    2>&1 | tee ~/Downloads/housing_scraper_load_tests/apartment_dot_com_no_throttling.txt
'''

from datetime import datetime

import glog

from housing.configs.config import Config
from housing.data.db_client import DbClient
from housing.scrapers.scripts import scrape_and_record
from housing.scrapers.apartments_dot_com import ApartmentsDotCom

CONFIG_PATH = '/Users/mark/Documents/housing/configs/scraper_load_test.yaml'
SCRAPER = ApartmentsDotCom

def main():
    db_client = DbClient()
    db_session = db_client.session()
    
    config = Config.load_from_file(CONFIG_PATH)

    loops = 0
    start_time = datetime.now()
    glog.info(f'Attempting to scrape {SCRAPER.__name__} until failure...')
    while True:
        loop_start = datetime.now()
        total_elapsed = round(loop_start.timestamp() - start_time.timestamp())
        glog.info(f'Attempting loop {loops} {total_elapsed}s after start')

        try:
            loop_result = scrape_and_record.scrape_and_record_all(
                configs=[config],
                scrapers=[SCRAPER],
                db_session=db_session,
            )
            total_elapsed = round(datetime.now().timestamp() - start_time.timestamp())
            loop_elapsed = round(loop_start.timestamp() - start_time.timestamp())
            glog.info(f'Finished loop {loops} after {loop_elapsed}s ({total_elapsed}s total), result:\n{loop_result.to_table()}')
            loops += 1
        except Exception as e:
            total_elapsed = round(datetime.now().timestamp() - start_time.timestamp())
            raise RuntimeError(f'scrape loop {loops} failed after {total_elapsed}s total') from e


if __name__ == '__main__':
    main()