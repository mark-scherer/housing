'''Script for scoring dev.'''

import json

import glog

from housing.configs.config import Config
from housing.data.unit_listing import UnitListing
from housing.models import score

CONFIG_PATH = '/Users/mark/Documents/housing/configs/seattle.yaml'
UNIT_ID = 1384 # Unit_id to score.

def main():
    config = Config.load_from_file(CONFIG_PATH)
    all_unit_listings = UnitListing.get_all_unit_listings(config.scraping_params)
    
    unit_listings = [ul for ul in all_unit_listings if ul.unit_id == UNIT_ID]
    if len(unit_listings) == 0:
        raise ValueError(f'Could not find unit_id {UNIT_ID} in {len(all_unit_listings)} returned unit_listings.')
    ul = unit_listings[0]

    ul_score, score_components = score.score(ul)
    glog.info(f'got score for unit_id {UNIT_ID}: {ul_score}: {json.dumps(score_components)}')


main()

