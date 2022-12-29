'''Fetch data from DB and update a google sheet.'''

from typing import List, Dict
import json
from datetime import datetime
import argparse

import glog

from housing.configs.config import Config
from housing.data.unit_listing import UnitListing
from housing.frontend.google_sheets_client import GoogleSheetsClient, SheetData
from housing.models import score

parser = argparse.ArgumentParser()
parser.add_argument('--dry_run', action=argparse.BooleanOptionalAction, required=True,
    help='If dry_run, skips actual sheet updates.')
FLAGS = parser.parse_args()

SORT_HEADER = 'sort_value'
UPDATED_AT_TS_FORMAT = '%m/%d/%y %I:%M%p'
DEBUG_UPDATE_DUMP_FILEPATH = '/Users/mark/Downloads/housing_google_sheet_update_data.json'


def _sheet_metadata(config: Config, upserted_data: List[UnitListing]) -> SheetData:
    '''Generate sheet metadata above the header row.'''
    scraping_params = config.scraping_params

    def _bedrooms_str() -> str:
        result = None
        min_bedrooms = scraping_params.min_bedrooms
        max_bedrooms = scraping_params.max_bedrooms
        if min_bedrooms == max_bedrooms:
            result = f'{min_bedrooms} BR'
        else:
            result = f'{min_bedrooms}-{max_bedrooms} BR'
        return result

    def _price_str() -> str:
        result = None
        min_price = scraping_params.min_price
        max_price = scraping_params.max_price
        if min_price == max_price:
            result = f'${min_price}'
        else:
            result = f'${min_price}-${max_price}'
        return result

    def _zipcodes_str() -> str:
        return ", ".join(scraping_params.zipcodes)

    def _scrapers_str() -> str:
        return ", ".join(scraping_params.scrapers)

    explanation_row = f'Rows are added programtically by apt-bot from {_scrapers_str()} for: {_bedrooms_str()}, {_price_str()} in zipcodes: {_zipcodes_str()}. ' \
        f'Last found {len(upserted_data)} listings at:'
    ts_row = datetime.now().strftime(UPDATED_AT_TS_FORMAT)

    return [
        [explanation_row],
        [ts_row],
    ]


def update_google_sheet(config: Config) -> None:
    '''Fetches DB data for specified config and updates its google sheet.'''
    
    possible_headers = UnitListing.fields()
    sheets_client = GoogleSheetsClient(config.spreadsheet_id, possible_headers=possible_headers)

    # Fetch DB data
    results = UnitListing.get_all_unit_listings(config.scraping_params)
    for ul in results:
        ul.predicted_score, _ = score.score(ul)
    glog.info(f'Fetched {len(results)} unit listings from DB for config: {config.name}')

    # Find necessary updates.
    new_sheet_data = [sheet_row.to_sheet_update() for sheet_row in results]
    
    # Update sheet.
    if not FLAGS.dry_run:
        sheets_client.smart_update(_values=new_sheet_data, primary_keys=UnitListing.PRIMARY_KEYS, sort_key=SORT_HEADER, sort_asc=False)
        glog.info(f'..updated sheet with new db data.')
    else:
        with open(DEBUG_UPDATE_DUMP_FILEPATH, 'w', encoding='utf-8') as f:
            json.dump(new_sheet_data, f, ensure_ascii=False, indent=4)
        glog.info(f'wrote update of {len(new_sheet_data)} sheet rows to {DEBUG_UPDATE_DUMP_FILEPATH}')

    # Fill in sheet metadata.
    metadata = _sheet_metadata(config, upserted_data=results)
    metadata_top_row_num = sheets_client.header_row_num - len(metadata)
    assert metadata_top_row_num >= 0, f'Not enough room for {len(metadata)} rows with current header position: row {sheets_client.header_row_num}'
    metadata_tl = GoogleSheetsClient.row_col_num_to_A1(row_num=metadata_top_row_num, col_num=0)
    if not FLAGS.dry_run:
        sheets_client.update(_range=metadata_tl, _values=metadata)
    
    