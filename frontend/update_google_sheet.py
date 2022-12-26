'''Fetch data from DB and update a google sheet.'''

from typing import List, Dict
import json
from datetime import datetime
from dataclasses import fields
import argparse

import glog

from housing.configs.config import Config
from housing.data.unit_listing import UnitListing
from housing.frontend.google_sheets_client import GoogleSheetsClient, SheetData

parser = argparse.ArgumentParser()
parser.add_argument('--dry_run', action=argparse.BooleanOptionalAction, required=True,
    help='If dry_run, skips actual sheet updates.')
FLAGS = parser.parse_args()

SORT_HEADER = 'sort_value'
UPDATED_AT_TS_FORMAT = '%m/%d/%y %I:%M%p'


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
    
    possible_headers = [f.name for f in fields(UnitListing)]
    sheets_client = GoogleSheetsClient(config.spreadsheet_id, possible_headers=possible_headers)

    # Fetch DB data
    results = UnitListing.get_all_unit_listings(config.scraping_params)
    glog.info(f'Fetched {len(results)} unit listings from DB for config: {config.name}')

    # Find necessary updates.
    new_sheet_data = [sheet_row.to_dict() for sheet_row in results]
    # glog.info(f'Attempting to make {len(new_sheet_data)} updates to the sheet: {json.dumps(new_sheet_data)}')
    
    # Update sheet.
    if not FLAGS.dry_run:
        sheets_client.smart_update(_values=new_sheet_data, primary_keys=UnitListing.PRIMARY_KEYS, sort_key=SORT_HEADER, sort_asc=False)
    glog.info(f'..updated sheet with new db data.')

    # Fill in sheet metadata.
    metadata = _sheet_metadata(config, upserted_data=results)
    metadata_top_row_num = sheets_client.header_row_num - len(metadata)
    assert metadata_top_row_num >= 0, f'Not enough room for {len(metadata)} rows with current header position: row {sheets_client.header_row_num}'
    metadata_tl = GoogleSheetsClient.row_col_num_to_A1(row_num=metadata_top_row_num, col_num=0)
    if not FLAGS.dry_run:
        sheets_client.update(_range=metadata_tl, _values=metadata)
    
    