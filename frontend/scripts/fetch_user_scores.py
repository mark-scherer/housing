'''Download user scores from a specified apt-bot google sheet.

python frontend/scripts/fetch_user_scores.py \
    --config_filepath=/Users/mark/Documents/housing/configs/seattle.yaml \
    --no-dry_run
'''

import argparse
import json
from typing import Dict, NamedTuple, List

import glog

from housing.configs.config import Config
from housing.data.db_client import DbClient
from housing.data.unit_listing import UnitListing
from housing.data.schema import Score
from housing.frontend.google_sheets_client import GoogleSheetsClient

parser = argparse.ArgumentParser()
parser.add_argument('--config_filepath', type=str, required=True, 
    help='Filepath of config to download user scores for.')
parser.add_argument('--dry_run', action=argparse.BooleanOptionalAction, required=True,
    help='If dry_run, skips actual DB updates.')
FLAGS = parser.parse_args()

UNIT_SCORE_TYPE = 'unit'
LOCATION_SCORE_TYPE = 'location'
MAX_SCORE_VALUE = 5.0
DEBUG_UPDATE_DUMP_FILEPATH = '/Users/mark/Downloads/housing_score_update_data.json'

class UpsertMetadata(NamedTuple):
    '''Wrapper around upsert metadata.'''
    new_rows: int
    updated_rows: int
    update_data: List[Dict]

def _upsert_score_type(
        score_data: Dict,
        unit_id: str,
        config_name: str,
        score_type: str,
        db_session) -> UpsertMetadata:
    '''Helper for upserting scores of the specified type.
    
    Return: number of new scores.
    ''' 
    new_scores = 0
    updated_scores = 0
    update_data = []  # For Debugging purposes.
    for user, score in score_data.items():
        if score is None:
            continue
        
        try:
            # Find or create new Score.
            score_row = db_session.query(Score).filter(
                Score.unit_id == unit_id, Score.user == user, Score.type == score_type
            ).first()
            existing_row = score_row is not None
            if not existing_row:
                score_row = Score(
                    unit_id=unit_id,
                    user=user,
                    type=score_type,

                    # Following fields filled in below, here given junk values.
                    configs=[],
                    score=-1
                )
            
            # Update Score data.
            score_row.configs = list(set(score_row.configs + [config_name]))
            score_row.configs = sorted(score_row.configs)
            score_row.score = score / MAX_SCORE_VALUE

            update_data.append(score_row.to_dict())
            if not existing_row:
                if not FLAGS.dry_run:
                    db_session.add(score_row)
                new_scores += 1
            else:
                updated_scores += 1
                
        except Exception as e:
            raise RuntimeError(f'Error upserting {score_type} score for user: {user}') from e

    return UpsertMetadata(new_rows=new_scores, updated_rows=updated_scores, update_data=update_data)


def main():
    db_client = DbClient()

    # Load scored listings for specified config.
    config = Config.load_from_file(FLAGS.config_filepath)
    sheets_client = GoogleSheetsClient(config.spreadsheet_id, possible_headers=UnitListing.fields())
    sheet_data = sheets_client.smart_get()
    sheet_unit_listings = [UnitListing.from_dict(row) for row in sheet_data]
    # If any score is manually input average_score will be populated, so can filter on that field.
    scored_sheet_unit_listings = [
        ul for ul in sheet_unit_listings
        if (ul.user_unit_scores or ul.user_location_scores) and ul.unit_id is not None]
    glog.info(f'Parsed {len(scored_sheet_unit_listings)} scored UnitListings from {len(sheet_unit_listings)} google sheet rows.')

    # Upsert scores in the DB.
    db_session = db_client.session()
    new_scores = 0
    updated_scores = 0
    update_data = []  # For debugging purposes.
    for score_ul in scored_sheet_unit_listings:
        try:
            _new_scores, _updated_scores, _update_data = _upsert_score_type(
                score_data=score_ul.user_unit_scores,
                unit_id=score_ul.unit_id,
                config_name=config.name,
                score_type=UNIT_SCORE_TYPE,
                db_session=db_session
            )
            new_scores += _new_scores
            updated_scores += _updated_scores
            update_data += _update_data
            
            _new_scores, _updated_scores, _update_data = _upsert_score_type(
                score_data=score_ul.user_location_scores,
                unit_id=score_ul.unit_id,
                config_name=config.name,
                score_type=LOCATION_SCORE_TYPE,
                db_session=db_session
            )
            new_scores += _new_scores
            updated_scores += _updated_scores
            update_data += _update_data
        except Exception as e:
            raise RuntimeError(f'Error upserting score for scored UnitListing: {json.dumps(score_ul.to_dict())}') from e
    
    if FLAGS.dry_run:
        with open(DEBUG_UPDATE_DUMP_FILEPATH, 'w', encoding='utf-8') as f:
            json.dump(update_data, f, ensure_ascii=False, indent=4)
        glog.info(f'wrote upsert of {len(update_data)} scores to {DEBUG_UPDATE_DUMP_FILEPATH}')
    else:
        db_session.commit()
        glog.info(f'Inserted {new_scores} new scores and updated {updated_scores} under config: {config.name}.')


main()