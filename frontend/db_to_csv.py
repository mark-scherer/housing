'''Converts DB data to local csv.'''

from pathlib import Path
from os import path
from datetime import datetime

import glog

from housing.configs.config import Config
from housing.frontend import db_to_sheet

CONFIG_PATH = '/Users/mark/Documents/housing/configs/dev.yaml'
OUTPUT_DIR = '/Users/mark/Downloads/housing_csvs'

FILENAME_TS_FORMAT = '%m-%d-%y_%H-%M'


def main():
    config = Config.load_from_file(CONFIG_PATH)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    output_name = f'{config.name}__{datetime.now().strftime(FILENAME_TS_FORMAT)}.csv'
    output_path = path.join(OUTPUT_DIR, output_name)

    glog.info(f'Attempting to write data for config {CONFIG_PATH} to {output_path}..')

    sheet_data = db_to_sheet.db_to_sheet(config=config)

    with open(output_path, 'w', newline='') as file:
        for row in sheet_data:
            file.write(','.join(row) + '\n')
        file.close()

        glog.info(f'..wrote {len(sheet_data)} rows to {output_path}.')

if __name__ == '__main__':
    main()