'''Fetch results from DB and update google sheet.

python frontend/scripts/update_google_sheets.py \
    --no-dry_run

TODO: BUGFIX:
- newly appended rows overwrite calculated columns
'''

import glog

from housing.configs.config import Config
from housing.frontend.update_google_sheet import update_google_sheet

CONFIG_PATH = '/Users/mark/Documents/housing/configs/seattle.yaml'


def main():
    config = Config.load_from_file(CONFIG_PATH)
    glog.info(f'loaded {config.name} from config: {CONFIG_PATH}, attempting to update sheet.')
    update_google_sheet(config)


if __name__ == '__main__':
    main()