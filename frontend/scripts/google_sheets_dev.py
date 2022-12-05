'''Dev script for google sheets interaction.'''

import json

import glog

from housing.frontend.google_sheets_client import GoogleSheetsClient, GridRange

SPREADSHEET_ID = '196O1m_cgE9FIjHZ_gyAzAhER2QRnuTuwnf72iL43Wr4'  # 'housing bot test'
UPDATE_RANGE = 'B1'
UPDATE_VALUES = [
    ['test1', 'test2'],
    ['testA', 'testB']
]
NEW_ROW = ['dataNA', 'dataNB', 'dataNC']
SMART_APPEND_DATA = [
    {'headerA': 6, 'HeaderB': 'z', 'HeaderC': 'y'}
]
TEST_SORT_GRIDRANGE = GridRange(
    min_col=1,
    max_col=5,
    min_row=1,
    max_row=100
)
SMART_UPDATE_DATA = [
    {'primary_key': '1', 'colA': 11},
    {'primary_key': '2', 'colB': 22},
    {'primary_key': '5', 'colC': 555},
]
HEADERS = ['primary_key', 'colA', 'colB', 'colC']

def main():

    # Setup client
    glog.info(f'Attempting google sheets interaction...')
    client = GoogleSheetsClient(spreadsheet_id=SPREADSHEET_ID, possible_headers=HEADERS)
    glog.info(f'..Created client')
    
    # Fetch sheet data
    # sheet_data = client.get()
    sheet_data = client.smart_get()
    glog.info(f'Got original sheet data: {json.dumps(sheet_data)}')

    # Update sheet data & refetch
    # client.update(_range=UPDATE_RANGE, _values=UPDATE_VALUES)
    # client.append(_range=UPDATE_RANGE, _values=[NEW_ROW, NEW_ROW])
    # client.smart_append(_values=SMART_APPEND_DATA, sort_key='headerA', sort_asc=False)
    # client.sort(1, grid_range=TEST_SORT_GRIDRANGE, asc=False)
    client.smart_update(_values=SMART_UPDATE_DATA, primary_key='primary_key')
    sheet_data = client.smart_get()
    glog.info(f'Got updated sheet data: {json.dumps(sheet_data)}')




if __name__ == '__main__':
    main()