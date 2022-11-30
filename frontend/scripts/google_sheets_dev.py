'''Dev script for google sheets interaction.'''

import json

import glog

from housing.frontend.google_sheets_client import GoogleSheetsClient

SPREADSHEET_ID = '196O1m_cgE9FIjHZ_gyAzAhER2QRnuTuwnf72iL43Wr4'  # 'housing bot test'
UPDATE_RANGE = 'B1'
UPDATE_VALUES = [
    ['test1', 'test2'],
    ['testA', 'testB']
]
NEW_ROW = ['dataNA', 'dataNB', 'dataNC']
SMART_APPEND_DATA = [
    {'a': 0, 'b': 2, 'c': 3}
]

def main():

    # Setup client
    glog.info(f'Attempting google sheets interaction...')
    client = GoogleSheetsClient(spreadsheet_id=SPREADSHEET_ID)
    glog.info(f'..Created client')
    
    # Fetch sheet data
    sheet_data = client.get()
    glog.info(f'Got original sheet data: {json.dumps(sheet_data)}')

    # Update sheet data & refetch
    # client.update(_range=UPDATE_RANGE, _values=UPDATE_VALUES)
    # client.append(_range=UPDATE_RANGE, _values=[NEW_ROW, NEW_ROW])
    client.smart_append(_values=SMART_APPEND_DATA)
    sheet_data = client.get()
    glog.info(f'Got updated sheet data: {json.dumps(sheet_data)}')




if __name__ == '__main__':
    main()