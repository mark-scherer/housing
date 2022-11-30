'''Google Sheets client - helper for interacting with google sheets.

TODO
- add sorted column arg to smart_append that resorts sheet by specified column after append
- switch auth from oauth (personal account requiring manual sign in) to SA
'''

from typing import Optional, List, Any, Dict
from os import path
from pathlib import Path
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import glog

CREDS_FILEPATH = '/etc/keys/housing_bot_oauth_secret.json'  # creds secret filepath
DEFAULT_TOKEN_FILEPATH = '/tmp/housing_bot_oauth_token.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
FULL_SHEET_RANGE = 'A1:Z999'
UPDATE_VALUE_INPUT_OPTION = 'USER_ENTERED'

SheetData = List[List[Any]]  # Sheet data as a nested list of cells
SmartSheetData = List[Dict[str, Any]]  # Sheet data as a list of keyed row data.

class GoogleSheetsClient:
    '''Helper for interacting with google sheets.'''

    def __init__(self, spreadsheet_id: str):
        self.creds = self._get_creds()
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.sheet = self.service.spreadsheets()
        
        self.spreadsheet_id = spreadsheet_id


    def get(self, _range: Optional[str] = FULL_SHEET_RANGE) -> SheetData:
        '''Get sheet data.
        
        Args:
            range: range to fetch in A1 notation, if excluded will fetch the whole sheet.

        Return: Nested list of formatted cell data.
        '''
        request = self.sheet.values().get(spreadsheetId=self.spreadsheet_id, range=_range)
        response = request.execute()
        assert response and response['values']
        return response['values']


    def update(self, _range: str, _values: SheetData) -> None:
        '''Update sheet data. 
        
        - This is a dumb update that just overwrites existing data with provided values
            starting at the top left of specified range.
        - Will error if provided values actually require a greater range than specified.
        - Will not modify existing data in specified range outside of the actual
            range required by the specified values.
        
        Args:
            _range: range to update in A1 notation.
            _values: values to set in specified range.
        '''
        request_body = {'values': _values}
        request = self.sheet.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=_range,
            body=request_body,
            valueInputOption=UPDATE_VALUE_INPUT_OPTION,
        )
        request.execute()


    def append(self, _range: str, _values: SheetData) -> None:
        '''Append rows to sheet.

        - Appends to the end of the first existing 'table' found in the specified range:
            - If specified range's top left cell is empty, appends begining at top left.
            - If specified range's top left cell is not empty, appends begining at bottom left
                of 'table' found to include range's top left cell, even if it's outside of
                specified range.
            - Will overwrite existing data if outside of 'table' discovered at specfied range's
                top left cell, unless the existing data is within the specified range - in which
                case append will start below existing data.
        - Tip: assuming the table has no footer, just set _range to the top left header cell.
        - Is a dumb update, just pastes in new data at the bottom of the found table
            regardless of if the data matches semantically or not.

        Args:
            _range: range to search for existing 'table' to append data to, in A1 notation.
            _values: values to append to found 'table'.
        '''
        request_body = {'values': _values}
        request = self.sheet.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=_range,
            body=request_body,
            valueInputOption=UPDATE_VALUE_INPUT_OPTION,
        )
        request.execute()


    def smart_append(self, _values: SmartSheetData) -> None:
        '''Append rows to sheet, inteligently matching existing table structure.
        
        - Peeks into column names in provided sheet data, then searches existing sheet
            for header row.
        - Reorders sheet data to match existing headers, including omitting missing columns.
        - Appends new rows to table starting at found header row.
        - Will error if:
            - No header row is found in the exisitng sheet where any values match any keys
                to peeked row to append.
            - Any row to append has no overlapping keys with found header row.
        - Appended data WILL play nice with filters and filter views but make sure
            filter doesn't filter out headers!
            - It's tricky to create filter proper, but try to highlight just header + all 
                exisitng data rows (DO NOT include entire column or any rows above 
                the header!)
        - Appended data WILL NOT play nice with sorts of either type b/c they are one off ops
            and do not persist through new data - need to resort sheet after append.
            - Sheet sorts (Data > Sort sheet > *) 
            - Column sorts (Data > Create a filter > Sort *)

        Args:
            _values: rows to append.
        '''

        def _sanitize_header(input_header: str) -> str:
            '''helper for sanitizing header value for fair match comparsion.'''
            return input_header.lower().strip().replace(' ', '_')

        def _is_headers_match(header_1: str, header_2: str) -> bool:
            '''Helper for determining if two headers are considered a match.'''
            return _sanitize_header(header_1) == _sanitize_header(header_2)
        
        # Find existing header row.
        possible_headers = list(_values[0].keys())
        existing_sheet_data = self.get()
        headers_tl = None
        for i, existing_row in enumerate(existing_sheet_data):
            for j, existing_cell in enumerate(existing_row):
                if any(_is_headers_match(possible_header, existing_cell) for possible_header in possible_headers):
                    column_letter = chr(ord('A') + j)
                    headers_tl = f'{column_letter}{i+1}'
                    break
            if headers_tl:
                break  # Need another break out of the outer loop.
        if not headers_tl:
            raise ValueError(f'Could not find any of these provided headers ({possible_headers}) in existing sheet: {json.dumps(existing_sheet_data)}')
        headers = existing_sheet_data[i][j:]
        glog.info(f'Found these headers starting at {headers_tl}: {headers}')

        # Create cell values to append.
        append_data = []
        for i, row_to_append in enumerate(_values):
            row_data = []
            populated_cells = 0
            for header in headers:
                cell_data = row_to_append.get(_sanitize_header(header), '')
                row_data.append(cell_data)
                if cell_data:
                    populated_cells += 1
            
            # Verify at least some overlap between found headers and row to append.
            if populated_cells == 0:
                raise ValueError(f'Did not find of sheet\'s headers ({headers}) populated in {i} row to append: {json.dumps(row_to_append)}')
            
            append_data.append(row_data)

        # Append data.
        self.append(_range=headers_tl, _values=append_data)



    def _get_creds_oauth(self, token_filepath: Optional[str] = DEFAULT_TOKEN_FILEPATH) -> Credentials:
        '''Gets creds via oauth, where user allows access via their personal account - 
        actions taken will be credited to their user.

        Args:
            token_filepath: filepath to oauth token json, if excluded will use default.
                Will overwrite with valid token if missing, invalid or expired.

        Return: Credentials object to be used creating google Resource.
        '''
        creds = None
        if path.exists(token_filepath):
            creds = Credentials.from_authorized_user_file(token_filepath, scopes=SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILEPATH, SCOPES)
                creds = flow.run_local_server(port=0)
            
            token_dir = path.dirname(token_filepath)
            Path(token_dir).mkdir(parents=True, exist_ok=True)
            with open(token_filepath, 'w') as token:
                token.write(creds.to_json())

        return creds


    def _get_creds(self) -> Credentials:
        '''Abstraction around creds generation to allow multiple creds types.
        
        Return: Credentials object to be used creating google Resource.
        '''
        return self._get_creds_oauth()

