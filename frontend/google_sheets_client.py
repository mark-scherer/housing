'''Google Sheets client - helper for interacting with google sheets.

TODO
- switch auth from oauth (personal account requiring manual sign in) to SA
'''

from typing import Optional, List, Any, Dict, NamedTuple, Tuple
from os import path
from pathlib import Path
import json
from collections import OrderedDict

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


class HeaderData(NamedTuple):
    header_to_col_num: OrderedDict[str, int]  # Maps santized column names to 0-based col numbers
    header_row_num: int  # 0-based header row number


class GridRange(NamedTuple):
    '''Google has special GridRange object that's represented like this.'''
    min_col: int
    min_row: int
    max_col: int
    max_row: int

    def to_google_GridRange(self, sheet_num: int) -> Dict:
        '''Convert to a google GridRange object.'''
        return {
            'sheetId': sheet_num,
            'startRowIndex': self.min_row,
            'endRowIndex': self.max_row,
            'startColumnIndex': self.min_col,
            'endColumnIndex': self.max_col
        }


class GoogleSheetsClient:
    '''Helper for interacting with google sheets.'''

    MAX_ROWS = 999
    MAX_COLS = 26

    @staticmethod
    def row_col_num_to_A1(row_num: int, col_num: int) -> str:
        '''Convert 0-based row & col nums to A1 notation.'''
        return f'{chr(ord("A") + col_num)}{row_num + 1}'

    def __init__(self, spreadsheet_id: str, possible_headers: List[str]):
        '''Create GoogleSheetsClient.
        
        Args:
            spreadsheet_id: Google sheet string id, 
                ex: https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit#gid=0
            possible_headers: list of expected sheet headers (sanitized)
        '''

        self.creds = self._get_creds()
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.sheet_service = self.service.spreadsheets()
        
        self.spreadsheet_id = spreadsheet_id
        self.possible_headers = possible_headers

        self.header_to_col_num = None
        self.header_row_num = None
        self.sheet_data = self.get()


    def get(self, _range: Optional[str] = FULL_SHEET_RANGE) -> SheetData:
        '''Get sheet data.
        
        Args:
            range: range to fetch in A1 notation, if excluded will fetch the whole sheet.

        Return: Nested list of formatted cell data.
        '''
        request =self.sheet_service.values().get(spreadsheetId=self.spreadsheet_id, range=_range)
        response = request.execute()
        assert response and response['values']
        sheet_data = response['values']
        
        if _range == FULL_SHEET_RANGE:
            self.sheet_data = sheet_data
            header_to_col_num, header_row_num = self._find_headers(sheet_data=sheet_data, possible_headers=self.possible_headers)
            self.header_to_col_num = header_to_col_num
            self.header_row_num = header_row_num
        
        return sheet_data


    def smart_get(self, update_sheet_data: bool = True) -> SmartSheetData:
        '''Get sheet data by intelligently finding headers and converting rows to dicts.
        
        Args:
            update_sheet_data: if True refetches sheet data first.

        Return: List of rows as dicts
        '''
        if update_sheet_data:
            self.get()
        
        max_row = len(self.sheet_data)
        result = []
        for i in range(self.header_row_num+1, max_row):
            row_result = {}
            for header, col_num in self.header_to_col_num.items():
                row_data = self.sheet_data[i]
                if col_num >= len(row_data):
                    continue
                cell_data = row_data[col_num]
                if cell_data:
                    row_result[header] = cell_data
            if row_result:
                result.append(row_result)
        return result


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
        request =self.sheet_service.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=_range,
            body=request_body,
            valueInputOption=UPDATE_VALUE_INPUT_OPTION,
        )
        request.execute()


    def smart_update(
            self,
            _values: SmartSheetData,
            primary_keys: List[str],
            upsert: bool = True,
            sort_key: Optional[str] = None,
            sort_asc: bool = True
        ) -> None:
        '''Update sheet data by intelligently finding rows in existing data using the primary key header.
        
        Args:
            _values: list of row changes to make, including primary_key + all cells to update.
            primary_keys: headers with primary key to find row to update by.
            upsert: if true will append rows it does not find in the current sheet data.
            sort_key, sort_asc: see smart_sort()

        Raises:
            - ValueError:
                - If a row in _values does not specify a value for any of primary_keys fields.
                - If a row in _values cannot be found in the sheet by its specified primary_keys and upsert = false.
                - If none of the specified columns to be updated by a row in _values are found in the sheet.
        '''
        def row_to_primary_key_values(row: Dict) -> Tuple:
            '''Helper to reliably and consistently create row primary key hashs.

            Args:
                row: complete row data
            
            Return: hash of row values in primary key fields.
            '''
            return tuple(row[key] if key in row else None for key in primary_keys)


        current_sheet_data_smart = self.smart_get()
        
        # Build lookup map from primary key values to row num.
        primary_keys_to_row_num = {}
        for row_num, current_row in enumerate(current_sheet_data_smart):
            # If row sets any of the primary key rows, add it to the map.
            if any(key in current_row for key in primary_keys):
                map_key = row_to_primary_key_values(current_row)
                primary_keys_to_row_num[map_key] = row_num + self.header_row_num + 1  # Plus 1 for actual header row.

        # glog.info(f'built primary_keys_to_row_num (header_row_num: {self.header_row_num}): {json.dumps(primary_keys_to_row_num)}')
        
        update_requests = []
        rows_to_append = []
        for i, updated_row in enumerate(_values):
            if not any(key in updated_row for key in primary_keys):
                raise ValueError(f'Update row {i} did not specify value for any primary_key fields {primary_keys}: {json.dumps(updated_row)}')
            primary_key_values = row_to_primary_key_values(updated_row)
            
            if primary_key_values not in primary_keys_to_row_num:
                if upsert:
                    rows_to_append.append(updated_row)
                    continue
                else:
                    raise ValueError(f'Could not find primary key values {primary_key_values} for updated row {i} in sheet: primary_key_values: {primary_keys_to_row_num.keys()}')
            row_num = primary_keys_to_row_num[primary_key_values]

            any_update_col_found = False
            for header, updated_cell in updated_row.items():
                if header in primary_keys:
                    continue

                if header not in self.header_to_col_num:
                    continue
                any_update_col_found = True
                col_num = self.header_to_col_num[header]
                
                update = {
                    'range': self.row_col_num_to_A1(row_num, col_num),
                    'values': [[updated_cell]]
                }
                update_requests.append(update)
            
            if not any_update_col_found:
                raise ValueError(f'Could not any specified update headers in sheet for row {i}: found headers: {self.header_to_col_num.keys()}, trying to update: {json.dumps(updated_row)}')    
        
        request_body = {
            'data': update_requests,
            'valueInputOption': UPDATE_VALUE_INPUT_OPTION,
        }
        request =self.sheet_service.values().batchUpdate(spreadsheetId=self.spreadsheet_id, body=request_body)
        request.execute()

        # Append rows that weren't found, if specified.
        if upsert:
            self.smart_append(_values=rows_to_append)

        # Resort if desired.
        if sort_key:
           self.smart_sort(sort_key=sort_key, asc=sort_asc)


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
        request =self.sheet_service.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=_range,
            body=request_body,
            valueInputOption=UPDATE_VALUE_INPUT_OPTION,
        )
        request.execute()


    def smart_append(
            self,
            _values: SmartSheetData,
            sort_key: Optional[str] = None,
            sort_asc: bool = True
        ) -> None:
        '''Append rows to sheet, inteligently matching existing table structure and optionally resorting after insert.
        
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
            sort_key, smart_asc: see smart_sort()
        '''
        
        # Create cell values to append.
        min_col_num = self.header_to_col_num[next(iter(self.header_to_col_num))]
        max_col_num = self.header_to_col_num[next(reversed(self.header_to_col_num))]
        append_width = max_col_num - min_col_num
        append_data = []
        for i, row_to_append in enumerate(_values):
            row_data = [''] * (max_col_num + 1)  # Plus 1 b/c cols are 0-indexed but array length is not.
            populated_cells = 0
            for header, col_num in self.header_to_col_num.items():
                cell_data = row_to_append.get(header)
                if cell_data:
                    row_data[min_col_num + col_num] = cell_data
                    populated_cells += 1
            
            # Verify at least some overlap between found headers and row to append.
            if populated_cells == 0:
                headers = self.header_to_col_num.keys()
                raise ValueError(f'Did not find of sheet\'s headers ({headers}) populated in {i} row to append: {json.dumps(row_to_append)}')
            
            append_data.append(row_data)

        # Append data.
        table_tl = f'{chr(ord("A") + min_col_num)}{self.header_row_num + 1}'
        self.append(_range=table_tl, _values=append_data)

        # Re-sort, if desired.
        if sort_key:
           self.smart_sort(sort_key=sort_key, asc=sort_asc)


    def sort(self, col_num: int, grid_range: GridRange, asc: bool = True) -> None:
        '''Sort specified range by specified column.

        - Will ignore blank cells included in range.
        
        Args:
            col_num: 0-based column number to sort by.
            grid_range: GridRange object to sort, including header row.
            asc: sort ascending? Otherwise desc.
        '''

        sort_order = 'ASCENDING' if asc else 'DESCENDING'
        sort_request = {
            'setBasicFilter': {
                'filter': {
                    'sortSpecs': {
                        'dimensionIndex': col_num,
                        'sortOrder': sort_order,
                    },
                    'range': grid_range.to_google_GridRange(0)
                }
            }
        }
        request_body = {'requests': [sort_request]}
        request =self.sheet_service.batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=request_body
        )
        request.execute()


    def smart_sort(self, sort_key: str, asc: bool = True) -> None:
        '''Sort sheet by a given header.''' 
        if sort_key not in self.header_to_col_num:
            raise ValueError(f'Did not find column to sort by ({sort_key}) in headers: {self.header_to_col_num.keys()}')
        sort_col_num = self.header_to_col_num[sort_key]
        sort_range = GridRange(
            min_row=self.header_row_num,
            max_row=self.MAX_ROWS,
            min_col=0,
            max_col=self.MAX_COLS
        )
        self.sort(col_num=sort_col_num, grid_range=sort_range, asc=asc)

    
    def _find_headers(self, sheet_data: SheetData, possible_headers: List[str]) -> Optional[HeaderData]:
        '''Helper for finding headers in sheet data.
        
        Args:
            sheet_data: complete sheet data to search for headers.
            possible_headers: list of possible (sanitized) headers to look for.

        Return: HeaderData, if found.

        Raises:
            ValueError: if a header is found twice.
        '''
        
        def _sanitize_header(input_header: str) -> str:
            '''helper for sanitizing header value for fair match comparsion.'''
            return input_header.lower().strip().replace(' ', '_')

        def _is_headers_match(header_1: str, header_2: str) -> bool:
            '''Helper for determining if two headers are considered a match.'''
            return _sanitize_header(header_1) == _sanitize_header(header_2)
        
        header_to_col_num = OrderedDict()
        header_row_num = None
        for i, existing_row in enumerate(sheet_data):
            is_header_row = False
            for j, existing_cell in enumerate(existing_row):
                    # Finding just one header match is enough to ID this as the header row.
                    if any(_is_headers_match(possible_header, existing_cell) for possible_header in possible_headers):
                        is_header_row = True
                        break
            
            if is_header_row:
                header_row_num = i
                
                # Take all headers, even if not found in possible_headers.
                for j, header in enumerate(existing_row):
                    header_to_col_num[_sanitize_header(header)] = j
                
                # If we just found the header row can stop looking thru rows.
                break
                    

            if header_row_num:
                # Since found header row, stop looking through rows.
                break

        if not header_row_num:
            raise ValueError(f'Could not find any of these provided headers ({possible_headers}) in existing sheet: {json.dumps(sheet_data)}')
        
        return HeaderData(header_to_col_num=header_to_col_num, header_row_num=header_row_num)


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

