'''Dataclass for storing addresses.'''

from dataclasses import dataclass, asdict
from typing import Optional, Dict

import usaddress


@dataclass
class Address:
    short_address: str
    city: str
    state: str
    zipcode: str
    unit_num: Optional[str] = None

    def id(self) -> str:
        '''Unique ID for Addrress.'''
        id_elements = [
            self.unit_num,
            self.short_address,
            self.zipcode,
        ]
        id = '-'.join([element for element in id_elements if element is not None])
        id.replace(' ', '-').lower()
        return id


    def to_dict(self) -> Dict:
        return asdict(self)

    @staticmethod
    def from_full_address(full_address: str) -> 'Address':
        '''Parse Address from full address string.'''

        def _validate_parsed_address_info(address_info: Dict, full_address: str) -> None:
            '''Validate parsed usaddress included all needed info.'''
            REQUIRED_FIELDS = ['AddressNumber', 'StreetName', 'StreetNamePostType', 'PlaceName', 'StateName', 'ZipCode']
            for field in REQUIRED_FIELDS:
                assert field in address_info, f'address string missing required element ({field}): {full_address} {address_info}'


        address_info, _ = usaddress.tag(full_address)
        _validate_parsed_address_info(address_info, full_address)
        
        short_address_elements = [
            address_info.get('AddressNumber'),
            address_info.get('StreetNamePreDirectional'),
            address_info.get('StreetName'),
            address_info.get('StreetNamePostType')
        ]
        short_address_elements = [element for element in short_address_elements if element is not None]
        short_address = ' '.join(short_address_elements)

        unit_num = address_info.get('OccupancyIdentifier')

        return Address(
            short_address=short_address,
            city=address_info['PlaceName'],
            state=address_info['StateName'],
            zipcode=address_info['ZipCode'],
            unit_num=unit_num
        )