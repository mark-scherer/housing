'''Dataclass for storing addresses.'''

from dataclasses import dataclass, asdict
from typing import Optional, Dict
import json
from os import path

import usaddress
import glog


@dataclass
class Address:
    SERIALIZATION_ELEMENT_FIELDS = [
        'unit_num',
        'short_address',
        'city',
        'state',
        'zipcode'
    ]
    SERIALIZATION_DELIMITER = '~'  # Cannot be dash or underscore as these are used in actual addresses
    GOOGLE_MAPS_BASE_URL = 'https://www.google.com/maps/place/'
    MISSING_ELEMENT_ERROR_REGEX = 'address string missing required element'

    short_address: str
    city: str
    state: str
    zipcode: str
    unit_num: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_string(self) -> str:
        element_values = [getattr(self, field) for field in self.SERIALIZATION_ELEMENT_FIELDS]
        element_values = [value for value in element_values if value is not None]
        return self.SERIALIZATION_DELIMITER.join(element_values).lower()

    def to_display_string(self) -> str:
        return f'#{self.unit_num} {self.short_address}, {self.zipcode}'

    def to_google_maps_url(self) -> str:
        encoded_address_elements = [self.short_address, self.city, self.state, self.zipcode]
        encoded_address = '+'.join(encoded_address_elements)
        return path.join(self.GOOGLE_MAPS_BASE_URL, encoded_address)

    @classmethod
    def from_string(cls, input: str) -> 'Address':
        serialized_elements = input.split(cls.SERIALIZATION_DELIMITER)
        
        # Sometimes unit num includes a dash.
        if len(serialized_elements) == len(cls.SERIALIZATION_ELEMENT_FIELDS) + 1:
            unit_num = '-'.join(serialized_elements[0:2])
            serialized_elements = [unit_num] + serialized_elements[2:]

        # Unit num might be missing.
        if len(serialized_elements) == len(cls.SERIALIZATION_ELEMENT_FIELDS):
            serialized_fields = cls.SERIALIZATION_ELEMENT_FIELDS
        elif len(serialized_elements) == len(cls.SERIALIZATION_ELEMENT_FIELDS) - 1:
            serialized_fields = cls.SERIALIZATION_ELEMENT_FIELDS[1:]
        else:
            raise ValueError(f'Parsed an unrecognized number of serialized address elements ({len(serialized_elements)}, expecting {len(cls.SERIALIZATION_ELEMENT_FIELDS)}): ' \
                f'{input}')

        element_values = {field: value for field, value in zip(serialized_fields, serialized_elements)}
        result = Address(**element_values)
        return result

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