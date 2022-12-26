'''Class for configs.'''

from os import path
from typing import NamedTuple, FrozenSet, Dict, Optional, List

import glog

from housing import utils

# Custom type to handle Studios.
# Studios represented as 0 Bedrooms
BedroomCount = int


class ScrapingParams(NamedTuple):
    '''Params controlling housing scrape.'''

    # Unit params
    min_bedrooms: BedroomCount
    max_bedrooms: BedroomCount

    # Location params
    zipcodes: FrozenSet[str]

    # Listing parms
    min_price: int
    max_price: int

    # Search metadata
    scrapers: FrozenSet[str]  # scrapers to use, not actually respected yet.

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScrapingParams':
        '''Create new ScrapingParams object from Dict.
        Corrects typing.
        '''
        def frozenset_from_list(input: List) -> FrozenSet:
            return frozenset([element for element in input])

        data['zipcodes'] = frozenset_from_list([str(zipcode) for zipcode in data['zipcodes']])
        data['scrapers'] = frozenset_from_list(data['scrapers'])
        return ScrapingParams(**data)

    def to_dict(self) -> Dict:
        return self._asdict()


class Config(NamedTuple):
    '''Housing config.'''

    name: str  # This is parsed from filename.
    scraping_params: ScrapingParams
    spreadsheet_id: str  # Google sheet id

    def to_dict(self) -> Dict:
        '''NamedTuple._asdict() only serializes top-level fields.
        Doesn't properly handle nested NamedTuple fields so need to define manually - 
        and cannot override NamedTuple._asdict() directly.
        '''
        return {
            'name': self.name,
            'scraping_params': self.scraping_params._asdict()
        }

    @classmethod
    def from_dict(cls, data: Dict, name: Optional[str] = None) -> 'Config':
        '''Created Config object from Dict.
        Optionally overrides name field.
        '''

        data['scraping_params'] = ScrapingParams.from_dict(data['scraping_params'])
        data['name'] = name or data['name']
        return Config(**data)

    @classmethod
    def load_from_file(cls, filepath: str) -> 'Config':
        '''Load Config from a yaml file.'''
        config_data = utils.load_yaml(filepath)
        assert 'config' in config_data, 'Loaded data does not have highest-level config field'
        
        filepath_without_ext, ext = path.splitext(filepath)
        name = path.basename(filepath_without_ext)
        config = cls.from_dict(data=config_data['config'], name=name)

        return config