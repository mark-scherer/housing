'''Class for configs.'''

from os import path
from typing import NamedTuple, FrozenSet, Dict, Optional, List
import re

import glog
import yaml

DEFAULT_MAX_RESULTS = 10

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
    scrapers: List[str]  # scrapers to use, not actually respected yet.
    max_results: int = DEFAULT_MAX_RESULTS

    @classmethod
    def from_dict(cls, data: Dict) -> 'ScrapingParams':
        '''Create new ScrapingParams object from Dict.
        Corrects typing.
        '''
        data['zipcodes'] = frozenset([str(zipcode) for zipcode in data['zipcodes']])
        return ScrapingParams(**data)


class Config(NamedTuple):
    '''Housing config.'''

    name: str  # This is parsed from filename.
    scraping_params: ScrapingParams
    sheet_id: str  # Google sheet id

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
        
        # Validate passed filepath
        filepath_without_ext, ext = path.splitext(filepath)
        assert re.search('^\.y(a)?ml', ext) is not None, \
            f'Filepath must be yaml, found: {ext}: {filepath}'
        assert path.exists(filepath), f'File not found: {filepath}'

        # Load yaml and parse.
        config = None
        with open(filepath, 'r') as file:
            try:
                config_data = yaml.safe_load(file)
                assert 'config' in config_data, 'Loaded data does not have highest-level config field'
                
                name = path.basename(filepath_without_ext)
                config = cls.from_dict(data=config_data['config'], name=name)

            except Exception as e:
                exception_type = type(e)
                raise exception_type(f'Error parsing yaml at {filepath}: {e}') from e

        return config