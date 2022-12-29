'''Abstract model for scoring UnitListings.'''

from typing import NamedTuple, Dict

from housing.data.unit_listing import UnitListing

class ScoreReturn(NamedTuple):
    '''Wrapper around score and metadata.'''
    score: float
    score_components: Dict

class Model:
    '''Abstract class for models to score UnitListings'''

    def score(self, unit_listing: UnitListing) -> ScoreReturn:
        '''Score a single UnitListings.
        
        Instance method: model classes should be general model frameworks while 
        model instances have specific weights.
        '''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')
