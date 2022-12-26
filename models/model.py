'''Abstract model for scoring UnitListings.'''

from typing import NamedTuple, Dict

from housing.data.unit_listing import UnitListing

class ScoreReturn(NamedTuple):
    '''Wrapper around score and metadata.'''
    score: float
    score_components: Dict

class Model:
    '''Abstract class for models to score UnitListings'''

    TRUE_STRINGS = ['true', 't']
    FALSE_STRINGS = ['false', 'f']

    def score(self, unit_listing: UnitListing) -> ScoreReturn:
        '''Score a single UnitListings.
        
        Instance method: model classes should be general model frameworks while 
        model instances have specific weights.
        '''
        raise NotImplementedError(f'must be overridden in {cls.__name__}')

    @classmethod
    def _strToBool(cls, input: str) -> bool:
        '''Helper for parsing stringifed postgres bools back into actual bools'''
        result = None
        if input:
            if input.lower() in cls.TRUE_STRINGS:
                result = True
            elif input.lower() in cls.FALSE_STRINGS:
                result = False
            else:
                raise ValueError(f'Could not parse bool from string: {input}')

        return result