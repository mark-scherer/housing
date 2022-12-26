'''Interface for accessing models.'''

from housing.data.unit_listing import UnitListing
from housing.models.model import ScoreReturn
from housing.models.simple_model import SimpleModel

ACTIVE_MODEL_TYPE = SimpleModel
ACTIVE_MODEL_CONFIG = '/Users/mark/Documents/housing/models/model_configs/simple_model_v1.yaml'
ACTIVE_MODEL = ACTIVE_MODEL_TYPE(ACTIVE_MODEL_CONFIG)

def score(unit_listing: UnitListing) -> ScoreReturn:
    '''Score a single unit_listing from the active model.'''
    return ACTIVE_MODEL.score(unit_listing)