'''Simple linear model based on easily scrapable UnitListing parameters.'''

from typing import List

from housing.data.unit_listing import UnitListing
from housing.models.model import Model, ScoreReturn
from housing import utils

class SimpleModel(Model):

    # Scoring coefficients.
    MAX_SCORED_PRICE: int = 0
    MIN_SCORED_PRICE: int = 0
    MAX_PRICE_SCORE: int = 0
    MIN_PRICE_SCORE: int = 0
    SCORE_PER_SQFT: float = 0
    SCORE_BY_BEDROOM: List[int] = []
    SCORE_PER_BATHROOM: int = 0
    PETS_ALLOWED_SCORE: int = 0
    PARKING_AVAILABLE_SCORE: int = 0


    def __init__(self, model_config_filepath: str):
        self.model_config = utils.load_yaml(model_config_filepath)

        for field, value in self.model_config.items():
            setattr(self, field, value)

    def score(self, unit_listing: UnitListing) -> ScoreReturn:
        # Score tracked in dict for better traceability.
        score_components = {}
        
        # Price.
        saturated_price = max(min(self.MAX_SCORED_PRICE, unit_listing.current_price), self.MIN_SCORED_PRICE)
        price_score_fraction = (saturated_price - self.MIN_PRICE_SCORE) / (self.MAX_SCORED_PRICE - self.MIN_SCORED_PRICE)
        price_score = (price_score_fraction * (self.MAX_PRICE_SCORE - self.MIN_PRICE_SCORE)) + self.MIN_PRICE_SCORE
        score_components['price'] = price_score

        # Sqft.
        sqft_score = 0
        if unit_listing.sqft:
            sqft_score = unit_listing.sqft * self.SCORE_PER_SQFT
        score_components['sqft'] = sqft_score

        # Bedrooms.
        bedroom_score = 0
        for i in range(unit_listing.bedrooms):
            assert len(self.SCORE_BY_BEDROOM) > i, f'Do not have scoring set for bedroom #{i}'
            bedroom_score += self.SCORE_BY_BEDROOM[i]
        score_components['bedrooms'] = bedroom_score

        # Bathrooms.
        bathrooms_score = unit_listing.bathrooms * self.SCORE_PER_BATHROOM
        score_components['bathrooms'] = bathrooms_score

        # Pets.
        pets_score = 0
        if unit_listing.pets_allowed is not None:
            pets_score = int(unit_listing.pets_allowed) * self.PETS_ALLOWED_SCORE
        score_components['pets'] = pets_score

        # Parking.
        parking_score = 0
        if unit_listing.parking_available is not None:
            parking_score = int(unit_listing.parking_available) * self.PARKING_AVAILABLE_SCORE
        score_components['parking'] = parking_score

        score = sum(score_components.values())
        return ScoreReturn(score=score, score_components=score_components)
