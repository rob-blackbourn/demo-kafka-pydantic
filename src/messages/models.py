from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import (
    BaseModel,
    PlainSerializer,
    PlainValidator
)

from .serialization import serialize_model, validate_model


class User(BaseModel):
    name: str
    date_of_birth: datetime
    height: float


class Location(BaseModel):
    name: str
    latitude: Decimal
    longitude: Decimal


class Address(BaseModel):
    street: str
    city: str


class Update[T: BaseModel](BaseModel):
    model: Annotated[
        T,
        PlainSerializer(serialize_model),
        PlainValidator(validate_model)
    ]
