
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from models import *


class StoreStatusSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = StoreStatus
        load_instance = True

class BusinessHoursSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = BusinessHours
        load_instance = True