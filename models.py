from sqlalchemy import Column, Integer, String, DateTime, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4
Base = declarative_base()


class StoreStatus(Base):
    __tablename__ = "store_status"

    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    status = Column(String)
    timestamp_utc = Column(DateTime)

class BusinessHours(Base):
    __tablename__ = 'business_hours'
    
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    day = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class StoreTimezone(Base):
    __tablename__ = 'store_timezone'
    
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    timezone_str = Column(String)

class Report(Base):
    __tablename__= "report"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    status = Column(String)
    file = Column(String)