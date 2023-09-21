from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from env import DB_URL

engine = create_engine(DB_URL)
session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

SESSION = session()
