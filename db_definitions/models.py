import os
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, func
from sqlalchemy.dialects.postgresql import UUID, TEXT
from sqlalchemy.orm import declarative_base

Base = declarative_base()

## REF: https://kitt.lewagon.com/camps/1769/challenges?path=02-Database-Fundamentals%2F04-Backend-and-Database-Management%2F01-Twitter-CRUD

class DataFiles(Base):
    """Class to represent the data files table"""

    # Table name
    # __tablename__ =
    __tablename__ = os.environ.get("DATA_FILES_TABLE", "datafiles")

    # Columns
    id = Column(Integer, primary_key=True)
    time_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    data_file_name = Column(String, nullable=False)
    upload_time_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
