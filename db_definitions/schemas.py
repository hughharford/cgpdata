from pydantic import BaseModel, ConfigDict
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID

## REF: https://kitt.lewagon.com/camps/1769/challenges?path=02-Database-Fundamentals%2F04-Backend-and-Database-Management%2F01-Twitter-CRUD


# data files section
class DataFilesBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_file_name: str | None = None

class DataFilesCreate(DataFilesBase):
    year: int | None = False
    month: int | None = False

class DataFiles(DataFilesBase):
    time_date_logged: datetime
    id: UUID
    class Config:
        from_attributes = True
