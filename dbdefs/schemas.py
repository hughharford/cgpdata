from pydantic import BaseModel, ConfigDict
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID


# data files section
class DataFilesBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_file_name: str | None = None
    upload_time_date: datetime | None = None

class DataFilesCreate(DataFilesBase):
    year: int | None = False
    month: int | None = False

class DataFiles(DataFilesBase):
    time_date: datetime
    id: UUID
    class Config:
        from_attributes = True
