from sqlalchemy.orm import Session
import os
from datetime import datetime
import uuid
import pytz

import sys
sys.path.insert(0, '/home/hugh.harford/code/hughharford/cgpdata')


from dbdefs import models, schemas
from dbdefs.database import SessionLocal


def get_db():
    """Helper function which opens a connection to the
    database and also manages closing the connection"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

####### data files section #######
# for api only..
def read_data_files(db: Session, skip: int = 0, limit: int = 100):
    """Function should return all data files listed with a skip and limit param"""
    return db.query(models.DataFiles).offset(skip).limit(limit).all()

def create_data_file_record(db: Session, datafile_about: dict, layer: str=None):
    '''
    Records datafile record in cgpbackbone db
    In table "datafiles".
    '''

    if not layer:
        raise ValueError("must pass layer into read_data_files")
    else:
        db_datafile_record = schemas.DataFilesCreate()

        db_datafile_record.data_file_name = datafile_about["datafilename"]
        db_datafile_record.upload_time_date = datafile_about["uploadtime"]
        db_datafile_record.year = int(datafile_about["year"])
        db_datafile_record.month = int(datafile_about["month"])
        db_datafile_record.blob_size = int(datafile_about["blob_size"])

        db_datafile_record.layer_name = layer

        db_ingoing = models.DataFiles(**db_datafile_record.model_dump())
        db_ingoing.id = uuid.uuid4()
        db.add(db_ingoing)
        db.commit()
        db.refresh(db_ingoing)

    return db_ingoing.id

if __name__ == "__main__":
    db_gen = get_db()
    db = next(db_gen)
    files_found = read_data_files(db)
    checker = []
    for f in files_found:
        checker.append(f.data_file_name)
        print(f.data_file_name)
