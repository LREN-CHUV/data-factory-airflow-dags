import os

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import orm

from airflow import configuration

Base = automap_base()
engine = create_engine(configuration.get('mri', 'SQL_ALCHEMY_CONN'))
Base.prepare(engine, reflect=True)

Participant = Base.classes.participant
Scan = Base.classes.scan
Dicom = Base.classes.dicom
Session = Base.classes.session
Sequence = Base.classes.sequence
Repetition = Base.classes.repetition

db_session = orm.Session(engine)
