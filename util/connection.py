import os

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import orm


AIRFLOW_CONN_MYSQL_TRACKER = "mysql+pymysql://mri:mri@lab02186:13306/mri"


# TODO: use AIRFLOW_CONN_MYSQL_TRACKER instead of hard-coded DB path
# AIRFLOW_CONN_MYSQL_TRACKER = os.environ.get('AIRFLOW_CONN_MYSQL_TRACKER')

# if not AIRFLOW_CONN_MYSQL_TRACKER:
#    raise ValueError("AIRFLOW_CONN_MYSQL_TRACKER not present in the environment")


Base = automap_base()
engine = create_engine(AIRFLOW_CONN_MYSQL_TRACKER)
Base.prepare(engine, reflect=True)

Participant = Base.classes.participant
Scan = Base.classes.scan
Dicom = Base.classes.dicom
Session = Base.classes.session
Sequence = Base.classes.sequence
Repetition = Base.classes.repetition

db_session = orm.Session(engine)
