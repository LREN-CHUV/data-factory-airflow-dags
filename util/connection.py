import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.pool import NullPool

AIRFLOW_CONN_MYSQL_TRACKER = os.environ.get('AIRFLOW_CONN_MYSQL_TRACKER')

if not AIRFLOW_CONN_MYSQL_TRACKER:
    raise ValueError("AIRFLOW_CONN_MYSQL_TRACKER not present in the environment")

Base = automap_base()
engine = create_engine(AIRFLOW_CONN_MYSQL_TRACKER, poolclass=NullPool)
Base.prepare(engine, reflect=True)

session_factory = sessionmaker(bind=engine, expire_on_commit=False)
Session = scoped_session(session_factory)
