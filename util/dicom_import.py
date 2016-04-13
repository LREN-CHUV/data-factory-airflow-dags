########################################################################################################################
# IMPORTS
########################################################################################################################

import glob
import traceback
import sys
import dicom
import datetime

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import exc

from pymysql import err


########################################################################################################################
# CONSTANTS
########################################################################################################################

DEFAULT_HANDEDNESS = 'unknown'
DEFAULT_ROLE = 'U'
DEFAULT_COMMENT = ''


########################################################################################################################
# FUNCTIONS - DICOM
########################################################################################################################

def dicom2db(folder, db_path):

    db = init_db(db_path)
    db_session = db["session"]
    participant_class = db["participant_class"]
    scan_class = db["scan_class"]
    dicom_class = db["dicom_class"]
    session_class = db["session_class"]
    sequence_class = db["sequence_class"]
    repetition_class = db["repetition_class"]

    for filename in glob.iglob(folder+'/*/*'):
        ds = dicom.read_file(filename)
        try:
            participant_id = extract_participant(participant_class, ds, db_session, DEFAULT_HANDEDNESS)
            scan_id = extract_scan(scan_class, ds, db_session, participant_id, DEFAULT_ROLE, DEFAULT_COMMENT)
            session_id = extract_session(session_class, ds, db_session, scan_id)
            sequence_id = extract_sequence(sequence_class, ds, db_session, session_id)
            repetition_id = extract_repetition(repetition_class, ds, db_session, sequence_id)
            extract_dicom(dicom_class, ds, db_session, filename, repetition_id)
        except (err.IntegrityError, exc.IntegrityError):
            traceback.print_exc(file=sys.stdout)
            db_session.rollback()


########################################################################################################################
# FUNCTIONS - UTILS
########################################################################################################################

def format_date(date):
    return datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]))


def format_gender(gender):
    if gender == 'M':
        return 'male'
    elif gender == 'F':
        return 'female'
    else:
        return 'unknown'


########################################################################################################################
# FUNCTIONS - DATABASE
########################################################################################################################

def init_db(db):

    base_class = automap_base()
    engine = create_engine(db)
    base_class.prepare(engine, reflect=True)

    participant_class = base_class.classes.participant
    scan_class = base_class.classes.scan
    dicom_class = base_class.classes.dicom
    session_class = base_class.classes.session
    sequence_class = base_class.classes.sequence
    repetition_class = base_class.classes.repetition

    return {
        "session": Session(engine),
        "participant_class": participant_class,
        "scan_class": scan_class,
        "dicom_class": dicom_class,
        "session_class": session_class,
        "sequence_class": sequence_class,
        "repetition_class": repetition_class
    }


def extract_participant(participant_class, ds, db_session, handedness):

    participant_birth_date = format_date(ds.PatientBirthDate)
    participant_gender = format_gender(ds.PatientSex)
    participant = participant_class(
        gender=participant_gender,
        handedness=handedness,
        birthdate=participant_birth_date
    )
    db_session.add(participant)
    db_session.commit()

    return participant.id


def extract_scan(scan_class, ds, db_session, participant_id, role, comment):

    scan_date = format_date(ds.StudyDate)
    scan = scan_class(
        date=scan_date,
        role=role,
        comment=comment,
        participant_id=participant_id
    )
    db_session.add(scan)
    db_session.commit()

    return scan.id


def extract_session(session_class, ds, db_session, scan_id):

    session_value = int(ds.StudyID)
    session = session_class(
        scan_id=scan_id,
        value=session_value
    )
    db_session.add(session)
    db_session.commit()

    return session.id


def extract_sequence(sequence_class, ds, db_session, session_id):

    sequence_name = ds.ProtocolName
    sequence = sequence_class(
        session_id=session_id,
        name=sequence_name
    )
    db_session.add(sequence)
    db_session.commit()

    return sequence.id


def extract_repetition(repetition_class, ds, db_session, sequence_id):

    repetition_value = int(ds.SeriesNumber)
    repetition = repetition_class(
        sequence_id=sequence_id,
        value=repetition_value
    )
    db_session.add(repetition)
    db_session.commit()

    return repetition.id


def extract_dicom(dicom_class, ds, db_session, path, repetition_id):

    dicom = dicom_class(
        path=path,
        repetition_id=repetition_id
    )
    db_session.add(dicom)
    db_session.commit()

    return dicom.id
