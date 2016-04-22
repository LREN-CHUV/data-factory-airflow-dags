########################################################################################################################
# IMPORTS
########################################################################################################################

import glob
import traceback
import sys
import dicom
import datetime

import connection
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

def dicom2db(folder):

    db_session = connection.db_session

    for filename in glob.iglob(folder+'/*/MR.*'):
        ds = dicom.read_file(filename)
        try:
            participant_id = extract_participant(ds, db_session, DEFAULT_HANDEDNESS)
            scan_id = extract_scan(ds, db_session, participant_id, DEFAULT_ROLE, DEFAULT_COMMENT)
            session_id = extract_session(ds, db_session, scan_id)
            sequence_id = extract_sequence(ds, db_session, session_id)
            repetition_id = extract_repetition(ds, db_session, sequence_id)
            extract_dicom(db_session, filename, repetition_id)
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


def extract_participant(ds, db_session, handedness):

    participant_birth_date = format_date(ds.PatientBirthDate)
    participant_gender = format_gender(ds.PatientSex)
    participant = connection.Participant(
        gender=participant_gender,
        handedness=handedness,
        birthdate=participant_birth_date
    )
    db_session.add(participant)
    db_session.commit()

    return participant.id


def extract_scan(ds, db_session, participant_id, role, comment):

    scan_date = format_date(ds.StudyDate)
    scan = connection.Scan(
        date=scan_date,
        role=role,
        comment=comment,
        participant_id=participant_id
    )
    db_session.add(scan)
    db_session.commit()

    return scan.id


def extract_session(ds, db_session, scan_id):

    session_value = int(ds.StudyID)
    session = connection.Session(
        scan_id=scan_id,
        value=session_value
    )
    db_session.add(session)
    db_session.commit()

    return session.id


def extract_sequence(ds, db_session, session_id):

    sequence_name = ds.ProtocolName
    sequence = connection.Sequence(
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


def extract_dicom(db_session, path, repetition_id):

    dicom = connection.Dicom(
        path=path,
        repetition_id=repetition_id
    )
    db_session.add(dicom)
    db_session.commit()

    return dicom.id
