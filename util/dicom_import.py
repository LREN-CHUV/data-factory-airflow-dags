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
    session = db["session"]
    participant_class = db["participant_class"]
    scan_class = db["scan_class"]
    image_class = db["image_class"]

    for filename in glob.iglob(folder+'/*/*'):
        ds = dicom.read_file(filename)
        try:
            participant_id = extract_participant(participant_class, ds, session, DEFAULT_HANDEDNESS)
            scan_id = extract_scan(scan_class, ds, session, participant_id, DEFAULT_ROLE, DEFAULT_COMMENT)
            extract_image(image_class, ds, session, scan_id, filename)
        except (err.IntegrityError, exc.IntegrityError):
            traceback.print_exc(file=sys.stdout)
            session.rollback()


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
    image_class = base_class.classes.image

    return {
        "session": Session(engine),
        "participant_class": participant_class,
        "scan_class": scan_class,
        "image_class": image_class
    }


def extract_participant(participant_class, ds, session, handedness):

    participant_birth_date = format_date(ds.PatientBirthDate)
    participant_gender = format_gender(ds.PatientSex)
    participant = participant_class(
        gender=participant_gender,
        handedness=handedness,
        birthdate=participant_birth_date
    )
    session.add(participant)
    session.commit()

    return participant.id


def extract_scan(scan_class, ds, session, participant_id, role, comment):

    scan_date = format_date(ds.StudyDate)
    scan = scan_class(
        date=scan_date,
        role=role,
        comment=comment,
        participant_id=participant_id
    )
    session.add(scan)
    session.commit()

    return scan.id


def extract_image(image_class, ds, session, scan_id, uri):

    image_sequence = ds.ProtocolName
    image_session = int(ds.SeriesNumber)
    image_repetition = int(ds.InstanceNumber)
    image = image_class(
        uri=uri,
        session=image_session,
        sequence=image_sequence,
        repetition=image_repetition,
        scan_id=scan_id
    )
    session.add(image)
    session.commit()

    return image.id
