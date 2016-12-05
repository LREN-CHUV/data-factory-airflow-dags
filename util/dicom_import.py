########################################################################################################################
# IMPORTS
########################################################################################################################

import os
import fnmatch
import dicom
import datetime
import logging

from . import connection
from sqlalchemy.exc import IntegrityError
from dicom.errors import InvalidDicomError


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

    for root, _, filenames in os.walk(folder):
        for f in fnmatch.filter(filenames, 'MR.*'):
            try:
                filename = root+"/"+f
                logging.info("Processing '%s'" % filename)
                ds = dicom.read_file(filename)

                participant_id = extract_participant(ds, DEFAULT_HANDEDNESS)
                scan_id = extract_scan(ds, participant_id, DEFAULT_ROLE, DEFAULT_COMMENT)
                session_id = extract_session(ds, scan_id)
                sequence_type_id = extract_sequence_type(ds)
                sequence_id = extract_sequence(session_id, sequence_type_id)
                repetition_id = extract_repetition(ds, sequence_id)
                extract_dicom(filename, repetition_id)

            except InvalidDicomError:
                logging.warning("%s is not a DICOM file !" % filename)

            except IntegrityError:
                print_db_except()
                connection.db_session.rollback()


def visit_info(folder):
    for root, _, filenames in os.walk(folder):
        for f in fnmatch.filter(filenames, 'MR.*'):
            filename = None
            try:
                filename = root+"/"+f
                logging.info("Processing '%s'" % filename)
                ds = dicom.read_file(filename)

                participant_id = ds.PatientID
                scan_date = format_date(ds.StudyDate)

                return participant_id, scan_date

            except InvalidDicomError:
                logging.warning("%s is not a DICOM file !" % filename)

            except IntegrityError:
                print_db_except()
                connection.db_session.rollback()


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


def print_db_except():
    logging.warning("A problem occurred while trying to insert data into DB.")
    logging.warning("This can be caused by a duplicate entry on a unique field.")
    logging.warning("A rollback will be performed !")


########################################################################################################################
# FUNCTIONS - DATABASE
########################################################################################################################


def extract_participant(ds, handedness):

    participant_id = ds.PatientID

    participant_birth_date = format_date(ds.PatientBirthDate)
    participant_gender = format_gender(ds.PatientSex)

    if participant_id is None:
        logging.warning("participant cannot be saved because no ID was found !")
        return None
    else:
        participant = connection.db_session.query(connection.Participant).filter_by(id=participant_id).first()

        if not participant:
            participant = connection.Participant(
                id=participant_id,
                gender=participant_gender,
                handedness=handedness,
                birthdate=participant_birth_date
            )
            connection.db_session.add(participant)
            connection.db_session.commit()

    return participant.id


def extract_scan(ds, participant_id, role, comment):

    scan_date = format_date(ds.StudyDate)

    scan = connection.db_session.query(connection.Scan).filter_by(participant_id=participant_id, date=scan_date).first()

    if not scan:
        scan = connection.Scan(
            date=scan_date,
            role=role,
            comment=comment,
            participant_id=participant_id
        )
        connection.db_session.add(scan)
        connection.db_session.commit()

    return scan.id


def extract_session(ds, scan_id):

    session_value = int(ds.StudyID)

    session = connection.db_session.query(connection.Session).filter_by(scan_id=scan_id, value=session_value).first()

    if not session:
        session = connection.Session(
            scan_id=scan_id,
            value=session_value
        )
        connection.db_session.add(session)
        connection.db_session.commit()

    return session.id


def extract_sequence_type(ds):

    sequence_name = ds.ProtocolName
    manufacturer = ds.Manufacturer
    manufacturer_model_name = ds.ManufacturerModelName
    institution_name = ds.InstitutionName
    slice_thickness = float(ds.SliceThickness)
    repetition_time = float(ds.RepetitionTime)
    echo_time = float(ds.EchoTime)
    number_of_phase_encoding_steps = int(ds.NumberOfPhaseEncodingSteps)
    percent_phase_field_of_view = float(ds.PercentPhaseFieldOfView)
    pixel_bandwidth = int(ds.PixelBandwidth)
    flip_angle = float(ds.FlipAngle)
    rows = int(ds.Rows)
    columns = int(ds.Columns)
    magnetic_field_strength = float(ds.MagneticFieldStrength)
    echo_train_length = int(ds.EchoTrainLength)
    percent_sampling = float(ds.PercentSampling)
    pixel_spacing = ds.PixelSpacing
    pixel_spacing_0 = float(pixel_spacing[0])
    pixel_spacing_1 = float(pixel_spacing[1])

    try:
        echo_number = int(ds.EchoNumber)
    except AttributeError:
        echo_number = int(0)
    try:
        space_between_slices = float(ds[0x0018, 0x0088].value)
    except KeyError:
        space_between_slices = float(0.0)

    sequence_type_list = connection.db_session.query(connection.SequenceType).filter_by(
        name=sequence_name,
        manufacturer=manufacturer,
        manufacturer_model_name=manufacturer_model_name,
        institution_name=institution_name,
        echo_number=echo_number,
        number_of_phase_encoding_steps=number_of_phase_encoding_steps,
        pixel_bandwidth=pixel_bandwidth,
        rows=rows,
        columns=columns,
        echo_train_length=echo_train_length
    ).all()

    for s in sequence_type_list:
        if str(s.slice_thickness) != str(slice_thickness) \
                or str(s.repetition_time) != str(repetition_time)\
                or str(s.echo_time) != str(echo_time)\
                or str(s.percent_phase_field_of_view) != str(percent_phase_field_of_view)\
                or str(s.flip_angle) != str(flip_angle)\
                or str(s.magnetic_field_strength) != str(magnetic_field_strength)\
                or str(s.flip_angle) != str(flip_angle)\
                or str(s.percent_sampling) != str(percent_sampling)\
                or str(s.pixel_spacing_0) != str(pixel_spacing_0)\
                or str(s.pixel_spacing_1) != str(pixel_spacing_1):
            sequence_type_list.remove(s)

    if len(sequence_type_list) > 0:
        sequence_type = sequence_type_list[0]
    else:
        sequence_type = connection.SequenceType(
            name=sequence_name,
            manufacturer=manufacturer,
            manufacturer_model_name=manufacturer_model_name,
            institution_name=institution_name,
            slice_thickness=slice_thickness,
            repetition_time=repetition_time,
            echo_time=echo_time,
            echo_number=echo_number,
            number_of_phase_encoding_steps=number_of_phase_encoding_steps,
            percent_phase_field_of_view=percent_phase_field_of_view,
            pixel_bandwidth=pixel_bandwidth,
            flip_angle=flip_angle,
            rows=rows,
            columns=columns,
            magnetic_field_strength=magnetic_field_strength,
            space_between_slices=space_between_slices,
            echo_train_length=echo_train_length,
            percent_sampling=percent_sampling,
            pixel_spacing_0=pixel_spacing_0,
            pixel_spacing_1=pixel_spacing_1
        )
        connection.db_session.add(sequence_type)
        connection.db_session.commit()

    return sequence_type.id


def extract_sequence(session_id, sequence_type_id):

    sequence = connection.db_session.query(connection.Sequence)\
        .filter_by(session_id=session_id, sequence_type_id=sequence_type_id)\
        .first()

    if not sequence:
        sequence = connection.Sequence(
            session_id=session_id,
            sequence_type_id=sequence_type_id
        )
        connection.db_session.add(sequence)
        connection.db_session.commit()

    return sequence.id


def extract_repetition(ds, sequence_id):

    repetition_value = int(ds.SeriesNumber)

    repetition = connection.db_session.query(connection.Repetition).filter_by(sequence_id=sequence_id, value=repetition_value).first()

    if not repetition:
        repetition = connection.Repetition(
            sequence_id=sequence_id,
            value=repetition_value
        )
        connection.db_session.add(repetition)
        connection.db_session.commit()

    return repetition.id


def extract_dicom(path, repetition_id):

    dcm = connection.db_session.query(connection.Dicom).filter_by(path=path, repetition_id=repetition_id).first()

    if not dcm:
        dcm = connection.Dicom(
            path=path,
            repetition_id=repetition_id
        )
        connection.db_session.add(dcm)
        connection.db_session.commit()

    return dcm.id
