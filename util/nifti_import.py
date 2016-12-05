#!/usr/bin/env python3.5


##########################################################################
# IMPORTS
##########################################################################

import os
import logging
import fnmatch
import re

from . import connection
from datetime import date
from os import path


##########################################################################
# CONSTANTS
##########################################################################

ARGS = ['root_folder', 'csv', 'db']


##########################################################################
# FUNCTIONS - MAIN
##########################################################################

def nifti2db(folder, participant_id, scan_date):

    for root, _, filenames in os.walk(folder):
        for f in fnmatch.filter(filenames, '*.nii'):
            file_path = root + "/" + f
            file_path = path.abspath(file_path)
            logging.debug('processing: %s', file_path)

            try:
                session = int(re.findall(
                    '/([^/]+?)/[^/]+?/[^/]+?/[^/]+?\.nii', file_path)[0])
                sequence = re.findall(
                    '/([^/]+?)/[^/]+?/[^/]+?\.nii', file_path)[0]
                repetition = int(re.findall(
                    '/([^/]+?)/[^/]+?\.nii', file_path)[0])

                if participant_id and scan_date:

                    filename = re.findall('/([^/]+?)\.nii', file_path)[0]

                    try:
                        prefix_type = re.findall('(.*)PR', filename)[0]
                    except IndexError:
                        prefix_type = "unknown"

                    try:
                        postfix_type = re.findall('-\d\d_(.+)', filename)[0]
                    except IndexError:
                        postfix_type = "unknown"

                    save_nifti_meta(
                        participant_id,
                        scan_date,
                        session,
                        sequence,
                        repetition,
                        prefix_type,
                        postfix_type,
                        file_path
                    )

            except ValueError:
                logging.warning(
                    "A problem occurred with '%s' ! Check the path format... " % file_path)

    logging.info('[FINISH]')


##########################################################################
# FUNCTIONS - UTILS
##########################################################################

def date_from_str(date_str):
    day = int(re.findall('(\d+)\.\d+\.\d+', date_str)[0])
    month = int(re.findall('\d+\.(\d+)\.\d+', date_str)[0])
    year = int(re.findall('\d+\.\d+\.(\d+)', date_str)[0])
    return date(year, month, day)


##########################################################################
# FUNCTIONS - DATABASE
##########################################################################

def save_nifti_meta(
        participant_id,
        scan_date,
        session,
        sequence,
        repetition,
        prefix_type,
        postfix_type,
        file_path
):

    if not connection.db_session.query(connection.Nifti).filter_by(path=file_path).first():

        scan = connection.db_session.query(connection.Scan).filter_by(date=scan_date, participant_id=participant_id)\
            .first()
        sequence_type_list = connection.db_session.query(
            connection.SequenceType).filter_by(name=sequence).all()

        if scan and len(sequence_type_list) > 0:
            scan_id = scan.id
            sess = connection.db_session.query(connection.Session).filter_by(
                scan_id=scan_id, value=session).first()

            if sess:
                session_id = sess.id
                if len(sequence_type_list) > 1:
                    logging.warning(
                        "Multiple sequence_type available for %s !" % sequence)
                sequence_type_id = sequence_type_list[0].id
                seq = connection.db_session.query(connection.Sequence).filter_by(
                    session_id=session_id,
                    sequence_type_id=sequence_type_id)\
                    .first()

                if seq:
                    sequence_id = seq.id
                    rep = connection.db_session.query(connection.Repetition).filter_by(
                        sequence_id=sequence_id,
                        value=repetition)\
                        .first()

                    if rep:
                        repetition_id = rep.id
                        nii = connection.Nifti(
                            repetition_id=repetition_id,
                            path=file_path,
                            result_type=prefix_type,
                            output_type=postfix_type
                        )
                        connection.db_session.add(nii)
                        connection.db_session.commit()
