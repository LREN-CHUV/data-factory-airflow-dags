"""Pipelines for image preprocessing"""


import os


def lren_build_daily_folder_path_callable(folder, date):
    daily_folder = os.path.join(folder, date.strftime('%Y'), date.strftime('%Y%m%d'))

    # Ugly hack for LREN
    if not os.path.isdir(daily_folder):
        daily_folder = os.path.join(folder, '2014', date.strftime('%Y%m%d'))

    return daily_folder


def lren_accept_folder(path):
    session_id = os.path.basename(path)
    sid = session_id.strip().lower()
    return not ('delete' in sid) and not ('phantom' in sid)
