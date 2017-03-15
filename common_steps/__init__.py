from airflow import configuration


def default_config(section, key, value):
    if not configuration.has_option(section, key):
        configuration.set(section, key, value)
