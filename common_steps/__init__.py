"""Common steps that can use used by any type of pipeline"""


from airflow import configuration


def default_config(section, key, value, fill_empty=False):
    if not configuration.has_option(section, key):
        configuration.set(section, key, value)
    if fill_empty and not configuration.get(section, key):
        configuration.set(section, key, value)


class Step:

    def __init__(self, task, task_id, priority_weight):
        self.task = task
        self.task_id = task_id
        self.priority_weight = priority_weight


initial_step = Step(None, None, 0)
