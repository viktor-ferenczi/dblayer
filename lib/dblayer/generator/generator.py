""" Database abstraction layer code generator
"""

import datetime

import bottle

import dblayer
from dblayer import constants

def generate(database, backend, abstraction_class_name):
    """ Generates database abstraction layer code for the given database
    model using the given database server specific backend module
    """
    return ''.join(bottle.template(
        'database',
        template_lookup=[constants.GENERATOR_TEMPLATE_DIRECTORY_PATH],
        template_settings=dict(noescape=True),
        constants=constants,
        database=database,
        backend=backend,
        abstraction_class_name=abstraction_class_name,
        now=datetime.datetime.now()))
