""" Database abstraction layer code generator
"""

import datetime

import bottle

import dblayer
from dblayer import constants

class GeneratorOptions(object):
    """ Code generation options
    """
    # Generate code for the following operations only
    insert = True
    update = True
    delete = True
    create = True
    drop = True

def generate(database, backend, abstraction_class_name, options=None):
    """ Generates database abstraction layer code for the given database
    model using the given database server specific backend module
    """
    if options is None:
        options = GeneratorOptions()
    else:
        assert isinstance(options, GeneratorOptions)
    
    format = __import__(backend.__name__, fromlist=('format', )).format
    
    return ''.join(bottle.template(
        'database',
        template_lookup=[constants.GENERATOR_TEMPLATE_DIRECTORY_PATH],
        template_settings=dict(noescape=True),
        constants=constants,
        database=database,
        backend=backend,
        options=options,
        format=format,
        abstraction_class_name=abstraction_class_name,
        now=datetime.datetime.now()))
