""" Utility functions
"""

import itertools, random, datetime

import dblayer
from dblayer import constants

def get_next_definition_serial(iterator=itertools.count()):
    """ Returns the next serial used to sort model definition instances
    """
    return iterator.next()

def get_random_id(random=random.SystemRandom()):
    """ Returns a new random database ID value
    """
    return random.randrange(*constants.DATABASE_ID_RANGE)

def log(msg, *args):
    """ Log
    """
    print '%s: %s' % (
        datetime.datetime.now().isoformat(' '),
        msg % args)
