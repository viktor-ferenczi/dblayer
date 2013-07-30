""" Procedure definitions
"""

import dblayer
from dblayer import util

class BaseProcedure(object):
    """ Base class for stored procedures
    """
    
    # Serial number to record the order of procedure definitions
    __definition_serial__ = 0

    # Reference to the database class containing this procedure
    # NOTE: Set by __new__ of the database definition class
    database_class = None

    # Name of the procedure
    # NOTE: Set by __new__ of the database definition class
    name = ''
    
    @staticmethod
    def sort_key(obj):
        return obj.__definition_serial__
    
    def __init__(self, language, argument_list, result, body):
        
        assert self.__class__ is not BaseProcedure, (
            'Only subclasses of BaseProcedure can be instantiated!')
        
        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()
        
        self.language = language
        self.argument_list = argument_list
        self.result = result
        self.body = body
    
    def __str__(self):
        return '<%s: %s%r returns %s>' % (
            self.__class__.__name__, 
            self.name,
            tuple(self.argument_list),
            self.result)
    
    def __repr__(self):
        return '%s.%s(%r, %r, %r, %r)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            self.language, 
            self.argument_list, 
            self.result, 
            self.body)
    
    def clone(self, database):
        """ Clone this procedure for the database instance
        
        NOTE: It is called by Database.__init__ to bound the procedures to the database instance.
        
        """
        clone = self.__class__(None)
        clone.__dict__.update(self.__dict__)
        clone.database = database
        return clone

class Procedure(BaseProcedure):
    """ Stored procedure
    """
