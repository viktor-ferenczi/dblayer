""" Trigger definitions
"""

import dblayer
from dblayer import util

class BaseTrigger(object):
    """ Base class for database triggers
    """
    
    # Serial number to record the order of trigger definitions
    __definition_serial__ = 0

    # Reference to the table class containing this trigger
    # NOTE: Set by __new__ of the table definition class
    table_class = None
    
    # Reference to the table instance containing this trigger or None for model triggers
    # NOTE: Filled in by Table.__init__ as part of cloning the triggers from the class to the instance
    table = None

    # Name of the trigger
    # NOTE: Set by __new__ of the table definition class
    name = ''
    
    # Indicates that this model object is added implicitly by some other model object
    implicit = False
    
    def __str__(self):
        return '<%s Trigger: %s.%s>' % (
            self.__class__.__name__, 
            self.table_class.__name__ if self.table_class else '?', 
            self.name)
    
    def __repr__(self):
        return '%s.%s(%r%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            self.procedure_name,
            ''.join(', %r' % parameter for parameter in self.procedure_parameters))
    
    @staticmethod
    def sort_key(obj):
        return obj.__definition_serial__
    
    def __init__(self, procedure_name, *procedure_parameters):
        
        assert self.__class__ is not BaseTrigger, (
            'Only subclasses of BaseTrigger can be instantiated!')
        
        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()
        
        self.procedure_name = procedure_name
        self.procedure_parameters = procedure_parameters
    
    def clone(self, table):
        """ Clone this trigger for a table instance
        
        NOTE: It is called by Table.__init__ to bound the triggers to the table instance.
        
        """
        clone = self.__class__(None)
        clone.__dict__.update(self.__dict__)
        clone.table = table
        return clone
    
class BeforeInsertRow(BaseTrigger):
    """ Trigger executed before inserting a database row
    """

class BeforeUpdateRow(BaseTrigger):
    """ Trigger executed before updating a database row
    """

class BeforeInsertOrUpdateRow(BaseTrigger):
    """ Trigger executed before inserting or updating a database row
    """

class BeforeDeleteRow(BaseTrigger):
    """ Trigger executed before deleting a database row
    """

class BeforeInsertStatement(BaseTrigger):
    """ Trigger executed before executing an insert statement
    """

class BeforeUpdateStatement(BaseTrigger):
    """ Trigger executed before executing an update statement
    """

class BeforeInsertOrUpdateStatement(BaseTrigger):
    """ Trigger executed before executing an insert or update statement
    """

class BeforeDeleteStatement(BaseTrigger):
    """ Trigger executed before executing a delete statement
    """

class AfterInsertRow(BaseTrigger):
    """ Trigger executed before inserting a database row
    """

class AfterUpdateRow(BaseTrigger):
    """ Trigger executed before updating a database row
    """

class AfterInsertOrUpdateRow(BaseTrigger):
    """ Trigger executed before inserting or updating a database row
    """

class AfterDeleteRow(BaseTrigger):
    """ Trigger executed before deleting a database row
    """

class AfterInsertStatement(BaseTrigger):
    """ Trigger executed before executing an insert statement
    """

class AfterUpdateStatement(BaseTrigger):
    """ Trigger executed before executing an update statement
    """

class AfterInsertOrUpdateStatement(BaseTrigger):
    """ Trigger executed before executing an insert or update statement
    """

class AfterDeleteStatement(BaseTrigger):
    """ Trigger executed before executing a delete statement
    """
