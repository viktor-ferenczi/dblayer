""" Index definitions
"""

import dblayer
from dblayer import util

class BaseIndex(object):
    """ Base class for database indexes models
    """
    
    # Serial number to record the order of column definitions
    __definition_serial__ = 0

    # Reference to the table class containing this column
    # NOTE: Set by __new__ of the table definition class
    table_class = None
    
    # Reference to the table instance containing this column or None for model columns
    # NOTE: Filled in by Table.__init__ as part of cloning the columns from the class to the instance
    table = None

    # Name of the constraint
    # NOTE: Set by __new__ in the table definition class
    name = ''
    
    # List of the member columns
    columns = ()
    
    # Indicates that this model object is added implicitly by some other model object
    implicit = False
    
    @staticmethod
    def sort_key(obj):
        return obj.__definition_serial__
    
    def __init__(self, *columns):
        
        assert self.__class__ is not BaseIndex, (
            'Only subclasses of BaseIndex can be instantiated!')
        
        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()

        # Verify
        assert columns, 'This index must be applied to at least one column!'
        self.columns = columns
    
    def __str__(self):
        return '<%s Index: %s.%s on %s>' % (
            self.__class__.__name__, 
            self.table_class.__name__ if self.table_class else '?', 
            self.name,
            ', '.join(column.name for column in self.columns))
    
    def __repr__(self):
        return '%s.%s(%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            ', '.join(column.name for column in self.columns))
    
    def clone(self, table):
        """ Clone this index for a table instance
        
        NOTE: It is called by Table.__init__ to bound the columns to the table instance.
        
        """
        clone = self.__class__(None)
        clone.__dict__.update(self.__dict__)
        clone.columns = [getattr(table, column.name) for column in self.columns]
        clone.table = table
        return clone

    def get_implicit_definition_list_for_table_class(self, table_class):
        """ Returns list of (name, definition) tuples for the implicit definitions
        required on the table model class level
        
        """
        return []

class Index(BaseIndex):
    """ Regular B-Tree based index
    """

class FullTextSearchIndex(BaseIndex):
    """ Full text search index
    
    This kind of index is automatically applied to a SearchDocument columns.
    
    """
