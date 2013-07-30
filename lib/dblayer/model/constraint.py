""" Constraint definitions
"""

import dblayer
from dblayer import util

class BaseConstraint(object):
    """ Base class for database constraints
    """
    
    # Serial number to record the order of constraint definitions
    __definition_serial__ = 0

    # Reference to the table class containing this constraint
    # NOTE: Set by __new__ of the table definition class
    table_class = None
    
    # Reference to the table instance containing this constraint or None for model constraints
    # NOTE: Filled in by Table.__init__ as part of cloning the constraints from the class to the instance
    table = None

    # Name of the constraint
    # NOTE: Set by __new__ of the table definition class
    name = ''
    
    # Indicates that this model object is added implicitly by some other model object
    implicit = False

    @staticmethod
    def sort_key(obj):
        return obj.__definition_serial__
    
    def __init__(self):
        
        assert self.__class__ is not BaseConstraint, (
            'Only subclasses of BaseConstraint can be instantiated!')
        
        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()
    
    def __str__(self):
        return '<%s Constraint: %s.%s>' % (
            self.__class__.__name__, 
            self.table_class.__name__ if self.table_class else '?', 
            self.name)

    def __repr__(self):
        return '%s.%s()' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__)
    
    def clone(self, table):
        """ Clone this constraint for a table instance
        
        NOTE: It is called by Table.__init__ to bound the constraints to the table instance.
        
        """
        clone = self.__class__(None)
        clone.__dict__.update(self.__dict__)
        clone.table = table
        return clone
    
class BaseColumnConstraint(BaseConstraint):
    """ Base class for constraints applied on a single column or a set of columns
    """
    # Tuple of the member columns
    columns = ()
    
    def __init__(self, *columns):
        BaseConstraint.__init__(self)
        
        assert self.__class__ is not BaseColumnConstraint, (
            'Only subclasses of BaseColumnConstraint can be instantiated!')
        
        assert columns, 'This constraint must be applied to at least one column!'
        self.columns = columns
    
    def __str__(self):
        return '<%s Constraint: %s.%s on %r>' % (
            self.__class__.__name__, 
            self.table_class.__name__ if self.table_class else '?', 
            self.name,
            tuple(column.name for column in self.columns))
    
    def __repr__(self):
        return '%s.%s(%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            ', '.join(column.name for column in self.columns))

    def clone(self, table):
        clone = BaseConstraint.clone(self, table)
        clone.columns = [getattr(table, column.name) for column in self.columns]
        return clone

class PrimaryKey(BaseColumnConstraint):
    """ Primary key constraint
    """
    
class ForeignKey(BaseColumnConstraint):
    """ Foreign key constraint
    """
    
class Unique(BaseColumnConstraint):
    """ Unique index constraint
    """

class Check(BaseConstraint):
    """ Constraint given as a free expression on the record fields
    """
    
    def __init__(self, expression):
        BaseConstraint.__init__(self)
        self.expression = expression

    def __repr__(self):
        return '%s.%s(%r)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            self.expression)
