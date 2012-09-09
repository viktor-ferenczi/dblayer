""" Column types
"""

import decimal

import dblayer
from dblayer import util
from dblayer.model import index, constraint

class BaseColumn(object):
    """ Base class for database column models
    """
    
    # Serial number to record the order of column definitions
    __definition_serial__ = 0
    
    # Abstract name of the SQL column type
    abstract_sql_column_type = None
    
    # Reference to the table class containing this column
    # NOTE: Set by __new__ of the table definition class
    table_class = None
    
    # Reference to the table instance containing this column or None for model columns
    # NOTE: Filled in by Table.__init__ as part of cloning the columns from the class to the instance
    table = None

    # Name of the column
    # NOTE: Set by __new__ of the table definition class
    name = ''
    
    # Makrs the primary key column
    primary_key = False
    
    # True value indicates that this column is accessible to Python code
    # as a filed on the associated Record subclass
    accessible = True
    
    # True value indicates that this column can be NULL (None in Python)
    # NOTE: NOT NULL fields are required only if default is None.
    null = False
    
    # Default value for the column or None
    # NOTE: It is a Python value, which will be converted to a database value.
    # NOTE: Setting null=False and default=None makes this a required field.
    default = None
    
    # Documentation
    doc = None

    @staticmethod
    def sort_key(obj):
        """ Sort key to preserve the lexical definition order
        """
        return obj.__definition_serial__
    
    def __init__(self, doc=None):
        
        assert self.__class__ is not BaseColumn, (
            'Only subclasses of BaseColumn can be instantiated!')
        
        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()

        # Store column documentation if any
        if doc is not None:
            self.doc = doc
        
    def __repr__(self):
        return '<%s Column: %s.%s as %s>' % (self.__class__.__name__, self.table_class.__name__ if self.table_class else '?', self.name, self.__class__.__name__)
    
    __str__ = __repr__
    
    def clone(self, table):
        """ Clone this column for a table instance
        
        NOTE: It is called by Table.__init__ to bound the columns to the table instance.
        
        """
        clone = self.__class__()
        clone.__dict__.update(self.__dict__)
        clone.table = table
        return clone
    
    def get_implicit_definition_list_for_table_class(self, table_class):
        """ Returns list of (name, definition) tuples for the implicit definitions
        required on the table model class level
        
        """
        return []

class PrimaryKey(BaseColumn):
    """ Primary key column
    """
    
    abstract_sql_column_type = 'PrimaryKey'
    doc = 'Primary key'
    primary_key = True
    
    def __init__(self, doc=None):
        BaseColumn.__init__(self, doc)
        
        # Move the primary key fields to the top of the column list
        self.__definition_serial__ -= 1000000000
        
    def get_implicit_definition_list_for_table_class(self, table_class):
        return [('pk_%s' % self.name, constraint.PrimaryKey(self))]
    
class ForeignKey(BaseColumn):
    """ Foreign key column
    """
    
    abstract_sql_column_type = 'ForeignKey'
    
    # Definition of the referenced table
    referenced_table_class = None
    
    # Referenced table instance, filled in by Database.__init__ after
    # creating all the database model objects
    referenced_table = None
    
    def __init__(self, referenced_table_class=None, null=False, doc=None):
        # NOTE: The referenced table class can be set to None and filled later
        self.referenced_table_class = referenced_table_class
        self.null = null
        BaseColumn.__init__(self, doc)
    
    def get_implicit_definition_list_for_table_class(self, table_class):
        return [('fk_%s' % self.name, constraint.ForeignKey(self))]
    
class Boolean(BaseColumn):
    """ Boolean column
    """
    
    abstract_sql_column_type = 'Boolean'
    
    def __init__(self, default=None, null=False, doc=None):
        self.default = None if default is None else bool(default)
        self.null = bool(null)
        BaseColumn.__init__(self, doc)
    
class Integer(BaseColumn):
    """ Integer column
    """
    
    abstract_sql_column_type = 'Integer'
    
    # Number of digits
    digits = None
    
    def __init__(self, digits=None, default=None, null=False, doc=None):
        self.digits = int(digits) if digits else None
        self.default = None if default is None else int(default)
        self.null = bool(null)
        BaseColumn.__init__(self, doc)
    
class Text(BaseColumn):
    """ Unicode text column
    """
    
    abstract_sql_column_type = 'Text'
    
    # Maximum number of characters in the string (unicode characters, not bytes)
    maxlength = None
    
    def __init__(self, maxlength=None, default=None, null=False, doc=None):
        self.maxlength = int(maxlength) if maxlength else None
        self.default = None if default is None else unicode(default)
        self.null = bool(null)
        BaseColumn.__init__(self, doc)
    
class Date(BaseColumn):
    """ Date column
    """
    
    abstract_sql_column_type = 'Date'
    
    def __init__(self, null=False, doc=None):
        self.null =  bool(null)
        BaseColumn.__init__(self, doc)
    
class Datetime(BaseColumn):
    """ Datetime (timestamp) column
    """
    
    abstract_sql_column_type = 'Datetime'
    
    def __init__(self, null=False, doc=None):
        self.null =  bool(null)
        BaseColumn.__init__(self, doc)

class SearchDocument(BaseColumn):
    """ Search document column
    """
    
    abstract_sql_column_type = 'SearchDocument'

    # Do not add this as a field on the Record subclass, hide from Python code
    accessible = False
    
    # Expression to generate the value of this column from the other columns.
    # It is currently limited to a tuple of columns to concatenate with space
    # separators to build up the search document.
    # TODO: Allow for free form expressions using the function module.
    expression = None
    
    def __init__(self, expression=None, doc=None):
        self.expression = expression
        self.null =  False
        BaseColumn.__init__(self, doc=doc)

    def get_implicit_definition_list_for_table_class(self, table_class):
        return [(self.name + '_index', index.FullTextSearchIndex(*self.expression))]
