""" Base class for database table definitions
"""

import dblayer
from dblayer import util, constants
from dblayer.model import column, constraint, index, trigger

class Table(object):
    """ Base class for database table models
    
    Subclasses are the table models.
    
    Instances represent actual database tables bound to a database model.
    
    Instances contain cloned column, constraint and index objects bound
    to that instance. It is needed to implement cross table references
    like foreign keys and joins.
    
    """
    
    # True value indicates that this object has to be created in the database
    _creatable = True
    
    # True value indicates that this object is writable in the database
    _writable = True
    
    # Flag indicating that the model has been initialized
    _initialized = False
    
    # Reference to the database class containing this table
    # NOTE: Set by __new__ of the database definition class
    _database_class = None
    
    # Name of the database table (never the alias name)
    # NOTE: Set by __new__ of the database definition class
    _table_name = ''
    
    # Name of the table, it can be either a database table name in the case
    # of a physical table or an alias name while referencing from a view
    # NOTE: Set by __new__ of the database and query definition classes
    _name = ''
    
    # List of column definitions in definition order
    _column_list = ()
    
    # List of database constraints in definition order
    _constraint_list = ()
    
    # List of database indexes in definition order
    _index_list = ()

    # List of database triggers in definition order
    _trigger_list = ()
    
    # The definition of the primary key column for this table,
    # which is just a reference to the PrimaryKey column definition instance.
    # It can also be None for read-only tables and views without a primary key.
    _primary_key = None
    
    # Foreign key column referencing this table instance inside a query if any
    _referer = None

    # Serial number to record the order of column definitions
    __definition_serial__ = 0
    
    def __repr__(self):
        return '<Table: %s>' % self._name
    
    __str__ = __repr__
    
    @staticmethod
    def _sort_key(obj):
        """ Sort key to preserve the lexical definition order
        """
        return obj.__definition_serial__
    
    def __new__(cls):
        # Initialize the class only once
        if not cls._initialized:
            cls.initialize()
            
        return super(Table, cls).__new__(cls)
    
    @classmethod
    def initialize(cls):
        
        # Mark the class as initialized
        cls._initialized = True
        
        # Assign name to constraints and indexes and collect them
        cls._prepare_table_definition()
        
    @classmethod
    def _prepare_table_definition(cls):
        """ Prepares the table definition before the first instantiation
        """
        cls._column_list = []
        cls._constraint_list = []
        cls._index_list = []
        cls._trigger_list = []
        for name in dir(cls):
            value = getattr(cls, name)

            if isinstance(value, column.BaseColumn):
                value.name = name
                value.table_class = cls
                cls._column_list.append(value)
                if value.primary_key:
                    assert cls._primary_key is None, 'More than one primary key columns are defined for table: %s' % cls.__name__
                    cls._primary_key = value
                    
            elif isinstance(value, constraint.BaseConstraint):
                value.name = name
                value.table_class = cls
                cls._constraint_list.append(value)
                
            elif isinstance(value, index.BaseIndex):
                value.name = name
                value.table_class = cls
                cls._index_list.append(value)
                
            elif isinstance(value, trigger.BaseTrigger):
                value.name = name
                value.table_class = cls
                cls._trigger_list.append(value)
        
        # Sort the definition objects to keep their source code order
        cls._column_list.sort(key=column.BaseColumn.sort_key)
        cls._constraint_list.sort(key=constraint.BaseConstraint.sort_key)
        cls._index_list.sort(key=index.BaseIndex.sort_key)
        cls._trigger_list.sort(key=trigger.BaseTrigger.sort_key)
        
        # If we have a primary key column, then it must be the first one
        if cls._primary_key:
            assert cls._column_list[0] is cls._primary_key, (
                'The primary key column must be the first one defined for table: %s' % cls.__name__)
            
        # Add implicit definitions required by some of the existing definitions
        for member in cls._column_list + cls._index_list:
            for name, definition in member.get_implicit_definition_list_for_table_class(cls):
                assert not hasattr(cls, name), (
                    'Attribute name %s.%s collides with implicit %s definition required by %s.%s!' % 
                    (cls.__name__, name, definition.__class__.__name__, cls.__name__, member.name))
                if isinstance(definition, constraint.BaseConstraint):
                    cls._constraint_list.append(definition)
                elif isinstance(definition, index.BaseIndex):
                    cls._index_list.append(definition)
                elif isinstance(definition, trigger.BaseTrigger):
                    cls._trigger_list.append(definition)
                else:
                    raise TypeError(
                        'Unsupported implicit definition required by %s.%s: %r' % 
                        (cls.__name__, member.name, definition))
                definition.table_class = cls
                definition.name = name
                definition.implicit = True
                setattr(cls, name, definition)
                
    def __init__(self):
        # Do not allow initialization of the abstract base class
        assert self.__class__ is not Table, (
            'Only subclasses of Table can be instantiated!')

        # Record the definition order
        self.__definition_serial__ = util.get_next_definition_serial()
        
        # Clone columns, constraints and indexes
        self._column_list = [column.clone(self) for column in self._column_list]
        self._constraint_list = [constraint.clone(self) for constraint in self._constraint_list]
        self._index_list = [index.clone(self) for index in self._index_list]
        self._trigger_list = [trigger.clone(self) for trigger in self._trigger_list]
        
        # Override the model definitions with the bound instances
        for column in self._column_list:
            setattr(self, column.name, column)
        for constraint in self._constraint_list:
            setattr(self, constraint.name, constraint)
        for index in self._index_list:
            setattr(self, index.name, index)
        for trigger in self._trigger_list:
            setattr(self, trigger.name, trigger)
            
        # Reassign the primary key
        if self._primary_key:
            self._primary_key = getattr(self, self._primary_key.name)
            
    def join(self, foreign_key_column):
        """ Joins this table inside a query with the given foreign key
        
        It is an INNER JOIN for non-nullable foreign keys and a LEFT JOIN
        for nullable ones. It just records the joined table as a source to
        build views, does not generate actual SQL.
        
        Each table can be joined with a single foreign key.
        Unjoined tables are cross-joined.
        
        """
        if constants.DEBUG:
            assert isinstance(foreign_key_column, column.ForeignKey)
            assert foreign_key_column.table, (
                'Only foreign keys bound to a table instance can be used to join tables.')
            assert self._referer is None, (
                'Table or view %s has already been joined with foreign key column: %s' %
                (self.__class__.__name__, foreign_key_column._name))
            
        self._referer = foreign_key_column

    @classmethod
    def pretty_format_class(cls):
        """ Formats source code defining the table
        """
        line_list = ['class %s(table.Table):' % cls.__name__]
        append_line = line_list.append

        if cls.__doc__:
            if '\n' in cls.__doc__:
                append_line('    """%s"""' % cls.__doc__)
            else:
                append_line('    """ %s """' % cls.__doc__.strip())
        else:    
            append_line('')
        
        extra_line_list = []
        
        for obj in cls._column_list:
            if isinstance(obj, column.ForeignKey) and obj.referenced_table_class is cls:
                obj.referenced_table_class = None
                append_line('    %s = %s' % (obj.name, obj.full_repr()))
                obj.referenced_table_class = cls
                extra_line_list.append('%s.%s.referenced_table_class = %s' % (
                    cls.__name__, obj.name, cls.__name__))
            else:
                append_line('    %s = %s' % (obj.name, obj.full_repr()))

        for obj in cls._constraint_list:
            if not obj.implicit:
                append_line('    %s = %r' % (obj.name, obj))
            
        for obj in cls._index_list:
            if not obj.implicit:
                append_line('    %s = %r' % (obj.name, obj))
            
        for obj in cls._trigger_list:
            if not obj.implicit:
                append_line('    %s = %r' % (obj.name, obj))
                
        if extra_line_list:
            append_line('')
            line_list.extend(extra_line_list)
            
        return '\n'.join(line_list)
