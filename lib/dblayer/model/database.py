""" Base class for database models

Database models are used to collect the tables, views and queries building
up a single database or schema. It is used to generate the code for the
database abstraction layer used by the actual application. It is not used
directly to access the database, it only represents the model of the database
or schema.

"""

import dblayer
from dblayer.model import table, column, query, procedure
from dblayer.generator import generator

class Database(object):
    """ Base class for database models
    """

    # Flag indicating that the model has been initialized
    _initialized = False
    
    # List of initialized table models in the database
    _table_list = ()
    
    # List of initialized stored procedure definitions in the database
    _procedure_list = ()    
    
    # Name of the abstraction layer class
    _abstraction_class_name = ''
    
    def __new__(cls, *args, **kws):
        # Initialize the class only once
        if not cls._initialized:
            cls.initialize()

        return super(Database, cls).__new__(cls)

    @classmethod
    def initialize(cls):
        """ Initialize the model objects and resolve all cross-dependencies
        """
        cls._initialized = True

        # Assign name to tables and collect them
        cls._table_list = []
        cls._procedure_list = []
        for name in dir(cls):
            value = getattr(cls, name)
            
            if isinstance(value, table.Table):
                assert value._database_class is None, 'Table already bound to a database class!'
                value.__class__._table_name = name
                value._name = name
                value._database_class = cls
                cls._table_list.append(value)
                
            elif isinstance(value, procedure.BaseProcedure):
                value.name = name
                value.database_class = cls
                cls._procedure_list.append(value)
                
        # Sort them by definition order
        cls._table_list.sort(key=table.Table._sort_key)
        cls._procedure_list.sort(key=procedure.Procedure.sort_key)
        
        # Connect all foreign keys by looking up the referenced tables
        table_map = dict(
            (table_instance.__class__.__name__, table_instance) 
            for table_instance in cls._table_list)
        assert len(table_map) == len(cls._table_list), (
            'Some of the table classes were used more than once to construct the database model!')
        for table_instance in cls._table_list:
            for fk_column in table_instance._column_list:
                if isinstance(fk_column, column.ForeignKey):
                    referenced_table = table_map.get(fk_column.referenced_table_class.__name__)
                    assert referenced_table, (
                        'Could not find referenced database table for foreign key: %s.%s' % 
                        (table_instance.__class__.__name__, fk_column.name))
                    fk_column.referenced_table = referenced_table
        
    def __init__(self, abstraction_class_name):
        self._abstraction_class_name = abstraction_class_name
        
    def __str__(self):
        return '<%s Database>' % self.__class__.__name__
    
    def __repr__(self):
        return '%s.%s(%r)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            self._abstraction_class_name)
    
    def generate(self, backend, options=None):
        """ Generate database abstraction layer module for use with the given
        database server specific backend module
        """
        return generator.generate(self, backend, self._abstraction_class_name, options)

    @classmethod
    def pretty_format_class(cls):
        """ Formats source code defining the database model, including all the tables used in it
        """
        # Initialize the class only once
        if not cls._initialized:
            cls.initialize()
        
        line_list = '''\
import dblayer
import dblayer.backend.postgresql
from dblayer.model import database, table, query
from dblayer.model import column, index, constraint, aggregate, function, trigger, procedure
'''.split('\n')
        append_line = line_list.append
        
        for obj in cls._table_list:
            assert isinstance(obj, table.Table)
            append_line(obj.__class__.pretty_format_class())
            append_line('')
        
        append_line('class %s(database.Database):' % cls.__name__)
        
        if cls.__doc__:
            if '\n' in cls.__doc__:
                append_line('    """%s"""' % cls.__doc__)
            else:
                append_line('    """ %s """' % cls.__doc__.strip())
        else:    
            append_line('')
            
        for obj in cls._table_list:
            assert isinstance(obj, table.Table)
            append_line('    %s = %s()' % (obj._name, obj.__class__.__name__))
            
        for obj in cls._procedure_list:
            assert isinstance(obj, procedure.Procedure)
            append_line('    %s = %r' % (obj.name, obj))
        
        return '\n'.join(line_list)
