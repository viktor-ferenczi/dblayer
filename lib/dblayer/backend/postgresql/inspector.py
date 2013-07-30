""" Database inspector for PostgreSQL
"""

import new

import dblayer
from dblayer import util
from dblayer.backend.postgresql import database as postgresql_database
from dblayer.model import database, table, column, index, constraint, function


class ColumnInfo(object):
    
    __slots__ = (
        'table_name', 
        'column_name', 
        'data_type', 
        'column_default', 
        'is_nullable', 
        'character_maximum_length', 
        'numeric_precision', 
        'numeric_precision_radix',
        'numeric_scale',
    )
    
    def __init__(self, table_name=None, column_name=None, data_type=None, column_default=None, 
                 is_nullable=None, character_maximum_length=None, numeric_precision=None, 
                 numeric_scale=None, numeric_precision_radix=None):
        self.table_name = table_name
        self.column_name = column_name
        self.data_type = data_type
        self.column_default = column_default
        self.is_nullable = is_nullable
        self.character_maximum_length = character_maximum_length
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
        self.numeric_precision_radix = numeric_precision_radix
        
    def load_information_schema(self, row):
        """ Loads column information from a row selected from information_schema.columns table
        """
        for name in self.__slots__:
            setattr(self, name, row[name])
            
    def __repr__(self):
        name_value_list = [(name, getattr(self, name)) for name in self.__slots__]
        return '%s(%s)' % (
            self.__class__.__name__, 
            ', '.join(
                '%s=%r' % (name, value) 
                for name, value in name_value_list 
                if value is not None))
    
    __str__ = __repr__

class DatabaseInspector(postgresql_database.DatabaseAbstraction):
    
    def __init__(self, primary_key_column_name_set=('id', ), primary_key_columns={}):
        """ Database inspector
        
        Inspects an existing PostgreSQL database and builds up the database model classes runtime.
        
        primary_key_column_name_set: set of primary key column names, defaults to ('id', )
        
        primary_key_columns: primary key names by table: {table_name: primary_key_column_name_set}
        
        """
        postgresql_database.DatabaseAbstraction.__init__(self)
        
        self.primary_key_column_name_set = primary_key_column_name_set
        self.primary_key_columns = primary_key_columns
        
        self.COLUMN_FACTORY_MAP = {
            'bigint': self.define_bigint_column,
            'boolean': self.define_boolean_column,
            'character varying': self.define_varchar_column,
            'date': self.define_date_column, 
            'integer': self.define_integer_column, 
            'real': self.define_real_column, 
            'double precision': self.define_double_column, 
            'numeric': self.define_numeric_column, 
            'text': self.define_text_column, 
            'timestamp without time zone': self.define_timestamp_column, 
            'tsvector': self.define_tsvector_column,
        }
    
    def inspect(self, dsn, database_class_name):
        """ Inspects a database and returns database model class
        
        You can use the pretty_format_class() class method on the database model class returned
        to generate source code for the model.
        
        """
        with self.session(dsn):
            table_class_list = self.inspect_tables()
            
        class_dict = {}
        for table_class in table_class_list:
            assert issubclass(table_class, table.Table)
            class_dict[table_class._table_name] = table_class()
            
        database_class = new.classobj(database_class_name, (database.Database, ), class_dict)
        assert issubclass(database_class, database.Database)
        
        return database_class
            
    def inspect_tables(self):
        """ Inspects database tables and returns list of table definition classes
        """
        raise NotImplementedError()
    
    def convert_table_name_to_python(self, table_name):
        """ Converts underscore table name used in the database to CapitalizedWords format
        """
        return ''.join(word.capitalize() for word in table_name.split('_'))
        
    def inspect_tables(self):
        
        sql = '''
SELECT * 
FROM information_schema.columns 
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
'''

        get_column_factory = self.COLUMN_FACTORY_MAP.get
        
        table_class_map = {}
        with self.cursor() as cursor:
            for row in self.execute_and_fetch_dict_iter(cursor, sql):
                
                column_info = ColumnInfo()
                column_info.load_information_schema(row)
                
                table_class = table_class_map.get(column_info.table_name)
                if table_class is None:
                    class_name = str(self.convert_table_name_to_python(column_info.table_name))
                    table_class = new.classobj(class_name, (table.Table, ), {})
                    table_class._table_name = column_info.table_name
                    table_class_map[column_info.table_name] = table_class
                    
                table_pk_columns = self.primary_key_columns.get(column_info.column_name, ())
                if (column_info.column_name in self.primary_key_column_name_set or
                    column_info.column_name in table_pk_columns):
                    column_factory = self.define_primary_key_column
                else:
                    column_factory = get_column_factory(
                        column_info.data_type, self.define_custom_column)
                
                try:
                    column_definition = column_factory(column_info)
                except ValueError:
                    util.log(
                        'WARNING: Skipping column due to unparsable column info: %r' % column_info)
                    continue
                assert isinstance(column_definition, column.BaseColumn)
                
                setattr(table_class, column_info.column_name, column_definition)
                        
        return table_class_map.values()
    
    def define_custom_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Custom(
            sql_type=column_info.data_type,
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_primary_key_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        serial = 'nextval' in (column_info.column_default or '').lower()
        column_definition = column.PrimaryKey(serial=serial)
        return column_definition
    
    def define_bigint_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Integer(
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_boolean_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Boolean(
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_varchar_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Text(
            maxlength=column_info.character_maximum_length,
            null=column_info.is_nullable)
        return column_definition
    
    def define_date_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Date(
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_integer_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        if isinstance(column_info.column_default, basestring):
            default=function.Custom(column_info.column_default)
        else:
            default=column_info.column_default
        column_definition = column.Integer(
            default=default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_real_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Float(
            double=False,
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_double_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Float(
            double=True,
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_numeric_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        if column_info.numeric_precision_radix != 10:
            raise NotImplementedError()
        if not column_info.numeric_scale:
            column_definition = column.Integer(
                digits=column_info.numeric_precision,
                default=column_info.column_default,
                null=column_info.is_nullable)
        else:
            assert column_info.numeric_precision is not None
            column_definition = column.Decimal(
                precision=column_info.numeric_precision,
                scale=column_info.numeric_scale,
                default=column_info.column_default,
                null=column_info.is_nullable)
        return column_definition
    
    def define_text_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Text(
            maxlength=column_info.character_maximum_length,
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition
    
    def define_timestamp_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.Datetime(
            default=column_info.column_default,
            null=column_info.is_nullable)
        return column_definition

    def define_tsvector_column(self, column_info):
        assert isinstance(column_info, ColumnInfo)
        column_definition = column.SearchDocument(implicit=False)
        return column_definition
