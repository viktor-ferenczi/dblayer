""" {{abstraction_class_name}} - generated database abstraction layer

Database model: {{database.__class__.__name__}}
Backend server: {{backend.__name__}}
Generated   on: {{now.isoformat(' ').rsplit('.', 1)[0]}}

NOTE: It is a generated database abstraction layer. Please do not modify.
      All your changes will be lost next time this code is generated!
      Please add your code in a subclass.

"""

import time

import dblayer
from dblayer import constants, util
from {{backend.__name__}} import database, record

NA = constants.NA

### Record classes

%for table in database._table_list:
%accessible_column_list = [column for column in table._column_list if column.accessible]
class {{table.__class__.__name__}}Record(record.Record):
    """ {{table.__class__.__name__}} record
    
    %for column in accessible_column_list:
    %if column.doc:
    * {{column.name}}: {{column.doc}}
    %else:
    * {{column.name}}: Undocumented
    %end
      %if column.null:
      Nullable, default: {{repr(column.default)}}
      %else:
      %if column.default is None:
      Required
      %else:
      Required, default: {{repr(column.default)}}
      %end
      %end
    
    %end
    """
    
    ### Runtime model information
    
    # Name of the corresponding database table storing the actual records
    _table_name = {{repr(table._name)}}
    
    # Column information
    _column_name_list = {{tuple(column.name for column in accessible_column_list)}}
    _quoted_column_name_list = {{tuple(format.quote_name(column.name) for column in accessible_column_list)}}
    _nullable_column_name_set = set({{tuple(column.name for column in accessible_column_list)}})
    _column_default_map = {{dict((column.name, column.default) for column in accessible_column_list if column.default is not None and not column.has_custom_default)}}
    
    ### Optimization
    
    __slots__ = _column_name_list
    
    ### Implementation
    
    def __init__(
        self,
        {{',\n        '.join('%s=%r' % (column.name, (None if column.has_custom_default else column.default)) for column in accessible_column_list)}}):
        """ Creates {{table.__class__.__name__}} record in memory
        
        %for column in accessible_column_list:
        %if column.doc:
        * {{column.name}}: {{column.doc}}
        %else:
        * {{column.name}}: Undocumented
        %end
          %if column.null:
          Nullable, default: {{repr(column.default)}}
          %else:
          %if column.default is None:
          Required
          %else:
          Required, default: {{repr(column.default)}}
          %end
          %end
        
        %end
        """
        {{'\n        '.join('self.%s = %s' % (column.name, column.name) for column in accessible_column_list)}}

    @property
    def tuple(self):
        """ Returns a tuple with the field values of this {{table.__class__.__name__}} record
        """
        return (
            {{',\n            '.join('self.%s' % column.name for column in accessible_column_list)}})
    
    @property
    def dict(self):
        """ Returns a dictionary with the field values of this {{table.__class__.__name__}} record
        """
        return dict(
            {{',\n            '.join('%s=self.%s' % (column.name, column.name) for column in accessible_column_list)}})

%end
### Database abstraction

class {{abstraction_class_name}}(database.DatabaseAbstraction):
    """ Database abstraction layer
    
    Database model: {{database.__class__.__name__}}
    Backend server: {{backend.__name__}}
    Generated   on: {{now.isoformat(' ').rsplit('.', 1)[0]}}
    
    """

    # Database backend module, required by the methods in the DatabaseAbstraction base class
    import {{backend.__name__}}.format as _format
    
    # Classes
    from {{backend.__name__}}.clauses import Clauses
    from {{backend.__name__}}.error import (
        Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError, 
        IntegrityError, InternalError, ProgrammingError, NotSupportedError)
    
    # Constants
    
    # SQL statement for the savepoint set before each indentity insert statement
    _SQL_IDENTITY_INSERT_SAVEPOINT = {{repr(format.format_savepoint('before_identity_insert'))}}
    
    # SQL statement to release a savepoint after a successful identity insert
    _SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT = {{repr(format.format_release_savepoint('before_identity_insert'))}}
    
    # SQL statement for rolling back to the savepoint after a failing insert statement
    _SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT = {{repr(format.format_rollback_to_savepoint('before_identity_insert'))}}
    
    ### Creating in-memory records - override them to provide custom record classes
    
    %for table in database._table_list:
    new_{{table._name}} = {{table.__class__.__name__}}Record
    %end
    
    ### Selecting from the database
    
    %for table in database._table_list:
    %table_name = table._name
    %table_constant_prefix = '_%s' % table_name.upper()
    %accessible_column_list = tuple(column for column in table._column_list if column.accessible)
    %table_column_name_list = tuple(column.name for column in accessible_column_list)
    %table_quoted_column_list = map(format.format_expression, accessible_column_list)
    %table_order_by_map = format.format_table_order_by_map(table)
    %table_condition_map = format.format_table_condition_map(table)
    %if table._creatable:
    def get_{{table._name}}(
        self,
        id=None,
        where=None,
        parameter_tuple=(),
        order_by=(),
        offset=None):
        """ Retrieves a {{table.__class__.__name__}} record or None if not found
        """
        if id is not None:
            %if table._primary_key is None:
            raise ValueError('The {{table.__class__.__name__}} table does not have a primary key, so no way to find a record by primary key value!')
            %else:
            %if constants.DEBUG:
            if where is not None:
                raise ValueError('Cannot pass a where condition while searching for a record by its primary key value (id)!')
            if parameter_tuple:
                raise ValueError('Cannot pass parameter_tuple while searching for a record by its primary key value (id)!')
            %end
            parameter_tuple = (id, )
            where = '{{format.quote_name(table._primary_key.name)}} = %s'
            %end
        else:
            if not isinstance(where, basestring):
                where = self._format.format_expression(where)
                
        formatted_order_by = self._format.format_order_by(self.{{table_constant_prefix}}_ORDER_BY_MAP, order_by)
        
        clauses = self.Clauses(
            table_list=({{repr(table._table_name)}}, ),
            field_list=self.new_{{table._name}}._quoted_column_name_list,
            where=where,
            order_by=formatted_order_by,
            limit=1,
            offset=offset)
            
        record = self.get_record(
            self.new_{{table._name}},
            clauses,
            parameter_tuple)
            
        if 0:
            assert isinstance(record, {{table.__class__.__name__}}Record)
            
        return record
        
    def get_{{table._name}}_list(
        self,
        where='',
        parameter_tuple=(),
        order_by=(),
        limit=None,
        offset=None):
        """ Retrieves list of {{table.__class__.__name__}} records
        """
        formatted_order_by = self._format.format_order_by(self.{{table_constant_prefix}}_ORDER_BY_MAP, order_by)
        
        clauses = self.Clauses(
            table_list=({{repr(table._table_name)}}, ),
            field_list=self.new_{{table._name}}._quoted_column_name_list,
            where=where,
            order_by=formatted_order_by,
            limit=limit,
            offset=offset)
            
        return self.get_record_list(
            self.new_{{table._name}},
            clauses,
            parameter_tuple)
    
    def get_{{table._name}}_iter(
        self,
        where='',
        parameter_tuple=(),
        order_by=(),
        limit=None,
        offset=None):
        """ Iterates on {{table.__class__.__name__}} records
        """
        if not isinstance(where, basestring):
            where = self._format.format_expression(where)
        
        formatted_order_by = self._format.format_order_by(self.{{table_constant_prefix}}_ORDER_BY_MAP, order_by)
        
        clauses = self.Clauses(
            table_list=({{repr(table._table_name)}}, ),
            field_list=self.new_{{table._name}}._quoted_column_name_list,
            where=where,
            order_by=formatted_order_by,
            limit=limit,
            offset=offset)
            
        for record in self.get_record_iter(
                self.new_{{table._name}},
                clauses,
                parameter_tuple):
            
            yield record
            
    def get_{{table._name}}_count(
        self,
        where='',
        parameter_tuple=()):
        """ Counts {{table.__class__.__name__}} records in the database
        """
        if not isinstance(where, basestring):
            where = self._format.format_expression(where)
        
        clauses = self.Clauses(
            table_list=({{repr(table._table_name)}}, ),
            field_list=('COUNT(*)', ),
            where=where)
            
        sql = self._format.format_select(clauses)
        
        with self.cursor() as cursor:
            return self.execute_and_fetch_one(cursor, sql)[0]
            
    {{table_constant_prefix}}_TABLE_LIST = [
        ({{repr(table._table_name)}}, {{repr(table_name)}})]
        
    {{table_constant_prefix}}_FIELD_LIST = [
        %for field_info in table_quoted_column_list:
        {{repr(field_info)}},
        %end
        ]
        
    {{table_constant_prefix}}_ORDER_BY_MAP = {
        %for name, sql_expression in sorted(table_order_by_map.items()):
        {{repr(name)}}: {{repr(sql_expression)}},
        %end
        }
        
    {{table_constant_prefix}}_CONDITION_MAP = {
        %for argument_name, condition_info in sorted(table_condition_map.items()):
        %suffix, formatting_function_name, value_expression = condition_info
        {{repr(argument_name)}}: ({{repr(suffix)}}, _format.{{formatting_function_name}}, {{repr(value_expression)}}),
        %end
        }
    
    def find_{{table_name}}(
        self,
        %if constants.DEBUG:
        %for name in table_column_name_list:
        {{name}}=NA,
        %end
        %end
        order_by=(),
        offset=None,
        where=None,
        **runtime_conditions):
        """ Finds {{table.__class__.__name__}} record by field values, returns None if not found
        """
        %if constants.DEBUG:
        %for name in table_column_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end
        
        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{table_constant_prefix}}_TABLE_LIST,
            self.{{table_constant_prefix}}_FIELD_LIST,
            self.{{table_constant_prefix}}_CONDITION_MAP,
            {},
            [],
            self.{{table_constant_prefix}}_ORDER_BY_MAP,
            where,
            None,
            runtime_conditions,
            order_by,
            1,
            offset)
        
        with self.cursor() as cursor:
            %if constants.LOG_SQL_ANALYSIS:
            self.log_analysis(cursor, sql, parameter_tuple)
            %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            %end
            row = self.execute_and_fetch_one(cursor, sql, parameter_tuple)
            %if constants.PROFILE_QUERIES:
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            %end
            
        if row is None:
            return None
            
        return self.new_{{table_name}}(*row)
        
    def find_{{table_name}}_list(
        self,
        %if constants.DEBUG:
        %for name in table_column_name_list:
        {{name}}=NA,
        %end
        %end
        order_by=(),
        limit=None,
        offset=None,
        where=None,
        **runtime_conditions):
        """ Finds {{table.__class__.__name__}} records by field values
        """
        %if constants.DEBUG:
        %for name in table_column_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end
        
        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{table_constant_prefix}}_TABLE_LIST,
            self.{{table_constant_prefix}}_FIELD_LIST,
            self.{{table_constant_prefix}}_CONDITION_MAP,
            {},
            [],
            self.{{table_constant_prefix}}_ORDER_BY_MAP,
            where,
            None,
            runtime_conditions,
            order_by,
            limit,
            offset)
        
        record_class = self.new_{{table_name}}    
        with self.cursor() as cursor:
            %if constants.LOG_SQL_ANALYSIS:
            self.log_analysis(cursor, sql, parameter_tuple)
            %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            %end
            record_list = [
                record_class(*row) 
                for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple)]
            %if constants.PROFILE_QUERIES:
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            %end
            
        return record_list
        
    def find_{{table_name}}_iter(
        self,
        %if constants.DEBUG:
        %for name in table_column_name_list:
        {{name}}=NA,
        %end
        %end
        order_by=(),
        limit=None,
        offset=None,
        where=None,
        **runtime_conditions):
        """ Finds {{table.__class__.__name__}} records by field values and iterates on them
        """
        %if constants.DEBUG:
        %for name in table_column_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end
        
        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{table_constant_prefix}}_TABLE_LIST,
            self.{{table_constant_prefix}}_FIELD_LIST,
            self.{{table_constant_prefix}}_CONDITION_MAP,
            {},
            [],
            self.{{table_constant_prefix}}_ORDER_BY_MAP,
            where,
            None,
            runtime_conditions,
            order_by,
            limit,
            offset)
        
        record_class = self.new_{{table_name}}
        %if constants.LOG_SQL_ANALYSIS:
        with self.cursor() as cursor:
            self.log_analysis(cursor, sql, parameter_tuple)
        %else:
        with self.cursor(named=True) as cursor:
        %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            row_list = list(self.execute_and_fetch_iter(cursor, sql, parameter_tuple))
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            for row in row_list:
                yield record_class(*row)
            %else:
            for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple):
                yield record_class(*row)
            %end
            
    def find_{{table_name}}_count(
        self,
        %if constants.DEBUG:
        %for name in table_column_name_list:
        {{name}}=NA,
        %end
        %end
        order_by=None,
        limit=None,
        offset=None,
        where=None,
        **runtime_conditions):
        """ Counds {{table.__class__.__name__}} records by field values

        The record ordering is ignored, but can be passed to provide
        compatible method signature with the actual find methods.
        
        """
        %if constants.DEBUG:
        %for name in table_column_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end

        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{table_constant_prefix}}_TABLE_LIST,
            ('COUNT(*)', ),
            self.{{table_constant_prefix}}_CONDITION_MAP,
            {},
            [],
            {},
            where,
            None,
            runtime_conditions,
            (),
            limit,
            offset)
        
        with self.cursor() as cursor:
            %if constants.LOG_SQL_ANALYSIS:
            self.log_analysis(cursor, sql, parameter_tuple)
            %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            %end
            row = self.execute_and_fetch_one(cursor, sql, parameter_tuple)
            %if constants.PROFILE_QUERIES:
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            %end
            
        return row[0]
        
    %else:
    %query = table
    %query_name = query._name
    %query_constant_prefix = '_%s' % query_name.upper()
    %query_table_list = query.get_table_list()
    %query_field_list = map(format.format_result, query._column_list)
    %query_group_by = map(format.format_expression, query._group_by)
    %query_order_by_map = format.format_query_order_by_map(query)
    %query_where_condition_map, query_having_condition_map = format.format_query_condition_map(query)
    %condition_name_list = [column.name for column in query._column_list + query._condition_list]
    {{query_constant_prefix}}_TABLE_LIST = [
        %for table_info in query_table_list:
        {{repr(table_info)}},
        %end
        ]
        
    {{query_constant_prefix}}_FIELD_LIST = [
        %for field_info in query_field_list:
        {{repr(field_info)}},
        %end
        ]
        
    {{query_constant_prefix}}_GROUP_BY = [
        %for field_info in query_group_by:
        {{repr(field_info)}},
        %end
        ]
        
    {{query_constant_prefix}}_ORDER_BY_MAP = {
        %for name, sql_expression in sorted(query_order_by_map.items()):
        {{repr(name)}}: {{repr(sql_expression)}},
        %end
        }
        
    {{query_constant_prefix}}_WHERE_CONDITION_MAP = {
        %for argument_name, condition_info in sorted(query_where_condition_map.items()):
        %suffix, formatting_function_name, value_expression = condition_info
        {{repr(argument_name)}}: ({{repr(suffix)}}, _format.{{formatting_function_name}}, {{repr(value_expression)}}),
        %end
        }
        
    {{query_constant_prefix}}_HAVING_CONDITION_MAP = {
        %for argument_name, condition_info in sorted(query_having_condition_map.items()):
        %suffix, formatting_function_name, value_expression = condition_info
        {{repr(argument_name)}}: ({{repr(suffix)}}, _format.{{formatting_function_name}}, {{repr(value_expression)}}),
        %end
        }
    
    def query_{{query_name}}_list(
        self,
        %if constants.DEBUG:
        %for name in condition_name_list:
        {{name}}=NA,
        %end
        %end
        order_by={{repr(query._order_by)}},
        limit=None,
        offset=None,
        where=None,
        having=None,
        **runtime_conditions):
        """ Executes the {{query.__class__.__name__}} query and returns the resulting records
        """
        %if constants.DEBUG:
        %for name in condition_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end
        
        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{query_constant_prefix}}_TABLE_LIST,
            self.{{query_constant_prefix}}_FIELD_LIST,
            self.{{query_constant_prefix}}_WHERE_CONDITION_MAP,
            self.{{query_constant_prefix}}_HAVING_CONDITION_MAP,
            self.{{query_constant_prefix}}_GROUP_BY,
            self.{{query_constant_prefix}}_ORDER_BY_MAP,
            where,
            having,
            runtime_conditions,
            order_by,
            limit,
            offset)
        
        record_class = self.new_{{query_name}}    
        with self.cursor() as cursor:
            %if constants.LOG_SQL_ANALYSIS:
            self.log_analysis(cursor, sql, parameter_tuple)
            %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            %end
            record_list = [
                record_class(*row) 
                for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple)]
            %if constants.PROFILE_QUERIES:
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            %end
            
        return record_list
        
    def query_{{query_name}}_iter(
        self,
        %if constants.DEBUG:
        %for name in condition_name_list:
        {{name}}=NA,
        %end
        %end
        order_by={{repr(query._order_by)}},
        limit=None,
        offset=None,
        where=None,
        having=None,
        **runtime_conditions):
        """ Executes the {{query.__class__.__name__}} query and iterates on the resulting records
        """
        %if constants.DEBUG:
        %for name in condition_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end
        
        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{query_constant_prefix}}_TABLE_LIST,
            self.{{query_constant_prefix}}_FIELD_LIST,
            self.{{query_constant_prefix}}_WHERE_CONDITION_MAP,
            self.{{query_constant_prefix}}_HAVING_CONDITION_MAP,
            self.{{query_constant_prefix}}_GROUP_BY,
            self.{{query_constant_prefix}}_ORDER_BY_MAP,
            where,
            having,
            runtime_conditions,
            order_by,
            limit,
            offset)
        
        record_class = self.new_{{query_name}}
        %if constants.LOG_SQL_ANALYSIS:
        with self.cursor() as cursor:
            self.log_analysis(cursor, sql, parameter_tuple)
        %else:
        with self.cursor(named=True) as cursor:
        %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            row_list = list(self.execute_and_fetch_iter(cursor, sql, parameter_tuple))
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            for row in row_list:
                yield record_class(*row)
            %else:
            for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple):
                yield record_class(*row)
            %end
            
    def query_{{query_name}}_count(
        self,
        %if constants.DEBUG:
        %for name in condition_name_list:
        {{name}}=NA,
        %end
        %end
        order_by=None,
        limit=None,
        offset=None,
        where=None,
        having=None,
        **runtime_conditions):
        """ Executes the {{query.__class__.__name__}} query and counts the resulting records

        The record ordering is ignored, but can be passed to provide
        compatible method signature with the actual query methods.
        
        """
        %if constants.DEBUG:
        %for name in condition_name_list:
        runtime_conditions[{{repr(name)}}] = {{name}}
        %end

        %end
        sql, parameter_tuple = self._format.format_query(
            self.{{query_constant_prefix}}_TABLE_LIST,
            ('COUNT(*)', ),
            self.{{query_constant_prefix}}_WHERE_CONDITION_MAP,
            self.{{query_constant_prefix}}_HAVING_CONDITION_MAP,
            self.{{query_constant_prefix}}_GROUP_BY,
            {},
            where,
            having,
            runtime_conditions,
            (),
            limit,
            offset)
        
        with self.cursor() as cursor:
            %if constants.LOG_SQL_ANALYSIS:
            self.log_analysis(cursor, sql, parameter_tuple)
            %end
            %if constants.PROFILE_QUERIES:
            start_time = time.time()
            %end
            row = self.execute_and_fetch_one(cursor, sql, parameter_tuple)
            %if constants.PROFILE_QUERIES:
            end_time = time.time()
            util.log('Query execution time: %dms', int((end_time - start_time) * 1000 + 0.5))
            %end
            
        return row[0]
        
    %end
    %end
    %if options.insert:
    ### Inserting into the database

    %for table in database._table_list:
    %if table._writable:
    %if table._primary_key:
    def add_{{table._name}}(self, record, generate_id=True):
        """ Inserts single {{table.__class__.__name__}} record into the {{table._name}} database table
        """
        self.add_record(self.new_{{table._name}}, record, generate_id, {{repr(table._primary_key.serial)}})
        
    def add_{{table._name}}_list(self, record_list, generate_id=True):
        """ Inserts multiple {{table.__class__.__name__}} records into the {{table._name}} database table
        """
        self.add_record_list(self.new_{{table._name}}, record_list, generate_id, {{repr(table._primary_key.serial)}})
        
    %end
    %end
    %end
    %end
    %if options.update:
    ### Updating into the database

    %for table in database._table_list:
    %if table._writable:
    %if table._primary_key:
    def update_{{table._name}}(self, record):
        """ Updates single {{table.__class__.__name__}} record into the {{table._name}} database table
        """
        self.update_record(self.new_{{table._name}}, record)
        
    def update_{{table._name}}_list(self, record_list):
        """ Updates multiple {{table.__class__.__name__}} records into the {{table._name}} database table
        """
        self.update_record_list(self.new_{{table._name}}, record_list)
        
    %end
    %end
    %end
    %end
    %if options.delete:
    ### Deleting from the database

    %for table in database._table_list:
    %if table._writable:
    %if table._primary_key:
    def delete_{{table._name}}(self, record_or_id):
        """ Deletes single {{table.__class__.__name__}} record from the {{table._name}} database table
        """
        self.delete_record(self.new_{{table._name}}, record_or_id)
        
    def delete_{{table._name}}_list(self, record_or_id_list):
        """ Deletes multiple {{table.__class__.__name__}} records from the {{table._name}} database table
        """
        self.delete_record_list(self.new_{{table._name}}, record_or_id_list)
        
    def truncate_{{table._name}}(self):
        """ Truncates table {{table._name}} in the database
        """
        statement_list = {{repr(format.format_truncate_table(table, database))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list)
        
    %end
    %end
    %end
    def truncate_all_tables(self):
        """ Truncates all the tables in the database
        
        {{'\n        '.join(table._name for table in reversed(database._table_list) if table._creatable and table._primary_key)}}
        
        """
        statement_list = {{repr(format.format_truncate_table_list(database._table_list, database))}}
        
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list)
            
    %end
    %if options.create or options.drop:
    ### Creating and dropping stored procedures
    
    %for procedure in database._procedure_list:
    %if options.create:
    def create_procedure_{{procedure.name}}(self, ignore_errors=False):
        """ Creates stored procedure {{table.__class__.__name__}} in the database
        """
        statement_list = {{repr(format.format_create_procedure(procedure))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
            
    %end
    %if options.drop:
    def drop_procedure_{{procedure.name}}(self, ignore_errors=False):
        """ Drops stored procedure {{table.__class__.__name__}} from the database
        """
        statement_list = {{repr(format.format_drop_procedure(procedure))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
        
    %end
    %end
    ### Creating and dropping all the stored procedures at once
    
    %if options.create:
    def create_all_procedures(self, ignore_errors=False):
        """ Creates all the stored procedures into the database
        
        {{'\n        '.join(procedure.name for procedure in database._procedure_list)}}
        
        """
        %for procedure in database._procedure_list:
        self.create_procedure_{{procedure.name}}(ignore_errors)
        %end
        
    %end
    %if options.drop:
    def drop_all_procedures(self, ignore_errors=False):
        """ Drops all the stored procedures from the database
        
        {{'\n        '.join(procedure.name for procedure in database._procedure_list)}}
        
        """
        %for procedure in reversed(database._procedure_list):
        self.drop_procedure_{{procedure.name}}(ignore_errors)
        %end
    
    %end
    ### Creating and dropping triggers
    
    %for table in database._table_list:
    %if table._creatable:
    %for trigger in table._trigger_list:
    %for procedure in database._procedure_list:
    %if options.create:
    def create_trigger_{{table._name}}_{{trigger.name}}(self, ignore_errors=False):
        """ Creates trigger {{table.__class__.__name__}}.{{trigger.name}} in the database
        """
        statement_list = {{repr(format.format_create_trigger(trigger))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
            
    %end
    %if options.drop:
    def drop_trigger_{{table._name}}_{{trigger.name}}(self, ignore_errors=False):
        """ Drops trigger {{table.__class__.__name__}}.{{trigger.name}} from the database
        """
        statement_list = {{repr(format.format_drop_trigger(trigger))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
        
    %end
    %end
    %end
    %end
    %end
    ### Creating and dropping the triggers of all the tables at once
    
    %if options.create:
    def create_all_triggers(self, ignore_errors=False):
        """ Creates the triggers for all the tables in the database
        
        {{'\n        '.join(table._name for table in database._table_list if table._creatable and table._trigger_list)}}
        
        """
        %for table in database._table_list:
        %if table._creatable:
        %for trigger in table._trigger_list:
        self.create_trigger_{{table._name}}_{{trigger.name}}(ignore_errors)
        %end
        %end
        %end
        
    %end
    %if options.drop:
    def drop_all_triggers(self, ignore_errors=False):
        """ Drops the triggers for all the tables from the database
        
        {{'\n        '.join(table._name for table in reversed(database._table_list) if table._creatable and table._trigger_list)}}
        
        """
        %for table in reversed(database._table_list):
        %if table._creatable:
        %for trigger in table._trigger_list:
        self.drop_trigger_{{table._name}}_{{trigger.name}}(ignore_errors)
        %end
        %end
        %end
        
    %end
    ### Creating and dropping tables

    %for table in database._table_list:
    %if table._creatable:
    %if options.create:
    def create_table_{{table._name}}(self, ignore_errors=False):
        """ Creates table {{table.__class__.__name__}} in the database
        """
        statement_list = {{repr(format.format_create_table(table, database))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
        
    %end
    %if options.drop:
    def drop_table_{{table._name}}(self, ignore_errors=False):
        """ Drops table {{table.__class__.__name__}} from the database
        """
        statement_list = {{repr(format.format_drop_table(table, database))}}
        with self.cursor() as cursor:
            self.execute_statement_list(cursor, statement_list, ignore_errors)
            
    %end
    %end
    %end
    ### Creating, dropping and truncating all the tables at once
    
    %if options.create:
    def create_all_tables(self, ignore_errors=False):
        """ Creates all the tables and views into the database
        
        {{'\n        '.join(table._name for table in database._table_list if table._creatable)}}
        
        """
        %for table in database._table_list:
        %if table._creatable:
        self.create_table_{{table._name}}(ignore_errors)
        %end
        %end
        
    %end
    %if options.drop:
    def drop_all_tables(self, ignore_errors=False):
        """ Drops all the tables and views from the database
        
        {{'\n        '.join(table._name for table in reversed(database._table_list) if table._creatable)}}
        
        """
        %for table in reversed(database._table_list):
        %if table._creatable:
        self.drop_table_{{table._name}}(ignore_errors)
        %end
        %end
        
    %end
    %if options.create:
    ### Creating languages
    
    def create_all_languages(self, ignore_errors=True):
        """ Creates all the procedural languages used in the procedures defined
        """
        %language_set = set(['plpgsql']) | set(procedure.language for procedure in database._procedure_list)
        %for language in sorted(language_set):
        self.create_language({{repr(language)}}, ignore_errors)
        %end

    %end
    ### Creating and dropping the database structure
    
    %if options.create:
    def create_structure(self, ignore_errors=False):
        """ Creates the whole database structure
        """
        self.create_all_languages()
        
        self.create_all_tables(ignore_errors)
        self.create_all_procedures(ignore_errors)
        self.create_all_triggers(ignore_errors)
        
    %end
    %if options.drop:
    def drop_structure(self, ignore_errors=False):
        """ Drops the whole database structure
        """
        self.drop_all_triggers(ignore_errors)
        self.drop_all_procedures(ignore_errors)
        self.drop_all_tables(ignore_errors)
        
    %end
    %end
