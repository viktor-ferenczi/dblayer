""" Base class for generated database abstraction layers
"""

import contextlib, itertools

import dblayer
from dblayer import constants, util
from dblayer.base import query

class DatabaseAbstraction(object):
    """ Base class for generated database abstraction layers
    """

    # Database backend module
    _backend = None    
    
    # Database connection
    connection = None
    
    # SQL statement for the savepoint set before each identity insert statement
    _SQL_IDENTITY_INSERT_SAVEPOINT = ''

    # SQL statement to release a savepoint after a successful identity insert
    _SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT = ''
    
    # SQL statement for rolling back to the savepoint after a failing identity insert statement
    _SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT = ''

    ### Connecting and disconnecting
    
    def connect(self, dsn):
        """ Connects to the database
        """
        assert self.connection is None, 'Already connected.'
        self.connection = self._backend.connect(dsn)
        self.named_cursor_counter = itertools.count()
        
    def close(self):
        """ Closes the database connection if any
        """
        if self.connection is None:
            return
        self.connection.close()
        self.connection = None
        self.named_cursor_counter = None
        
    def __del__(self):
        """ Try to close the connection when the object is deallocated, but silence all known errors
        """
        try:
            self.close()
        except self.Error:
            pass
        
    @property
    def connected(self):
        return self.connection is not None
    
    ### Database session context
    
    @contextlib.contextmanager
    def session(self, dsn):
        self.connect(dsn)
        try:
            yield
        finally:
            self.close()
            
    ### Transaction setup and helpers
    
    def enable_transactions(self, isolation_level=1):
        """ Enables database transactions and sets the isolation level
        """
        self.connection.set_isolation_level(isolation_level)
        self.connection.rollback()
        
    def disable_transactions(self):
        """ Disables transactions, switches back to auto commit mode
        """
        self.connection.rollback()
        self.connection.set_isolation_level(0)
        
    def commit(self):
        """ Commits the current transaction
        """
        self.connection.commit()
        
    def rollback(self):
        """ Rolls back the current transaction
        """
        self.connection.rollback()
        
    ### Transaction context
    
    @contextlib.contextmanager
    def transaction(self):
        """ Context manager to encapsualte a transaction
        """
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()
            
    ### Cursor context
    
    @contextlib.contextmanager
    def cursor(self, named=False):
        """ Context manager for a single database cursor
        """
        # See this forum thread on why we need to use named cursors as well as unnamed ones:
        # http://www.velocityreviews.com/forums/t649192-psycopg2-and-large-queries.html
        if named:
            cursor_name = 'cursor_%d' % self.named_cursor_counter.next()
            cursor = self.connection.cursor(cursor_name)
        else:
            cursor = self.connection.cursor()
            
        try:
            yield cursor
        finally:
            try:
                cursor.close()
            except self.Error:
                # Closing a cursor in a broken transaction state can cause an extra exception here.
                # It can be safely suppressed, since we already have an error condition anyway.
                pass

    ### Execute helpers
    
    def execute(self, cursor, sql, parameter_tuple=()):
        """ Executes a single SQL statement on the given cursor 
        
        NOTE: It is not suitable to retrieve a result set, since the cursor
              is closed right after executing the statement.
        
        """
        if constants.LOG_SQL_STATEMENTS:
            util.log('SQL statement: execute(%r, %r)', sql, parameter_tuple)
            
        cursor.execute(sql, parameter_tuple)
    
    def executemany(self, cursor, sql, parameter_tuple_list):
        """ Executes a single SQL statement on the given cursor for each parameter_tuple
        
        NOTE: It is not suitable to retrieve a result set, since the cursor
              is closed right after executing the statement.
        
        """
        if not parameter_tuple_list:
            return
        
        if constants.LOG_SQL_STATEMENTS:
            util.log('SQL statement: executemany(%r, %r)', sql, parameter_tuple_list)
            
        cursor.executemany(sql, parameter_tuple_list)
    
    def execute_statement_list(self, cursor, statement_list, ignore_errors=False):
        """ Executes a list of SQL statements on the given cursor 
        
        statement_list = [(sql, parameter_tuple)*]
        
        NOTE: It is not suitable to retrieve a result set, since the cursor
              is closed right after executing the statements.
        
        """
        if not statement_list:
            return
        
        if constants.LOG_SQL_STATEMENTS:
            util.log('SQL statement: execute_statement_list(%r)', statement_list)
            
        if ignore_errors:
            for sql, parameter_tuple in statement_list:
                try:
                    cursor.execute(self._backend.format_savepoint('execute_statement_list_ignoring_errors'))
                    cursor.execute(sql, parameter_tuple)
                except self._backend.ProgrammingError, reason:
                    if str(reason).startswith('syntax error'):
                        raise
                    cursor.execute(self._backend.format_rollback_to_savepoint('execute_statement_list_ignoring_errors'))
                else:
                    cursor.execute(self._backend.format_release_savepoint('execute_statement_list_ignoring_errors'))
        else:
            for sql, parameter_tuple in statement_list:
                cursor.execute(sql, parameter_tuple)
                
    def execute_and_fetch_one(self, cursor, sql, parameter_tuple=()):
        """ Executes an SQL query on the given cursor and returns the first result row if any
        
        Returns the first row of the result set or None in the case of an
        empty result set.
        
        """
        if constants.LOG_SQL_STATEMENTS:
            util.log('SQL statement: execute_and_fetch_one(%r, %r)', sql, parameter_tuple)
            
        cursor.execute(sql, parameter_tuple)
        row = cursor.fetchone()
        
        if constants.LOG_SQL_RESULT_ROWS:
            util.log('Returning SQL result row: %r' % (row, ))
            
        return row

    def execute_and_fetch_iter(self, cursor, sql, parameter_tuple=()):
        """ Executes an SQL query on the given cursor and yields each row from the result set
        """
        if constants.LOG_SQL_STATEMENTS:
            util.log('SQL statement: execute_and_fetch_iter(%r, %r)', sql, parameter_tuple)
            
        cursor.arraysize = constants.CURSOR_ARRAYSIZE
        cursor.execute(sql, parameter_tuple)
        
        while 1:
            row_list = cursor.fetchmany()
            
            if not row_list:
                break
            
            for row in row_list:
                
                if constants.LOG_SQL_RESULT_ROWS:
                    util.log('Yielding SQL result row: %r' % (row, ))
                    
                yield row
        
    ### Select query helpers
    
    def get_record(self, record_class, clauses, parameter_tuple=()):
        """ Retrieves a single record form the database or None if no record found
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            assert isinstance(clauses, dblayer.base.query.Clauses)
            assert isinstance(parameter_tuple, (tuple, list))
            
        sql = self._backend.format_select(clauses)
        
        with self.cursor() as cursor:
            row = self.execute_and_fetch_one(cursor, sql, parameter_tuple)
            
        if row is None:
            return None
        
        return record_class(*row)

    def get_record_list(self, record_class, clauses, parameter_tuple=()):
        """ Retrieves a list of records form the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            assert isinstance(clauses, dblayer.base.query.Clauses)
            assert isinstance(parameter_tuple, (tuple, list))
            
        sql = self._backend.format_select(clauses)
        
        with self.cursor() as cursor:
            return [
                record_class(*row)
                for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple)]
        
    def get_record_iter(self, record_class, clauses, parameter_tuple=()):
        """ Yields records retrieved form the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            assert isinstance(clauses, dblayer.base.query.Clauses)
            assert isinstance(parameter_tuple, (tuple, list))
            
        sql = self._backend.format_select(clauses)
        
        with self.cursor() as cursor:
            for row in self.execute_and_fetch_iter(cursor, sql, parameter_tuple):
                yield record_class(*row)
    
    ### Insert query helpers
    
    def add_record(self, record_class, record, generate_id=True):
        """ Inserts new record into the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            assert isinstance(record, record_class), 'Got record of unexpected type: %r' % (record, )
            
        record.finalize()
        
        table_name = record_class._table_name
        clauses = query.Clauses(
            table_list=(table_name, ),
            field_list=record_class._column_name_list)
        sql = self._backend.format_insert(clauses)

        with self.cursor() as cursor:
            
            # We might need to generate another ID, so the retry loop
            for retry in xrange(1, constants.MAX_INSERT_RETRY_COUNT):
                
                try:
                    if generate_id:
                        record.id = util.get_random_id()
                    elif constants.DEBUG:
                        assert record.id, 'No record ID specified with ID generation disabled: %r' % record
                        
                    parameter_tuple = record.tuple
                    
                    if self._SQL_IDENTITY_INSERT_SAVEPOINT:
                        self.execute(cursor, self._SQL_IDENTITY_INSERT_SAVEPOINT)
                    
                    self.execute(cursor, sql, parameter_tuple)
                    
                except self._backend.IntegrityError, reason:
                    
                    if not self._backend.is_primary_key_conflict(reason):
                        raise
                    
                    if self._SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT:
                        self.execute(cursor, self._SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT)
                    
                else:
                    if self._SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT:
                        self.execute(cursor, self._SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT)
                    
                    return
                
            # Reproduce the error, it will re-raise the exception
            self.execute(cursor, sql, parameter_tuple)

    def add_record_list(self, record_class, record_list, generate_id=True):
        """ Inserts a list of records of the same type into the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            
        if not record_list:
            return
        
        if len(record_list) == 1:
            self.add_record(record_class, record_list[0], generate_id)
            return
        
        for record in record_list:
            if constants.DEBUG:
                assert isinstance(record, record_class), 'Got record of unexpected type: %r' % (record, )
            record.finalize()
        
        table_name = record_class._table_name
        clauses = query.Clauses(
            table_list=(table_name, ),
            field_list=record_class._column_name_list)
        sql = self._backend.format_insert(clauses)
        
        with self.cursor() as cursor:
            
            # We need to catch conflicting ID values (rare, but possible)
            try:
                if generate_id:
                    for record in record_list:
                        record.id = util.get_random_id()
                elif constants.DEBUG:
                    for record in record_list:
                        assert record.id, 'No record ID specified with ID generation disabled: %r' % record
                    
                parameter_tuple_list = [record.tuple for record in record_list]
                
                if self._SQL_IDENTITY_INSERT_SAVEPOINT:
                    self.execute(cursor, self._SQL_IDENTITY_INSERT_SAVEPOINT)
                    
                self.executemany(cursor, sql, parameter_tuple_list)
                
            except self._backend.IntegrityError:
                
                if self._SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT:
                    self.execute(cursor, self._SQL_IDENTITY_INSERT_ROLLBACK_SAVEPOINT)
                    
                # Split the record list into two parts and try again
                split_index =  len(record_list) // 2
                self.add_record_list(record_class, record_list[:split_index], generate_id)
                self.add_record_list(record_class, record_list[split_index:], generate_id)
                
            else:
                if self._SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT:
                    self.execute(cursor, self._SQL_IDENTITY_INSERT_RELEASE_SAVEPOINT)
            
    ### Update query helpers
    
    def update_record(self, record_class, record):
        """ Updates a record already in the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            assert isinstance(record, record_class), 'Got record of unexpected type: %r' % (record, )
            assert record.id is not None, 'Cannot update record which has not been added to the database!'
            
        record.finalize()
        
        clauses = query.Clauses(
            table_list=(record_class._table_name, ),
            field_list=record_class._column_name_list[1:],
            where='id = ?')
        sql = self._backend.format_update(clauses)
        
        parameter_tuple = record.tuple[1:] + (record.id, )
        
        with self.cursor() as cursor:
            self.execute(cursor, sql, parameter_tuple)

    def update_record_list(self, record_class, record_list):
        """ Updates a list of records already in the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            
        if not record_list:
            return
        
        if len(record_list) == 1:
            self.update_record(record_class, record_list[0])
            return
        
        for record in record_list:
            if constants.DEBUG:
                assert isinstance(record, record_class), 'Got record of unexpected type: %r' % (record, )
                assert record.id is not None, 'Cannot update record which has not been added to the database!'
            record.finalize()
        
        clauses = query.Clauses(
            table_list=(record_class._table_name, ),
            field_list=record_class._column_name_list[1:],
            where='id = ?')
        sql = self._backend.format_update(clauses)
        
        parameter_tuple_list = [
            record.tuple[1:] + (record.id, )
            for record in record_list]
        
        with self.cursor() as cursor:
            self.executemany(cursor, sql, parameter_tuple_list)
            
    ### Delete query helpers
    
    def delete_record(self, record_class, record_or_id):
        """ Deletes a record from the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
            
        clauses = query.Clauses(
            table_list=(record_class._table_name, ),
            where='id = ?')
        sql = self._backend.format_delete(clauses)
        
        parameter_tuple = (record_or_id.id if isinstance(record_or_id, record_class) else record_or_id, )
        
        with self.cursor() as cursor:
            self.execute(cursor, sql, parameter_tuple)
            
    def delete_record_list(self, record_class, record_or_id_list):
        """ Deletes a list of records from the database
        """
        if constants.DEBUG:
            assert issubclass(record_class, dblayer.base.record.Record)
        
        clauses = query.Clauses(
            table_list=(record_class._table_name, ),
            where='id = ?')
        sql = self._backend.format_delete(clauses)
        
        parameter_tuple_list = [
            (record_or_id.id if isinstance(record_or_id, record_class) else record_or_id, )
            for record_or_id in record_or_id_list]
        
        with self.cursor() as cursor:
            self.executemany(cursor, sql, parameter_tuple_list)
            
    ### Create helpers
    
    def create_language(self, language, ignore_errors=True):
        """ Creates a procedural language
        """
        with self.cursor() as cursor:
            statement_list = [('CREATE LANGUAGE %s' % self._backend.quote_name(language), ())]
            self.execute_statement_list(cursor, statement_list, ignore_errors)
        
    ### Debug helpers
    
    if constants.LOG_SQL_ANALYSIS:
        def log_analysis(self, cursor, sql, parameter_tuple):
            cursor.execute('EXPLAIN ' + sql, parameter_tuple)
            util.log(
                'Analyzing query:\n'
                '%s\n'
                'With parameters: %r\n'
                '%s',
                sql,
                parameter_tuple,
                '\n'.join(row[0] for row in cursor.fetchall()))
