""" Functions formatting pieces of SQL statements
"""

import datetime

import dblayer
from dblayer import constants

NA = constants.NA

### Quoting and escaping

def quote_name(name):
    """ Quotes a table or field name for use in SQL statements
    """
    if constants.DEBUG:
        assert '"' not in name, 'Names must not contain double quotes!'
        assert '\\' not in repr(name), (
            'Names must not contain special characters which need to be escaped!')
    return '"%s"' % name

def quote_alias_name(name, alias):
    """ Quotes aliased table or field name for use in SQL statements
    """
    return '%s AS %s' % (quote_name(name), quote_name(alias))

def quote_table_column_name(table_name, column_name):
    """ Quotes a column reference
    """
    return '%s.%s' % (quote_name(table_name), quote_name(column_name))

def quote_literal_value(value):
    """ Quotes a literal value for use in SQL statements
    """
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        value = value.decode(constants.ENCODING)
    if isinstance(value, unicode):
        return u"E'%s'" % repr(value)[2: -1].replace("'", "''")
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime.time, datetime.date, datetime.datetime)):
        return repr(value.isoformat())
    if isinstance(value, datetime.timedelta):
        return repr('%d day %f sec' % (value.days, value.seconds + 1e-6 * value.microseconds))
    if isinstance(value, (tuple, list)):
        return '(%s)' % (', '.join(map(quote_literal_value, value)))
    raise ValueError('Cannot quote literal value: %r' % (value,))

### Handling of parameter placeholders

def replace_parameter_placeholders(sql):
    """ Replaces ? with the parameter placeholder acceptable by the database server
    """
    # NOTE: It does not replace inside string literals
    split_sql = sql.split("'")
    for i in xrange(0, len(split_sql), 2):
        split_sql[i] = split_sql[i].replace('?', '%s')
    return "'".join(split_sql)

### Formatting of column definitions based on the database model

def format_default_not_null(column, sql, parameter_list):
    """ Appends the DEFAULT and NOT NULL common type modifiers as needed
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.BaseColumn)
        
    if column.default is not None:
        if isinstance(column.default, dblayer.model.function.BaseFunction):
            sql.append('DEFAULT %s' % format_expression(column.default))
        else:
            sql.append('DEFAULT ?')
            parameter_list.append(column.default)
        
    if not column.null:
        sql.append('NOT NULL')
        
def format_custom_column(column):
    """ Returns column type definition for the given custom column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Custom)
        
    return (column.sql_type, ())

def format_primary_key_column(column):
    """ Returns column type definition for the given primary key column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.PrimaryKey)
        
    if column.serial:
        return ('BIGSERIAL PRIMARY KEY', ())
        
    return ('BIGINT NOT NULL', ())

def format_foreign_key_column(column):
    """ Returns column type definition for the given foreign key column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.ForeignKey)
        
    sql = ['BIGINT']
    parameter_list = []
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))

def format_boolean_column(column):
    """ Returns the column type definition for the given boolean column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Boolean)
        
    sql = ['BOOLEAN']
    parameter_list = []
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))

def format_integer_column(column):
    """ Returns the column type definition for the given integer column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Integer)
        
    sql = []
    parameter_list = []
    if not column.digits or column.digits <= 9:
        sql = ['INTEGER']
    elif column.digits <= 18:
        sql = ['BIGINT']
    else:
        sql = ['NUMERIC(%d)' % column.digits]
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))
    
def format_float_column(column):
    """ Returns the column type definition for the given float or double column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Float)
        
    sql = []
    parameter_list = []
    if column.double:
        sql = ['DOUBLE PRECISION']
    else:
        sql = ['REAL']
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))
    
def format_decimal_column(column):
    """ Returns the column type definition for the given decimal column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Decimal)
        
    sql = []
    parameter_list = []
    if column.precision is not None:
        if column.scale is not None:
            sql = ['NUMERIC(%d, %d)' % (column.precision, column.scale)]
        else:
            sql = ['NUMERIC(%d)' % column.precision]
    else:
        assert not column.scale
        sql = ['NUMERIC']
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))
    
def format_text_column(column):
    """ Returns the column type definition for the given boolean column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Text)
        
    sql = []
    parameter_list = []
    if column.maxlength:
        sql = ['VARCHAR(%d)' % column.maxlength]
    else:
        sql = ['TEXT']
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))

def format_date_column(column):
    """ Returns the column type definition for the given date column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Date)
        
    sql = ['DATE']
    parameter_list = []
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))

def format_datetime_column(column):
    """ Returns the column type definition for the given datetime column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.Datetime)
        
    sql = ['TIMESTAMP WITHOUT TIME ZONE']
    parameter_list = []
    format_default_not_null(column, sql, parameter_list)
    sql = ' '.join(sql)
    return (sql, tuple(parameter_list))

def format_search_document_column(column):
    """ Returns the column type definition for the given search document column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.SearchDocument)
        
    return ('tsvector', ())

def format_column(column):
    """ Returns the column type definition for the given column
    """
    if constants.DEBUG:
        assert isinstance(column, dblayer.model.column.BaseColumn)
        
    formatter = COLUMN_FORMATTER_MAP[column.abstract_sql_column_type]
    return formatter(column)

COLUMN_FORMATTER_MAP = dict(
    # Table columns
    Custom=format_custom_column,
    PrimaryKey=format_primary_key_column,
    ForeignKey=format_foreign_key_column,
    Boolean=format_boolean_column,
    Integer=format_integer_column,
    Float=format_float_column,
    Decimal=format_decimal_column,
    Text=format_text_column,
    Date=format_date_column,
    Datetime=format_datetime_column,
    SearchDocument=format_search_document_column,
)

### Formatting of index creation based on the database model

def format_create_btree_index(index):
    """ Returns the definition of a regular B-Tree based index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.BaseIndex)
        
    sql = 'CREATE INDEX %s ON %s USING btree(%s);' % (
        quote_name('%s_%s' % (index.table._name, index.name)),
        quote_name(index.table._name),
        ', '.join(quote_name(column.name) for column in index.columns))
    return [(sql, ())]

def format_create_full_text_search_index(index):
    """ Returns the definition of a full text search index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.FullTextSearchIndex)

    document_expression = " || ' ' || ".join(
        ("COALESCE(new.%s, '')" if column.null else "new.%s") % quote_name(column.name)
        for column in index.columns)
        
    variables = dict(
        table_name=quote_name(index.table._name),
        index_name=quote_name('%s_%s' % (index.table._name, index.name)),
        trigger_name=quote_name('%s_%s_update_trigger' % (index.table._name, index.name)),
        procedure_name=quote_name('fn_%s_%s_update_trigger' % (index.table._name, index.name)),
        search_document_column_name=quote_name(index.name[:-6]),
        document_expression=document_expression)
    
    create_procedure_sql = '''\
CREATE FUNCTION %(procedure_name)s () RETURNS trigger AS $$
BEGIN
  new.%(search_document_column_name)s := to_tsvector(%(document_expression)s);
  RETURN new;
END
$$ LANGUAGE plpgsql;''' % variables
    
    create_index_sql = '''\
CREATE INDEX %(index_name)s ON %(table_name)s \
USING gin(%(search_document_column_name)s);''' % variables
    
    create_trigger_sql = '''\
CREATE TRIGGER %(trigger_name)s \
BEFORE INSERT OR UPDATE \
ON %(table_name)s \
FOR EACH ROW \
EXECUTE PROCEDURE %(procedure_name)s ();''' % variables
    
    statements = [
        (create_procedure_sql, ()), 
        (create_index_sql, ()),
        (create_trigger_sql, ())]
    
    return statements

def format_create_index(index):
    """ Returns the definition of an index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.BaseIndex)
        
    formatter = CREATE_INDEX_FORMATTER_MAP[index.__class__.__name__]
    return formatter(index)

CREATE_INDEX_FORMATTER_MAP = dict(
    Index=format_create_btree_index,
    FullTextSearchIndex=format_create_full_text_search_index,
)

### Formatting of index removal based on the database model

def format_drop_btree_index(index):
    """ Returns the statements to drop a regular B-Tree based index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.BaseIndex)
        
    sql = 'DROP INDEX %s;' % quote_name('%s_%s' % (index.table._name, index.name))
    return [(sql, ())]

def format_drop_full_text_search_index(index):
    """ Returns the statements to drop a full text search index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.FullTextSearchIndex)
        
    table_name = quote_name(index.table._name)
    index_name = quote_name('%s_%s' % (index.table._name, index.name))
    trigger_name = quote_name('%s_%s_update_trigger' % (index.table._name, index.name))
    procedure_name = quote_name('fn_%s_%s_update_trigger' % (index.table._name, index.name))
    
    statements = [
        ('DROP TRIGGER %s ON %s;' % (trigger_name, table_name), ()),
        ('DROP INDEX %s;' % index_name, ()),
        ('DROP FUNCTION %s();' % procedure_name, ())]
    
    return statements

def format_drop_index(index):
    """ Returns the definition of an index
    """
    if constants.DEBUG:
        assert isinstance(index, dblayer.model.index.BaseIndex)
        
    formatter = DROP_INDEX_FORMATTER_MAP[index.__class__.__name__]
    return formatter(index)

DROP_INDEX_FORMATTER_MAP = dict(
    Index=format_drop_btree_index,
    FullTextSearchIndex=format_drop_full_text_search_index,
)

### Formatting of procedure definitions based on the database model

def format_create_procedure(procedure):
    """ Returns the list of tuples of SQL statements and parameter_tuple to create a stored procedure
    """
    if constants.DEBUG:
        assert isinstance(procedure, dblayer.model.procedure.BaseProcedure)
    
    variables = dict(
        name=quote_name(procedure.name),
        language=quote_name(procedure.language),
        argument_list=', '.join(procedure.argument_list),
        result=procedure.result,
        body=procedure.body)
    
    sql = '''\
CREATE FUNCTION %(name)s (%(argument_list)s) RETURNS %(result)s
BEGIN
  %(body)s
END
$$ LANGUAGE %(language)s;''' % variables
    
    return [(sql, ())]

def format_drop_procedure(procedure, cascade=False):
    """ Returns the list of tuples of SQL statements and parameter_tuple to drop a stored procedure
    """
    if constants.DEBUG:
        assert isinstance(procedure, dblayer.model.procedure.BaseProcedure)
        
    sql = 'DROP FUNCTION %s (%s);' % (
        quote_name(procedure.name),
        ', '.join(procedure.argument_list))
    return [(sql, ())]

### Formatting of trigger definitions based on the database model

def format_create_trigger(trigger):
    """ Returns the list of tuples of SQL statements and parameter_tuple to create a trigger
    """
    if constants.DEBUG:
        assert isinstance(trigger, dblayer.model.trigger.BaseTrigger)
    
    timing, event, scope = TRIGGER_FORMATTER_MAP[trigger.__class__.__name__]
    
    variables = dict(
        trigger_name=quote_name(trigger.name),
        timing=timing,
        event=event,
        scope=scope,
        table_name=quote_name(trigger.table._name),
        procedure_name=quote_name(trigger.procedure_name),
        procedure_parameters=', '.join(map(format_expression, trigger.procedure_parameters)))
    
    sql = '''\
CREATE TRIGGER %(trigger_name)s \
%(timing)s %(event)s \
ON %(table_name)s \
FOR EACH %(scope)s \
EXECUTE PROCEDURE %(procedure_name)s (%(procedure_parameters)s);''' % variables
    return [(sql, ())]

TRIGGER_FORMATTER_MAP = dict(
    BeforeInsertRow=('BEFORE', 'INSERT', 'ROW'),
    BeforeUpdateRow=('BEFORE', 'UPDATE', 'ROW'),
    BeforeInsertOrUpdateRow=('BEFORE', 'INSERT OR UPDATE', 'ROW'),
    BeforeDeleteRow=('BEFORE', 'DELETE', 'ROW'),
    BeforeInsertStatement=('BEFORE', 'INSERT', 'STATEMENT'),
    BeforeUpdateStatement=('BEFORE', 'UPDATE', 'STATEMENT'),
    BeforeInsertOrUpdateStatement=('BEFORE', 'INSERT OR UPDATE', 'STATEMENT'),
    BeforeDeleteStatement=('BEFORE', 'DELETE', 'STATEMENT'),
    AfterInsertRow=('AFTER', 'INSERT', 'ROW'),
    AfterUpdateRow=('AFTER', 'UPDATE', 'ROW'),
    AfterInsertOrUpdateRow=('AFTER', 'INSERT OR UPDATE', 'ROW'),
    AfterDeleteRow=('AFTER', 'DELETE', 'ROW'),
    AfterInsertStatement=('AFTER', 'INSERT', 'STATEMENT'),
    AfterUpdateStatement=('AFTER', 'UPDATE', 'STATEMENT'),
    AfterInsertOrUpdateStatement=('AFTER', 'INSERT OR UPDATE', 'STATEMENT'),
    AfterDeleteStatement=('AFTER', 'DELETE', 'STATEMENT'),
)

def format_drop_trigger(trigger, cascade=False):
    """ Returns the list of tuples of SQL statements and parameter_tuple to drop a trigger
    """
    if constants.DEBUG:
        assert isinstance(trigger, dblayer.model.trigger.BaseTrigger)
        
    sql = 'DROP TRIGGER %s ON %s%s;' % (
        quote_name(trigger.name),
        quote_name(trigger.table._name),
        ' CASCADE' if cascade else '')
    return [(sql, ())]

### Formatting of constraint definitions based on the database model

def format_primary_key_constraint(constraint):
    """ Returns the definition of a primary key constraint
    """
    if constants.DEBUG:
        assert isinstance(constraint, dblayer.model.constraint.PrimaryKey)
    
    sql = 'PRIMARY KEY (%s)' % (', '.join(quote_name(column.name) for column in constraint.columns))
    return (sql, ())
    
def format_foreign_key_constraint(constraint):
    """ Returns the definition of a foreign key constraint
    """
    if constants.DEBUG:
        assert isinstance(constraint, dblayer.model.constraint.ForeignKey)
        
    table = constraint.table
    
    # NOTE: We need to look up the cloned foreign key column by name to get the right object here
    fk_column_name = constraint.columns[0].name
    fk_column = getattr(table, fk_column_name)
    
    referenced_table = fk_column.referenced_table
    assert referenced_table, 'The referenced table for foreign key column %s.%s has not been determined!' % (fk_column.table_class.__name__, fk_column.name)
    pk_column = referenced_table._primary_key
    assert pk_column, 'Referenced table %r does not have a primary key column!' % referenced_table._name
    sql = (
        'FOREIGN KEY (%s) REFERENCES %s (%s) MATCH SIMPLE '
        'ON UPDATE NO ACTION ON DELETE NO ACTION' % (
            quote_name(fk_column.name), 
            quote_name(referenced_table._name), 
            quote_name(pk_column.name)))
    return (sql, ())
    
def format_unique_constraint(constraint):
    """ Returns the definition of a unique index constraint
    """
    if constants.DEBUG:
        assert isinstance(constraint, dblayer.model.constraint.Unique)
        
    sql = 'UNIQUE (%s)' % (', '.join(quote_name(column.name) for column in constraint.columns))
    return (sql, ())

def format_check_constraint(constraint):
    """ Returns the definition of a check constraint
    """
    if constants.DEBUG:
        assert isinstance(constraint, dblayer.model.constraint.Check)
        
    sql = 'CHECK %s' % format_expression(constraint.expression)
    return (sql, ())

def format_constraint(constraint):
    """ Returns the definition of a constraint
    """
    if constants.DEBUG:
        assert isinstance(constraint, dblayer.model.constraint.BaseConstraint)
    
    formatter = CONSTRAINT_FORMATTER_MAP[constraint.__class__.__name__]
    return formatter(constraint)

CONSTRAINT_FORMATTER_MAP = dict(
    PrimaryKey=format_primary_key_constraint,
    ForeignKey=format_foreign_key_constraint,
    Unique=format_unique_constraint,
    Check=format_check_constraint,
)

### Formatting of SQL statements based on the database model

def format_create_table(table, database):
    """ Returns the list of tuples of SQL statements and parameter_tuple to create a table
    """
    if constants.DEBUG:
        assert isinstance(table, dblayer.model.table.Table)
    
    quoted_table_name = quote_name(table._name)
    
    definition_list = []
    parameter_list = []
    
    # Collect list of column definitions
    for column in table._column_list:
        definition, new_parameters = format_column(column)
        definition_list.append(
            '%s %s' % (
                quote_name(column.name),
                definition))
        parameter_list.extend(new_parameters)
        
    # Collect list of constraint definitions
    for constaint in table._constraint_list:
        definition, new_parameters = format_constraint(constaint)
        definition_list.append(
            'CONSTRAINT %s %s' % (
                quote_name('%s__%s' % (table._name, constaint.name)),
                definition))
        parameter_list.extend(new_parameters)

    # SQL CREATE TABLE statement and parameter_tuple
    sql = 'CREATE TABLE %s (\n%s) WITH (OIDS=FALSE);' % (
        quote_name(table._name), ',\n'.join(definition_list))
    statements = [(sql, tuple(parameter_list))]
    
    # Format CREATE INDEX statements
    for index in table._index_list:
        statements.extend(format_create_index(index))
        
    # Prepare the parameter placeholders
    statements = [
        (replace_parameter_placeholders(sql), parameter_tuple)
        for sql, parameter_tuple in statements]
    
    return statements

def format_drop_table(table, database, cascade=False):
    """ Returns the list of tuples of SQL statements and parameter_tuple to drop a table
    """
    if constants.DEBUG:
        assert isinstance(table, dblayer.model.table.Table)

    statements = []
    
    # Format DROP INDEX statements
    for index in table._index_list:
        statements.extend(format_drop_index(index))
    
    # Format statements to drop the table
    sql = 'DROP TABLE %s%s;' % (
        quote_name(table._name),
        ' CASCADE' if cascade else '')
    statements.append((sql, ()))
    
    return statements
    
def format_truncate_table(table, database):
    """ Returns the list of tuples of SQL statements and parameter_tuple to truncate a table
    """
    if constants.DEBUG:
        assert isinstance(table, dblayer.model.table.Table)
        
    return [('TRUNCATE TABLE %s' % quote_name(table._name), ())]

def format_truncate_table_list(table_list, database):
    """ Returns the list of tuples of SQL statements and parameter_tuple to truncate multiple tables
    """
    if constants.DEBUG:
        for table in table_list:
            assert isinstance(table, dblayer.model.table.Table)
        
    return [('TRUNCATE TABLE %s' % ', '.join(quote_name(table._name) for table in table_list if table._writable), ())]

### Formatting SQL statements runtime (no database model available, only the record classes)

def format_cross_join_group_list(clauses):
    """ Formats the FROM clause of SQL SELECT statements
    
    Considers JOINs of all supported kind. Returns list of cross joined groups.
    
    """
    if constants.DEBUG:
        assert isinstance(clauses, dblayer.backend.base.clauses.Clauses)
        
    from_list = []
    join_group = []
    
    for source in clauses.table_list:
        
        if isinstance(source, basestring):
            # Single table, no aliases
            if join_group:
                from_list.append(' '.join(join_group))
            join_group = [quote_name(source)]
            
        elif len(source) == 2:
            # New cross join group
            table_name, alias_name = source
            if join_group:
                from_list.append(' '.join(join_group))
            join_group = [quote_alias_name(table_name, alias_name)]
            
        else:
            # Join to an existing cross join group
            (table_name,
             alias_name,
             join_type,
             id_field_name,
             referer_table_name,
             fk_field_name) = source
            
            if constants.DEBUG:
                assert join_type in constants.JOIN_TYPES, 'Unknown join type: %r' % (join_type, )
                assert join_group, 'Trying to append a new join to an empty cross join group!'
                
            join_group.append('%s %s ON %s = %s' % (
                join_type,
                quote_alias_name(table_name, alias_name),
                quote_table_column_name(table_name, id_field_name),
                quote_table_column_name(referer_table_name, fk_field_name)))
            
    from_list.append(' '.join(join_group))
        
    return from_list

def format_select(clauses, cache={}):
    """ Formats a SELECT SQL statement with the given clauses
    
    Returns (sql, parameter_tuple)
    
    """
    if constants.DEBUG:
        assert isinstance(clauses, dblayer.backend.base.clauses.Clauses)
        assert clauses.field_list, 'SQL SELECT statements must have a field list!'
        assert clauses.table_list, 'SQL SELECT statements must have source table(s) to select from!'
        
    sql = cache.get(clauses)
    if sql is not None:
        return sql
    
    cross_join_group_list = format_cross_join_group_list(clauses)
    
    sql = [
        'SELECT',
        ', '.join(clauses.field_list),
        'FROM',
        ', '.join(cross_join_group_list)]
    
    if clauses.where:
        sql.extend(('WHERE', clauses.where))

    if clauses.group_by:
        sql.extend(('GROUP BY', ', '.join(clauses.group_by)))

    if clauses.having:
        sql.extend(('HAVING', clauses.having))

    if clauses.order_by:
        sql.extend(('ORDER BY', ', '.join(clauses.order_by)))
        
    if clauses.limit:
        sql.extend(('LIMIT', str(clauses.limit)))
        
    if clauses.offset:
        sql.extend(('OFFSET', str(clauses.offset)))
        
    sql = ' '.join(sql)
    sql = replace_parameter_placeholders(sql)
    
    cache[clauses] = sql
    return sql + ';'

def format_insert(clauses, cache={}):
    """ Formats a INSERT SQL statement with the given clauses
    
    Returns (sql, parameter_tuple)
    
    """
    if constants.DEBUG:
        assert isinstance(clauses, dblayer.backend.base.clauses.Clauses)
        assert clauses.field_list, 'SQL INSERT statements must have a field list!'
        assert not clauses.where, 'SQL INSERT statements do not have a where clause!'
        assert not clauses.group_by, 'SQL INSERT statements do not have a group_by clause!'
        assert not clauses.having, 'SQL INSERT statements do not have a having clause!'
        assert not clauses.order_by, 'SQL INSERT statements do not have an order_by clause!'
        assert not clauses.limit, 'SQL INSERT statements do not have a limit clause!'
        assert not clauses.offset, 'SQL INSERT statements do not have an offset clause!'
        assert len(clauses.table_list) == 1, 'SQL INSERT statements can only work on a single table!'
        
    sql = cache.get(clauses)
    if sql is not None:
        return sql
    
    sql = [
        'INSERT INTO',
        quote_name(clauses.table_list[0]),
        '(%s)' % ', '.join(map(quote_name, clauses.field_list)),
        'VALUES',
        '(%s)' % ', '.join('?' * len(clauses.field_list))]
    
    sql = ' '.join(sql) + ';'
    sql = replace_parameter_placeholders(sql)
    
    cache[clauses] = sql
    return sql

def format_update(clauses, cache={}):
    """ Formats a UPDATE SQL statement with the given clauses
    
    Returns (sql, parameter_tuple)
    
    """
    if constants.DEBUG:
        assert isinstance(clauses, dblayer.backend.base.clauses.Clauses)
        assert clauses.field_list, 'SQL UPDATE statements must have a field list!'
        assert clauses.where, 'SQL UPDATE statements should have a where clause! (Otherwise they would be dangerous.) Use a TRUE condition if you intentionally want to update all the records.'
        assert not clauses.group_by, 'SQL UPDATE statements do not have a group_by clause!'
        assert not clauses.having, 'SQL UPDATE statements do not have a having clause!'
        assert not clauses.order_by, 'SQL UPDATE statements do not have an order_by clause!'
        assert not clauses.limit, 'SQL UPDATE statements do not have a limit clause!'
        assert not clauses.offset, 'SQL UPDATE statements do not have an offset clause!'
        assert len(clauses.table_list) == 1, 'SQL UPDATE statements can only work on a single table!'
        
    sql = cache.get(clauses)
    if sql is not None:
        return sql
    
    sql = [
        'UPDATE',
        quote_name(clauses.table_list[0]),
        'SET',
        ', '.join('%s=?' % quote_name(name) for name in clauses.field_list),
        'WHERE',
        clauses.where]

    sql = ' '.join(sql) + ';'
    sql = replace_parameter_placeholders(sql)
    
    cache[clauses] = sql
    return sql

def format_delete(clauses, cache={}):
    """ Formats a DELETE SQL statement with the given clauses
    
    Returns (sql, parameter_tuple)
    
    """
    if constants.DEBUG:
        assert isinstance(clauses, dblayer.backend.base.clauses.Clauses)
        assert not clauses.field_list, 'SQL DELETE statements do not have a field list!'
        assert clauses.where, 'SQL DELETE statements should have a where clause! (Otherwise they would be dangerous.) Use a TRUE condition if you intentionally want to delete all the records or truncate the table instead.'
        assert not clauses.group_by, 'SQL DELETE statements do not have a group_by clause!'
        assert not clauses.having, 'SQL DELETE statements do not have a having clause!'
        assert not clauses.order_by, 'SQL DELETE statements do not have an order_by clause!'
        assert not clauses.limit, 'SQL DELETE statements do not have a limit clause!'
        assert not clauses.offset, 'SQL DELETE statements do not have an offset clause!'
        assert len(clauses.table_list) == 1, 'SQL DELETE statements can only work on a single table!'
        
    sql = cache.get(clauses)
    if sql is not None:
        return sql
    
    sql = [
        'DELETE FROM',
        quote_name(clauses.table_list[0]),
        'WHERE',
        clauses.where]

    sql = ' '.join(sql) + ';'
    sql = replace_parameter_placeholders(sql)
    
    cache[clauses] = sql
    return sql

### Savepoints

# NOTE: These functions should return None for SQL servers not needing
# a savepoint to restore from an aborted transaction after failing queries

def format_savepoint(name):
    """ Formats a SAVEPOINT statement to preserve the current transaction state
    """
    return 'SAVEPOINT ' + quote_name(name);

def format_release_savepoint(name):
    """ Formats a RELEASE SAVEPOINT statement to free up resources for a
    savepoint we don't want to rollback to anymore
    
    """
    return 'RELEASE SAVEPOINT ' + quote_name(name);

def format_rollback_to_savepoint(name):
    """ Formats a ROLLBACK TO SAVEPOINT statement to roll back the
    current transaction to a previous savepoint
    
    """
    return 'ROLLBACK TO SAVEPOINT ' + quote_name(name);

### Query results

def format_result(result):
    """ Formats a query result expression and defines its alias name
    """
    if constants.DEBUG:
        assert isinstance(result, dblayer.model.query.BaseQueryResult)
        
    return '%s AS %s' % (format_expression(result.expression), quote_name(result.name))

### Functions and aggregates

def format_custom_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1

    return function.args[0]
    
def format_var_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1

    return quote_name(function.args[0])
    
def format_not_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return '(NOT %s)' % format_expression(function.args[0])

def format_and_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 0
    
    return '(%s)' % ' AND '.join(map(format_expression, function.args))

def format_or_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 0
    
    return '(%s)' % ' OR '.join(map(format_expression, function.args))

def format_equal_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s = %s)' % tuple(map(format_expression, function.args))

def format_not_equal_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s <> %s)' % tuple(map(format_expression, function.args))

def format_less_than_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s < %s)' % tuple(map(format_expression, function.args))

def format_less_than_or_equal_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s <= %s)' % tuple(map(format_expression, function.args))

def format_greater_than_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s > %s)' % tuple(map(format_expression, function.args))

def format_greater_than_or_equal_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s >= %s)' % tuple(map(format_expression, function.args))

def format_in_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    a, b = function.args
    b = tuple(b)
    if b:
        return '(%s IN %s)' % (format_expression(a), quote_literal_value(b))
    return 'FALSE'

def format_not_in_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    a, b = function.args
    b = tuple(b)
    if b:
        return '(%s NOT IN %s)' % (format_expression(a), quote_literal_value(b))
    return 'TRUE'

def format_neg_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return '(-%s)' % format_expression(function.args[0])

def format_add_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 1
    
    return '(%s)' % ' + '.join(map(format_expression, function.args))

def format_sub_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 1
    
    return '(%s)' % ' - '.join(map(format_expression, function.args))

def format_mul_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 1
    
    return '(%s)' % ' * '.join(map(format_expression, function.args))

def format_div_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 1
    
    return '(%s)' % ' / '.join(map(format_expression, function.args))

def format_concat_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 0
    
    return 'CONCAT(%s)' % ', '.join(map(format_expression, function.args))

def format_left_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return 'LEFT(%s, %s)' % tuple(map(format_expression, function.args))

def format_right_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return 'RIGHT(%s, %s)' % tuple(map(format_expression, function.args))

def format_substring_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert 2 <= len(function.args) <= 3
    
    return 'SUBSTR(%s)' % ', '.join(map(format_expression, function.args))

def format_contains_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(STRPOS(%s, %s) > 0)' % tuple(map(format_expression, function.args))

def format_like_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s LIKE %s)' % tuple(map(format_expression, function.args))

def format_not_like_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s NOT LIKE %s)' % tuple(map(format_expression, function.args))

def format_match_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s ~ %s)' % tuple(map(format_expression, function.args))

def format_not_match_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
    
    return '(%s !~ %s)' % tuple(map(format_expression, function.args))

def format_full_text_search_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 2
        
    return '(%s @@ %s)' % tuple(map(format_expression, function.args))

def format_coalesce_function(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) > 0
    
    return 'COALESCE(%s)' % ', '.join(tuple(map(format_expression, function.args)))

def format_count_aggregate(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return 'COUNT(%s)' % format_expression(function.args[0])

def format_min_aggregate(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return 'MIN(%s)' % format_expression(function.args[0])

def format_max_aggregate(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return 'MAX(%s)' % format_expression(function.args[0])

def format_sum_aggregate(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return 'SUM(%s)' % format_expression(function.args[0])

def format_avg_aggregate(function):
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
        assert len(function.args) == 1
    
    return 'AVG(%s)' % format_expression(function.args[0])

def format_function(function):
    """ Formats an SQL function or aggregate
    """
    if constants.DEBUG:
        assert isinstance(function, dblayer.model.function.BaseFunction)
    
    formatter = FUNCTION_FORMATTER_MAP[function.__class__.__name__]
    return formatter(function)
    
FUNCTION_FORMATTER_MAP = dict(
    
    # Functions
    Custom=format_custom_function,
    Var=format_var_function,
    Not=format_not_function,
    And=format_and_function,
    Or=format_or_function,
    Equal=format_equal_function,
    NotEqual=format_not_equal_function,
    LessThan=format_less_than_function,
    LessThanOrEqual=format_less_than_or_equal_function,
    GreaterThan=format_greater_than_function,
    GreaterThanOrEqual=format_greater_than_or_equal_function,
    In=format_in_function,
    NotIn=format_not_in_function,
    Neg=format_neg_function,
    Add=format_add_function,
    Sub=format_sub_function,
    Mul=format_mul_function,
    Div=format_div_function,
    Concat=format_concat_function,
    Left=format_left_function,
    Right=format_right_function,
    Substring=format_substring_function,
    Contains=format_contains_function,
    Like=format_like_function,
    NotLike=format_not_like_function,
    Match=format_match_function,
    NotMatch=format_not_match_function,
    FullTextSearch=format_full_text_search_function,
    Coalesce=format_coalesce_function,
    
    # Aggregates
    Count=format_count_aggregate,
    Min=format_min_aggregate,
    Max=format_max_aggregate,
    Sum=format_sum_aggregate,
    Avg=format_avg_aggregate,
)

def format_expression(expression):
    """ Formats an expression, which is either a column reference of a function
    """
    if isinstance(expression, dblayer.model.column.BaseColumn):
        if expression.table is None:
            return quote_name(expression.name)
        return quote_table_column_name(expression.table._name, expression.name)
    
    if isinstance(expression, dblayer.model.index.FullTextSearchIndex):
        return quote_table_column_name(expression.table._name, expression.name + '_document')
    
    if isinstance(expression, dblayer.model.function.BaseFunction):
        return format_function(expression)
    
    return quote_literal_value(expression)

### Formatting query conditions at runtime

def format_eq_condition(sql_expression, value):
    if value is None:
        return ('%s IS NULL' % sql_expression, ())
    return ('%s = ?' % sql_expression, (value, ))

def format_ne_condition(sql_expression, value):
    if value is None:
        return ('%s IS NOT NULL' % sql_expression, ())
    return ('%s <> ?' % sql_expression, (value, ))

def format_gt_condition(sql_expression, value):
    return ('%s > ?' % sql_expression, (value, ))

def format_lt_condition(sql_expression, value):
    return ('%s < ?' % sql_expression, (value, ))

def format_ge_condition(sql_expression, value):
    return ('%s >= ?' % sql_expression, (value, ))

def format_le_condition(sql_expression, value):
    return ('%s <= ?' % sql_expression, (value, ))

def format_range_condition(sql_expression, value):
    # NOTE: It can only be used with an sql_expression without parameter placeholders.
    lower_limit, upper_limit = value
    return (
        '%s >= ? AND %s < ?' % (sql_expression, sql_expression),
        (lower_limit, upper_limit))

def format_not_in_range_condition(sql_expression, value):
    # NOTE: It can only be used with an sql_expression without parameter placeholders.
    lower_limit, upper_limit = value
    return (
        '(%s < ? OR %s > ?)' % (sql_expression, sql_expression),
        (lower_limit, upper_limit))

def format_in_condition(sql_expression, value):
    value = tuple(value)
    if not value:
        return ('FALSE', ())
    return ('%s IN ?' % sql_expression, (value, ))

def format_not_in_condition(sql_expression, value):
    value = tuple(value)
    if not value:
        return ('TRUE', ())
    return ('%s NOT IN ?' % sql_expression, (value, ))

def format_like_condition(sql_expression, value):
    return ('%s LIKE ?' % sql_expression, (value, ))

def format_not_like_condition(sql_expression, value):
    return ('%s NOT LIKE ?' % sql_expression, (value, ))

def format_similar_to_condition(sql_expression, value):
    return ('%s SIMILAR TO ?' % sql_expression, (value, ))

def format_not_similar_to_condition(sql_expression, value):
    return ('%s NOT SIMILAR TO ?' % sql_expression, (value, ))

def format_match_condition(sql_expression, value):
    return ('%s ~ ?' % sql_expression, (value, ))

def format_not_match_condition(sql_expression, value):
    return ('%s !~ ?' % sql_expression, (value, ))

def format_search_condition(sql_expression, value):
    return ('%s @@ plainto_tsquery(?)' % sql_expression, (value, ))

# Keyword argument suffixes, SQL template and function to prepare 
# the expressions passed as parameters
QUERY_CONDITION_OPERATOR_LIST = (
    ('', format_eq_condition),
    ('_eq', format_eq_condition),
    ('_ne', format_ne_condition),
    ('_gt', format_gt_condition),
    ('_lt', format_lt_condition),
    ('_ge', format_ge_condition),
    ('_le', format_le_condition),
    ('_range', format_range_condition),
    ('_not_in_range', format_not_in_range_condition),
    ('_in', format_in_condition),
    ('_not_in', format_not_in_condition),
    ('_like', format_like_condition), 
    ('_not_like', format_not_like_condition), 
    ('_similar_to', format_similar_to_condition), 
    ('_not_similar_to', format_not_similar_to_condition), 
    ('_match', format_match_condition),
    ('_not_match', format_not_match_condition),
    ('_search', format_search_condition),
)

### Formatting query conditions

def format_query_condition(condition):
    """ Returns formatted query condition
    
    Yields (argument_name, (suffix, formatting_function_name, value_expression))
    tuples for each operator.
    
    """
    if 0:
        assert isinstance(condition, dblayer.model.query.Result)
        
    value_expression = format_expression(condition.expression)
    
    # Equals to a given value
    for suffix, formatting_function in QUERY_CONDITION_OPERATOR_LIST:
        
        # Name of the argument for this operator
        argument_name = condition.name + suffix
        
        # Yield SQL argument name and SQL expression for this operator
        yield (argument_name, (suffix, formatting_function.func_name, value_expression))
        
def format_query_condition_map(query):
    """ Returns dictionaries mapping names of all possible query conditions
    to the formatted SQL condition and parameter_tuple
    
    Returns (where_condition_map, having_condition_map)
    
    The returned dictionaries map all possible keyword argument names to
    (suffix, sql_expression, prepare_value_function_name) tuples.
    
    """
    # Process all the query conditions
    where_condition_map = {}
    having_condition_map = {}
    for condition in query._column_list + query._condition_list:
        
        # Select the appropriate condition map
        if condition.after_group_by:
            condition_map = having_condition_map
        else:
            condition_map = where_condition_map
            
        # Append each possible operator for this condition
        condition_map.update(format_query_condition(condition))
        
    return (where_condition_map, having_condition_map)

def format_query_order_by_map(query):
    """ Returns a dictionary mapping all the possible ascending and descending 
    order by fields to their SQL expression
    """
    order_by_map = {}
    for query_result in query._column_list + query._condition_list:
        name = query_result.name
        sql_expression = format_expression(query_result.expression)
        order_by_map[name] = sql_expression
        order_by_map['+' + name] = sql_expression
        order_by_map['-' + name] = sql_expression + ' DESC'
    return order_by_map

def format_table_column_condition(column):
    """ Returns formatted table column condition
    
    Yields (argument_name, (suffix, formatting_function_name, value_expression))
    tuples for each operator.
    
    """
    if 0:
        assert isinstance(condition, dblayer.model.query.BaseColumn)
        
    value_expression = format_expression(column)
    
    # Equals to a given value
    for suffix, formatting_function in QUERY_CONDITION_OPERATOR_LIST:
        
        # Name of the argument for this operator
        argument_name = column.name + suffix
        
        # Yield SQL argument name and SQL expression for this operator
        yield (argument_name, (suffix, formatting_function.func_name, value_expression))

def format_table_condition_map(table):
    """ Returns dictionary mapping names of all possible column conditions
    to the formatted SQL condition and parameter_tuple
    
    The returned dictionary maps all possible keyword argument names to
    (suffix, sql_expression, prepare_value_function_name) tuples.
    
    """
    # Process all the query conditions
    condition_map = {}
    for column in table._column_list:
        # Append each possible operator for this condition
        condition_map.update(format_table_column_condition(column))
        
    return condition_map

def format_table_order_by_map(table):
    """ Returns a dictionary mapping all the possible ascending and descending 
    order by columns to their SQL expression
    """
    order_by_map = {}
    for column in table._column_list:
        name = column.name
        sql_expression = format_expression(column)
        order_by_map[name] = sql_expression
        order_by_map['+' + name] = sql_expression
        order_by_map['-' + name] = sql_expression + ' DESC'
    return order_by_map

### Formatting queries at runtime

def format_query(
    table_list,
    field_list,
    where_condition_map,
    having_condition_map,
    group_by,
    order_by_map,
    runtime_where_condition,
    runtime_having_condition,
    runtime_conditions,
    order_by,
    limit,
    offset):
    """ Formats a SELECT SQL statement at runtime for the actual conditions
    
    Returns (sql, parameter_tuple)
    
    Partial caching is done by the format_select function, but the evaluation
    of the dynamic conditions cannot be cached. The actual runtime conditions
    are the keywords parameters passed to the query method on the database
    abstraction layer.
    
    Passing a tuple in the order_by parameter defines the record ordering.
    Values can be condition names. Use '-' prefix for descending sort.
    You can also use a redundant '+' prefix for ascending short.
    
    """
    if constants.DEBUG:
        assert isinstance(runtime_conditions, dict)
        assert isinstance(where_condition_map, dict)
        assert isinstance(having_condition_map, dict)
        assert isinstance(group_by, (tuple, list))
        
    # Parse keyword parameters (actual runtime conditions) passed
    where = []
    having = []
    parameter_list = []
    for name, value in runtime_conditions.iteritems():
        
        if value is NA:
            continue

        # Find the condition by name
        condition = where_condition_map.get(name)
        if condition:
            clause = where
        else:
            condition = having_condition_map.get(name)
            if condition:
                clause = having
            else:
                raise TypeError(
                    'Query method received unknown condition: %s=%r' %
                    (name, value))

        # Format the condition
        suffix, formatting_function, value_expression = condition
        sql_expression, sql_parameters = formatting_function(value_expression, value)
        
        # Append it to the clause
        clause.append(sql_expression)
        parameter_list.extend(sql_parameters)
        
    # Where condition built up runtime
    if runtime_where_condition is not None:
        sql_expression = format_expression(runtime_where_condition)
        where.append(sql_expression)
        
    # Having condition built up runtime
    if runtime_having_condition is not None:
        sql_expression = format_expression(runtime_having_condition)
        having.append(sql_expression)
    
    # Join the conditions together
    where = ' AND '.join(where)
    having = ' AND '.join(having)
    
    # NOTE: The group_by expressions are formatted while the abstraction layer is generated.
    
    # Format order by items
    formatted_order_by = format_order_by(order_by_map, order_by)
        
    clauses = dblayer.backend.base.clauses.Clauses(
        table_list=table_list,
        field_list=field_list,
        where=where,
        group_by=group_by,
        having=having,
        order_by=formatted_order_by,
        limit=limit,
        offset=offset)
    
    return (format_select(clauses), tuple(parameter_list))

def format_order_by(order_by_map, order_by):
    """ Formats the items of an ORDER BY clause
    """
    if constants.DEBUG:
        assert isinstance(order_by_map, dict)
        assert isinstance(order_by, (tuple, list))
        
    formatted_order_by = map(order_by_map.get, order_by)
    
    if None in formatted_order_by:
        raise ValueError(
            'Unparsable column order: %r' % 
            order_by[formatted_order_by.index(None)])
    
    return formatted_order_by
