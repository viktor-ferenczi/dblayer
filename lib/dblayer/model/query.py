""" Base class for query definitions
"""

from dblayer import constants
from dblayer.model import table, column, index

class Query(table.Table):
    """ Query
    """
    # Queries need not be created and cannot be written
    _creatable = False
    _writable = False
    
    # Mapping of alias names to the source Table instances
    # NOTE: Filled in by the __new__ class method
    _table_map = None
    
    # Conditions
    # NOTE: Filled in by the __new__ class method
    _condition_list = ()
    
    # Group by expressions (override in your subclass)
    _group_by = ()
    
    # Ordering expressions (override in your subclass)
    _order_by = ()
    
    def __new__(cls):
        # Initialize the class only once
        if not cls._initialized:
            cls.initialize()
            
        return super(Query, cls).__new__(cls)
    
    @classmethod
    def initialize(cls):
        
        # Mark the class as initialized
        # NOTE: It prevent Table's initialization from being executed, it is intentional
        cls._initialized = True
        
        # Collect objects from the class definition
        cls._collect_result_condition_list()
    
    @classmethod
    def _collect_result_condition_list(cls):
        """ Collects the result column and condition definitions from the model class
        """
        cls._table_map = {}
        cls._column_list = []
        cls._condition_list = []
        for name in dir(cls):
            value = getattr(cls, name)
            
            if isinstance(value, table.Table):
                value._name = name
                cls._table_map[name] = value

            elif isinstance(value, Result):
                value.table_class = cls
                value.name = name
                cls._column_list.append(value)
                
            elif isinstance(value, Condition):
                value.table_class = cls
                value.name = name
                cls._condition_list.append(value)
                
        # Sort them by definition order
        cls._column_list.sort(key=column.BaseColumn.sort_key)
        cls._condition_list.sort(key=Condition.sort_key)
        
    def __repr__(self):
        return '<Query: %s>' % self._name
    
    __str__ = __repr__
    
    def get_table_list(self):
        """ Returns the table list for the clauses of the SELECT statement
        
        Items of the returned list can be either:
        
        (table_name, alias_name) to start a new cross join group or
        (table_name, alias_name, join_type, id_field_name, referer_table_name, fk_field_name) to continue one.
        
        Unaliased tables are not used here.
        
        """
        table_map = self._table_map.copy()
        
        def iterate_joined_tables(referer):
            """ Appends tables referenced from the given table to the table list
            """
            # Iterate on all the tables remained in the table map
            for alias_name, table in table_map.items():

                # Determine the referer table for this table in the JOIN chain
                foreign_key = table._referer
                referer_table = foreign_key.table if foreign_key else None
                
                # Is the referer the table we're looking for?
                if referer_table is referer:
                    
                    # Delete the referenced table from the map
                    del table_map[alias_name]
                    
                    # Construct table list item
                    if referer is None:
                        item = (
                            # Database table name, not the alias in this query
                            table._table_name,
                            # Alias name in this query for this table
                            alias_name)
                    else:
                        join_type = constants.LEFT_JOIN if foreign_key.null else constants.INNER_JOIN
                        item = (
                            # Database table name, not the alias in this query
                            table._table_name,
                            # Alias name in this query for this table
                            alias_name,
                            # Type of this joine, like INNER JOIN or LEFT JOIN
                            join_type,
                            # Name of the primary key column in the joined table
                            table._primary_key.name,
                            # Alias name of the referer (already joined) table
                            referer_table._name,
                            # Name of the referer foreign key column in the referer (already joined) table
                            foreign_key.name)
                        
                    if constants.DEBUG:
                        assert not sum(1 for x in item if not x), 'Empty name(s) in join definition item: %r' % (item, )
                        
                    # Yield table list item
                    yield item
                        
                    # Find all the referer tables below this point in the tree
                    for item in iterate_joined_tables(table):
                        yield item
                        
        table_list = list(iterate_joined_tables(None))
        assert not table_map
        return table_list

    @classmethod
    def pretty_format_class(cls):
        """ Formats source code defining the query
        """
        if not cls._initialized:
            cls.initialize()
            
        line_list = ['class %s(query.Query):' % cls.__name__]
        append_line = line_list.append

        if cls.__doc__:
            if '\n' in cls.__doc__:
                append_line('    """%s"""' % cls.__doc__)
            else:
                append_line('    """ %s """' % cls.__doc__.strip())
        else:    
            append_line('')

        for alias, table in sorted(cls._table_map.items()):
            append_line('    %s = %s()' % (alias, table.__class__.__name__))
        append_line('')
        
        for obj in cls._column_list:
            append_line('    %s = %s' % (obj.name, obj.full_repr()))
            
        append_line('')

        for obj in cls._condition_list:
            append_line('    %s = %s' % (obj.name, obj.full_repr()))

        append_line('')

        if cls._group_by:
            append_line('    _group_by = %r' % (cls._group_by, ))

        if cls._order_by:
            append_line('    _order_by = %r' % (cls._order_by, ))
            
        for i in xrange(len(line_list) - 1, 1, -1):
            if not line_list[i] and not line_list[i - 1]:
                del line_list[i]
            
        return '\n'.join(line_list)

class BaseQueryResult(column.BaseColumn):
    """ Base class for columns used in a query either as a result or a condition
    
    The expression can be a column of a source table or any expression built
    from functions and aggregations in any way acceptable in an SQL statement.
    
    """
    
    # Expression to evaluate to get the value of the given result column
    expression = None
    
    # Column definition for calculated results, required for expressions,
    # filled automatically in the case of simple source column results
    column_type = None

    # True value indicates that the expression is written into the HAVING clause
    after_group_by = False
    
    def __init__(self, expression, column_type=None, doc=None):
        column.BaseColumn.__init__(self, doc=doc)
        
        assert self.__class__ is not BaseQueryResult, (
            'Only subclasses of BaseQueryResult can be instantiated!')
        
        # Cloning the column?
        if expression is None:
            return
        
        # Automatically set the column_type in the case of simple results
        if isinstance(expression, column.BaseColumn):
            assert column_type is None, 'Passing column_type is not required in the case of simple column references!'
            column_type = getattr(expression.table_class, expression.name)
            self.full_repr_exclude = ('column_type', )
        elif isinstance(expression, index.FullTextSearchIndex):
            column_type = column.Text
        else:
            assert column_type is not None, 'Passing column_type is required in the case of calculated expressions!'
            assert column_type.table is None, 'Pass an unbound column type, not the column of a bound table!'
            
        self.expression = expression
        self.column_type = column_type
        
    def clone(self, table):
        """ Clone this query result column for a table instance
        
        NOTE: It is called by Table.__init__ to bound the columns to the table instance.
        
        """
        clone = self.__class__(None)
        clone.__dict__.update(self.__dict__)
        clone.table = table
        return clone

class Result(BaseQueryResult):
    """ Query result
    """
    
class Condition(BaseQueryResult):
    """ Query condition precessed before processing the aggregates (where)
    """

class PostCondition(Condition):
    """ Query condition precessed after processing the aggregates (having)
    """
    # The expression is written into the HAVING clause
    after_group_by = True
