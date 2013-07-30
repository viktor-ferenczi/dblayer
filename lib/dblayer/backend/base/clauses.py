""" Data structures for preparing SQL queries
"""

import dblayer
from dblayer import constants

class Clauses(object):
    """ Hashable clauses for a single query
    """
    
    __slots__ = (
        # List of source tables. Possible items:
        # table_name for non-aliased and single tables
        # (table_name, alias_name) to start a new cross join group or
        # (table_name, alias_name, join_type, join_condition) to continue a group.
        'table_list',
        
        # List of field expressions, each item must be a string
        'field_list',
        
        # Optional list of where conditions, they are joined by the AND operator
        'where',
        
        # Optional list of group by expressions, each item must be a string
        'group_by',
        
        # Optional list of having conditions, each item must be a string, they are joined by the AND operator
        'having',
        
        # Optional list of order by expressions, each item must be a string
        'order_by',
        
        # Optional integer limit for the number of records returned
        'limit',
        
        # Optional integer offset to shift the limit window over the result set
        'offset',
        
        # Hash value calculated from all the above for caching SQL statements
        'hash_value')
    
    def __init__(
        self,
        table_list=(),
        field_list=(),
        where='',
        group_by=(),
        having='',
        order_by=(),
        limit=None,
        offset=None):
        
        if constants.DEBUG:
            assert isinstance(table_list, (tuple, list))
            assert isinstance(field_list, (tuple, list))
            assert isinstance(where, basestring)
            assert isinstance(group_by, (tuple, list))
            assert isinstance(having, str)
            assert isinstance(order_by, (tuple, list))
            
        self.table_list = tuple(table_list)
        self.field_list = tuple(field_list)
        self.where = str(where)
        self.group_by = tuple(group_by)
        self.having = str(having)
        self.order_by = tuple(order_by)
        self.limit = None if limit is None else int(limit)
        self.offset = None if offset is None else int(offset)
        
        self.hash_value = None
        
    def __repr__(self):
        return '%s.%s(%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__,
            ', '.join(
                '%s=%r' % (name, value)
                for name, value in zip(self.__slots__, self.get_tuple())))
    
    __str__ = __repr__
        
    def get_tuple(self):
        return (
            self.table_list,
            self.field_list,
            self.where,
            self.group_by,
            self.having,
            self.order_by,
            self.limit,
            self.offset)
    
    def __hash__(self):
        if self.hash_value is None:
            self.hash_value = hash(self.get_tuple())
        elif constants.DEBUG:
            assert self.hash_value == hash(self.get_tuple()), 'Clauses instance has been changed since its hash calculated the last time!'
        return self.hash_value
    
    def __eq__(self, other):
        if not isinstance(other, Clauses):
            return False
        if hash(self) != hash(other):
            return False
        return self.get_tuple() == other.get_tuple()
