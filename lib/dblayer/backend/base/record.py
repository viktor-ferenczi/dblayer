""" Base class for the generated record classes
"""

class Record(object):
    """ Base class for the generated record classes
    """

    # Name of the corresponding database table storing the actual records
    _table_name = ''
    
    ### Runtime column information
    
    # List of column names
    _column_name_list = ()
    
    # Set of names of nullable columns
    _nullable_column_name_set = set()
    
    # Map of column names to default field values (if there's one)
    _column_default_map = {}
    
    ### Optimization
    
    # Subclasses will define record fields as slots
    __slots__ = ()

    ### Textual representation for logging and debugging
    
    def __repr__(self):
        get_default_field_value = self._column_default_map.get
        return '%s.%s(%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__,
            ', '.join(
                '%s=%r' % (name, getattr(self, name))
                for name in self._column_name_list
                if getattr(self, name) is not get_default_field_value(name)))
    
    __str__ = __repr__
    
    def __eq__(self, other):
        if other.__class__ is not self.__class__:
            return False
        for name in self._column_name_list:
            if getattr(self, name) != getattr(other, name):
                return False
        return True
    
    def finalize(self):
        """ Finalizes the record
        
        Override in your subclass to provide calculated field values
        right before storing or updating the record into the database.
        Finalization should not raise exceptions normally.
        
        This method should only access this single record,
        it must not execute queries for its operation.
        
        """
