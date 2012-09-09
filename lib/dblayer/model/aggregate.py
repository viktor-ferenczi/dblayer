""" Aggregate functions can be used in to aggregate data in queries
"""

import dblayer
from dblayer.model import function

class BaseAggregate(function.BaseFunction):
    """ Base class for aggregates
    """
    
    def __init__(self, *args):
        function.BaseFunction.__init__(self, *args)
        
        assert self.__class__ is not BaseAggregate, (
            'Only subclasses of BaseAggregate can be instantiated!')

class Count(BaseAggregate):
    pass

class Min(BaseAggregate):
    pass

class Max(BaseAggregate):
    pass

class Sum(BaseAggregate):
    pass

class Avg(BaseAggregate):
    pass
