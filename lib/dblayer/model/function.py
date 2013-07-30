""" Functions can be used in result expressions of queries
"""

class BaseFunction(object):
    """ Base class for functions
    """
    def __init__(self, *args):
        self.args = args
        
        assert self.__class__ is not BaseFunction, (
            'Only subclasses of BaseFunction can be instantiated!')
        
    def __str__(self):
        return '<%s%r>' % (self.__class__.__name__, tuple(self.args))
    
    def __repr__(self):
        return '%s.%s(%s)' % (
            self.__class__.__module__.rsplit('.', 1)[-1],
            self.__class__.__name__, 
            ', '.join(map(repr, self.args)))

### Custom

class Custom(BaseFunction):
    """ Custom function
    
    It is useful for inspected databases, where we don't have expressions in parsed format.
    
    """

### Variable

class Var(BaseFunction):
    """ Variable reference
    
    It is useful for triggers and domains, where the variable name
    is determined by the SQL server ("new" or "value").
    
    """

### Logical

class Not(BaseFunction):
    """ Logical NOT of its parameters
    
    Returns a boolean.
    
    """
    def __init__(self, a):
        BaseFunction.__init__(self, a)

class And(BaseFunction):
    """ Logical AND of its parameters
    
    Returns a boolean.
    
    """

class Or(BaseFunction):
    """ Logical OR of its parameters
    
    Returns a boolean.
    
    """

### Comparision

class Equal(BaseFunction):
    """ True if its two parameters are equal
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class NotEqual(BaseFunction):
    """ True if its two parameters are not equal
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class LessThan(BaseFunction):
    """ True if the first parameter is less than the second one
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class LessThanOrEqual(BaseFunction):
    """ True if the first parameter is less or equal than the second one
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class GreaterThan(BaseFunction):
    """ True if the first parameter is greater than the second one
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class GreaterThanOrEqual(BaseFunction):
    """ True if the first parameter is greater than or equal the second one
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class In(BaseFunction):
    """ True if its first parameter is in member of the expression passed as its second parameter
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

class NotIn(BaseFunction):
    """ True if its first parameter is in member of the expression passed as its second parameter
    
    Returns a boolean.
    
    """
    def __init__(self, a, b):
        BaseFunction.__init__(self, a, b)

### Numeric

class Neg(BaseFunction):
    """ Negate a single numeric value
    """
    def __init__(self, a):
        BaseFunction.__init__(self, a)

class Add(BaseFunction):
    """ Add any number of numeric values
    """

class Sub(BaseFunction):
    """ Subtract any number of numeric values from the first one
    """

class Mul(BaseFunction):
    """ Multiply any number of numeric values
    """

class Div(BaseFunction):
    """ Divide the first numeric value with the subsequent ones
    """

# TODO: Pow, Exp, Log, ...

### String

class Concat(BaseFunction):
    """ Concatenates any number of values, also converts them to strings
    
    Returns a string.
    
    """

class Left(BaseFunction):
    """ Take the leftmost characters of a string (string, count)
    
    Returns a string.
    
    """
    def __init__(self, text, length):
        BaseFunction.__init__(self, text, length)

class Right(BaseFunction):
    """ Take the rightmost characters of a string (string, count)
    
    Returns a string.
    
    """
    def __init__(self, text, length):
        BaseFunction.__init__(self, text, length)

class Substring(BaseFunction):
    """ Extract substring (string, first_character[, count])
    
    Returns a string.
    
    """
    def __init__(self, text, position, length=None):
        if length is None:
            BaseFunction.__init__(self, text, position)
        else:
            BaseFunction.__init__(self, text, position, length)

class Contains(BaseFunction):
    """ Checks whether a substring is in text
    
    Returns a boolean.
    
    """
    def __init__(self, text, substring):
        BaseFunction.__init__(self, text, substring)

class Like(BaseFunction):
    """ Checks whether a LIKE pattern matches text
    
    Returns a boolean.
    
    """
    def __init__(self, text, pattern):
        BaseFunction.__init__(self, text, pattern)

class NotLike(BaseFunction):
    """ Checks whether a LIKE pattern does not match text
    
    Returns a boolean.
    
    """
    def __init__(self, text, pattern):
        BaseFunction.__init__(self, text, pattern)

class Match(BaseFunction):
    """ Checks whether a regular expression matches text
    
    Returns a boolean.
    
    """
    def __init__(self, text, pattern):
        BaseFunction.__init__(self, text, pattern)

class NotMatch(BaseFunction):
    """ Checks whether a regular expression does not matche text
    
    Returns a boolean.
    
    """
    def __init__(self, text, pattern):
        BaseFunction.__init__(self, text, pattern)

class FullTextSearch(BaseFunction):
    """ Performs full text search
    
    For optimal performance the expression should be a column with a 
    full text search index on it, but it also works for any expression
    resulting in a string or a value can be converted to a string.
    
    The pattern is like: word1 & word2
    
    Returns a boolean.
    
    """
    def __init__(self, column, pattern):
        BaseFunction.__init__(self, column, pattern)

### Selection

class Coalesce(BaseFunction):
    """ Results in the first not-NULL expression or NULL if all are NULL
    """

# TODO: If-Else, Case-Else

### GIS functions

# TODO
