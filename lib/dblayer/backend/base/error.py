""" Exception classes (errors)
"""

class Warning(StandardError):
  """ Database warning 
  """

class Error(StandardError):
  """ Base class for error exceptions
  """

class InterfaceError(Error):
  """ Error related to the database interface
  """

class DatabaseError(Error):
  """ Error related to the database engine
  """

class DataError(DatabaseError):
  """ Error related to problems with the processed data 
  """

class OperationalError(DatabaseError):
  """ Error related to database operation (disconnect, memory allocation etc)
  """

class IntegrityError(DatabaseError):
  """ Error related to database integrity
  """

class InternalError(DatabaseError):
  """ The database encountered an internal error
  """

class ProgrammingError(DatabaseError):
  """ Error related to database programming (SQL error, table not found etc)
  """

class NotSupportedError(DatabaseError):
  """ A method or database API was used which is not supported by the database
  """

