""" Constants
"""

import re, os

# Enables various sanity checks useful for development
DEBUG = int(os.environ.get('DBLAYER_DEBUG', 0))

# Encoding used to convert strings to unicode whenever needed
ENCODING = 'utf8'

# Full pathname of the code generator template directory
GENERATOR_TEMPLATE_DIRECTORY_PATH = os.path.join(
    os.path.dirname(__file__), 'generator', 'template')

# Range of database ID values (actual values are chosen randomly)
DATABASE_ID_RANGE = (2**62, 2**63)

# Number of rows should be loaded from the database at once
CURSOR_ARRAYSIZE = 128

# Logging
LOG_SQL_STATEMENTS = DEBUG and True
LOG_SQL_RESULT_ROWS = DEBUG and False

# Log analysis of SQL statements before executing them
LOG_SQL_ANALYSIS = DEBUG and True

# Measure the wall clock execution time of queries
PROFILE_QUERIES = DEBUG and True

# Maximum number of retries on a failing single row INSERT query
MAX_INSERT_RETRY_COUNT = 100

# Join types
INNER_JOIN = 'INNER JOIN'
LEFT_JOIN = 'LEFT JOIN'
JOIN_TYPES = (INNER_JOIN, LEFT_JOIN)

# Undefined singleton, used to detect missing parameters
# while allowing None as a valid value
class NA(object):
    def __repr__(self):
        return 'NA'
    __str__ = __repr__
NA = NA()
