""" Test constants
"""

import dblayer
from dblayer import constants


# Enable this to debug rolling back to savepoints on conflicting primary keys
##if constants.DEBUG:
##    constants.DATABASE_ID_RANGE = (1, 10)
##    constants.MAX_INSERT_RETRY_COUNT = 3

# Database to use for testing
TEST_DSN = ''

# Enables dropping of the test tables after running the unit test cases
LEAVE_CLEAN_DATABASE = True

# Regular expressions used by the test check constraints
RXP_IDENTIFIER = r'^[a-zA-Z_][a-zA-Z_0-9]*$'
RXP_EMAIL = r'^[\w\-\.]+@[\w\-]+(?:\.[\w\-]+)*$'
