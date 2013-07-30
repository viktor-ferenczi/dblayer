import psycopg2
import psycopg2.extensions

import dblayer.backend.base
from dblayer.backend.base import database

### Force returning of unicode string from the database
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class DatabaseAbstraction(database.DatabaseAbstraction):
    
    def _connect(self, dsn, client_encoding='UTF8'):
        
        self.connection = psycopg2.connect(dsn)
        self.connection.set_client_encoding(client_encoding)
