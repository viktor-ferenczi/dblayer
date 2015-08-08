# Tutorial #

## Define your scheme ##

Tables, indexes and constrains can be defined as simple Python classes:

```
import dblayer
from dblayer.model import database, table, column, index, constraint

class User(table.Table):
    """ User
    """
    id = column.PrimaryKey()
    email = column.Text(maxlength=80)
    first_name = column.Text(maxlength=100)
    last_name = column.Text(maxlength=100)
    phone = column.Text(maxlength=50, null=True)
    notes = column.Text(null=True, doc='Notes')

    unique_email = constraint.Unique(email)

    real_name_index = index.Index(first_name, last_name)
    
    full_text = column.SearchDocument(
        expression=(email, first_name, last_name, phone, notes), 
        doc='Full text search document to find users by all of their text attributes')
    
class Group(table.Table):
    """ Group
    """
    id = column.PrimaryKey()
    name = column.Text(maxlength=100)
    unique_name = constraint.Unique(name)
    notes = column.Text(null=True, doc='Notes')
    
class GroupUser(table.Table):
    """ User associated to group
    """
    id = column.PrimaryKey()
    group = column.ForeignKey(Group)
    user = column.ForeignKey(User)
    unique_group_user = constraint.Unique(group, user)
```

You need to define a model for the whole database as well, which will define the actual name of your tables. Their database identifiers have nothing to do with their class names.

```
class TutorialDatabaseModel(database.Database):
    """ Tutorial database model
    """
    
    # Tables
    user = User()
    group = Group()
    group_user = GroupUser()
```

## Generate code ##

Each time you change the model definitions you need to regenerate the source code of the abstraction layer:

```
import dblayer
from dblayer.backend import postgresql

database_model = TutorialDatabaseModel('TutorialDatabase')
module_source_code = database_model.generate(postgresql)
with open('abstraction.py', 'wt') as module_file:
    module_file.write(module_source_code)
```

## Connect to the database ##

Each database connection requires an instance of the generated abstraction layer class:

```
import abstraction

db = abstraction.TutorialDatabase()
with db.session(DSN):
    pass # Do something with the database
```

DSN is the connection string to be passed to `psycopg2`.

You can also connect and disconnect explicitly:

```
import abstraction

db = abstraction.TutorialDatabase()
db.connect(DSN)
try:
    pass # Do something with the database
finally:
    db.close()
```

Database abstraction layer instances are reusable, they can be used for a series of connections safely. Database connections are not thread safe, so you will need a separate instance for each thread.

## Transactions ##

Using a context manager:

```
db = abstraction.TutorialDatabase()
with db.session(DSN):
    with db.transaction():
        pass # Do something with the database in a single transaction
```

Explicit transaction management:

```
db = abstraction.TutorialDatabase()
with db.session(DSN):
    try:
        pass # Do something with the database in a single transaction
    except:
        db.rollback()
        raise
    else:
        db.commit()
```

TODO: Nested transactions?

## Creating database structure ##

TODO

## Inserting records ##

TODO

## Selecting records ##

TODO

## Updating records ##

TOOD

## Deleting records ##

TODO

## Executing queries ##

TODO

## Subclassing queries ##

TODO

## Defining views ##

NOT IMPLEMENTED