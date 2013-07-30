""" Data model used for the unit test cases
"""

import dblayer
import dblayer.backend.postgresql
from dblayer.model import database, table, column, index, constraint
from dblayer.model import query, aggregate, function, trigger, procedure
from dblayer.test import constants

### Mixins

class SlugMixin(object):
    """ Adds a slug field with the proper constraints
    """
    slug = column.Text(maxlength=30)
    validate_slug = constraint.Check(function.Match(slug, constants.RXP_IDENTIFIER))
    unique_slug = constraint.Unique(slug)

### Table models

class User(table.Table):
    """ User
    """
    id = column.PrimaryKey(serial=True)
    email = column.Text(maxlength=150)
    first_name = column.Text(maxlength=150)
    last_name = column.Text(maxlength=150)
    phone = column.Text(maxlength=100, null=True)
    notes = column.Text(null=True, doc='Custom notes')

    validate_email = constraint.Check(function.Match(email, constants.RXP_EMAIL))
    unique_email = constraint.Unique(email)

    real_name_index = index.Index(first_name, last_name)
    
    full_text = column.SearchDocument(
        expression=(email, first_name, last_name, phone, notes), 
        doc='Full text search document to find users by all of their text attributes')
    
class Group(table.Table, SlugMixin):
    """ Group
    """
    id = column.PrimaryKey()
    name = column.Text(maxlength=100)
    unique_name = constraint.Unique(name)
    notes = column.Text(null=True, doc='Custom notes')
    
class GroupUser(table.Table):
    """ User associated to group
    """
    id = column.PrimaryKey()
    group = column.ForeignKey(Group)
    user = column.ForeignKey(User)
    unique_group_user = constraint.Unique(group, user)
    
class Role(table.Table, SlugMixin):
    """ Role
    """
    id = column.PrimaryKey()
    name = column.Text(maxlength=100)
    unique_name = constraint.Unique(name)
    notes = column.Text(null=True, doc='Custom notes')

class GroupRole(table.Table):
    """ Role associated to group
    """
    id = column.PrimaryKey()
    group = column.ForeignKey(Group)
    role = column.ForeignKey(Role)
    unique_group_role = constraint.Unique(group, role)
    
class Activation(table.Table):
    """ Activation attempt
    """
    id = column.PrimaryKey()
    user = column.ForeignKey(User)
    secret = column.Integer(digits=30, doc='Secret activation code needs to be presented.')
    issued = column.Datetime(doc='Timestamp of the activation e-mail sent to the user.')
    valid_until = column.Datetime(doc='The user is deleted if not activated before this time limit.')
    passed = column.Boolean(doc='Successful activation sets this field to True.')

class Product(table.Table):
    """ Product or service
    """
    id = column.PrimaryKey()
    base = column.ForeignKey(null=True, doc='Base (parent) product or service')
    predecessor = column.ForeignKey(null=True, doc='Predecessor')
    active = column.Boolean(default=True, doc='Only active products should be ordered')
    model = column.Text(null=True, maxlength=50, doc='Model number or other internal product identifier for fast searching')
    name = column.Text(maxlength=200, doc='Official name of the product')
    unique_name = constraint.Unique(name)
    notes = column.Text(null=True, doc='Custom notes')
    last_modified = column.Datetime(null=True, doc='Last modification date and time')
    set_last_modified = trigger.BeforeInsertOrUpdateRow('fn_set_last_modified')
    
Product.base.referenced_table_class = Product
Product.predecessor.referenced_table_class = Product
    
class Invoice(table.Table):
    """ Invoice
    """
    id = column.PrimaryKey()
    serial = column.Text(maxlength=30, doc='Strictly serially incremented invoice number, generated only as the very last step.')
    unique_serial = constraint.Unique(serial)
    seller = column.ForeignKey(User)
    customer = column.ForeignKey(User)
    net_amount = column.Decimal(precision=18, scale=2)
    vat_amount = column.Integer(digits=18, default=0)
    gross_amount = column.Decimal(precision=18, scale=2)
    issued_date = column.Date()
    due_date = column.Date()
    paid_date = column.Date(null=True, doc='Date of the last payment which was sufficient to fully pay this invoice in FIFO order.')
    
class InvoiceItem(table.Table):
    """ Service
    """
    id = column.PrimaryKey()
    invoice = column.ForeignKey(Invoice, doc='The invoice this item is belonging to.')
    product = column.ForeignKey(Product, doc='Product or service sold.')
    first_day = column.Date(null=True, doc='First day of service if applicable.')
    last_day = column.Date(null=True, doc='Last day of service if applicable.')
    quantity = column.Integer(digits=9, null=True, doc='Quantity sold if applicable.')
    net_amount = column.Decimal(precision=18, scale=2)
    vat_percent = column.Float(double=False, default=0)
    vat_amount = column.Integer(digits=18, default=0)
    gross_amount = column.Decimal(precision=18, scale=2)
    notes = column.Text(null=True, maxlength=200, doc='Custom notes for this item if any.')
    
class Payment(table.Table):
    """ Payment
    """
    id = column.PrimaryKey()
    invoice = column.ForeignKey(Invoice, doc='Invoice this payment is for.')
    user = column.ForeignKey(User, doc='Actual user paying for the invoice. It should match the user on the invoice.')
    payment_date = column.Date(doc='Effective date of payment.')
    amount = column.Integer(digits=18, doc='Amount payed. It is accounted against the gross amount of the invoice.')
    commission_percent = column.Float(default=0.0, doc='Commission percentage')

### Queries

class UserContact(query.Query):
    """ Contact details of the users
    """
    # Source tables
    user = User()
    
    # Result fields
    id = query.Result(user.id)
    first_name = query.Result(user.first_name)
    last_name = query.Result(user.last_name)
    email = query.Result(user.email)
    phone = query.Result(user.phone)
    
    # Order by
    _order_by = ('first_name', 'last_name')

class ProductSale(query.Query):
    """ Sales by product based on the invoices
    """
    # Source tables
    sale = InvoiceItem()
    invoice = Invoice()
    product = Product()
    
    # Table joins
    invoice.join(sale.invoice)
    product.join(sale.product)
    
    # Result fields
    product_id = query.Result(product.id)
    product_model = query.Result(product.model)
    product_name = query.Result(product.name)
    quantity = query.Result(aggregate.Sum(sale.quantity), InvoiceItem.quantity)
    net_amount = query.Result(aggregate.Sum(sale.net_amount), InvoiceItem.net_amount)
    vat_amount = query.Result(aggregate.Sum(sale.vat_amount), InvoiceItem.vat_amount)
    gross_amount = query.Result(aggregate.Sum(sale.gross_amount), InvoiceItem.gross_amount)
    
    # Conditions
    model = query.Condition(product.model)
    active = query.Condition(product.active)
    seller = query.Condition(invoice.seller)
    customer = query.Condition(invoice.customer)
    issued_date = query.Condition(invoice.issued_date)
    
    # Group by
    _group_by = (product.id, product.model, product.name)
    
    # Order by
    _order_by = ('product_name', '+product_model', '-net_amount')
    
##class InvoicePaymentView(view.View):
##    """ Total payments by invoice
##    """
##    
##class ProductIncomeView(view.View):
##    """ Actual income by product based both on the invoices and the payments
##    """
##
##class UserPaymentView(view.View):
##    """ Total income and debt per user
##    """
    
### Database model

class TestDatabaseModel(database.Database):
    """ Test database model
    """
    
    # Tables
    user = User()
    # NOTE: Create twice to test single initialization in __new__
    user = User()
    group = Group()
    group_user = GroupUser()
    role = Role()
    group_role = GroupRole()
    activation = Activation()
    product = Product()
    invoice = Invoice()
    invoice_item = InvoiceItem()
    payment = Payment()
    
    # Queries
    user_contact = UserContact()
    # NOTE: Create twice to test single initialization in __new__
    user_contact = UserContact()
    product_sale = ProductSale()
    
    # Stored procedures
    fn_set_last_modified = procedure.Procedure(
        language='plpgsql',
        argument_list=(),
        result='trigger AS $$',
        body='''\
new.last_modified := NOW();
RETURN new;
''')
    
### Generate database abstraction layer

def generate(module_path='abstraction.py', 
             database_model_class=TestDatabaseModel, 
             abstraction_class_name='TestDatabase'):
    test_database_model = database_model_class(abstraction_class_name)
    # NOTE: Create twice to test single initialization in __new__
    test_database_model = database_model_class(abstraction_class_name)
    source = test_database_model.generate(dblayer.backend.postgresql)
    with open(module_path, 'wb') as module_file:
        module_file.write(source)
