# -*- coding: utf8 -*-

import datetime
import os
import re
import unittest

import dblayer

import dblayer.model.database
import dblayer.model.table
from dblayer.test import model

MODEL_DIR = os.path.dirname(model.__file__)

from dblayer import constants
from dblayer.backend.base import clauses
from dblayer.graph import gml

from dblayer.test import constants as test_constants


class TestGraph(unittest.TestCase):

    def setUp(self) -> None:
        model.generate()
        self.abstraction = __import__('abstraction')

    def testGML(self):
        model_instance = model.TestDatabaseModel('TestDatabase')
        exporter = gml.GMLExporter(model_instance)
        exporter.export('model.gml')


class TestAbstraction(unittest.TestCase):

    def setUp(self):
        model.generate()
        self.abstraction = __import__('abstraction')
        self.db = self.abstraction.TestDatabase()
        self.db.connect(test_constants.TEST_DSN)
        self.db.enable_transactions()
        with self.db.transaction():
            self.db.drop_structure(ignore_errors=True)
        with self.db.transaction():
            self.db.create_structure()

    def tearDown(self):
        if test_constants.LEAVE_CLEAN_DATABASE:
            self.db.rollback()
            with self.db.transaction():
                self.db.drop_structure(ignore_errors=True)
        self.db.disable_transactions()
        self.db.close()

        # It must not fail
        self.db.close()

    def test_database_session(self):
        db2 = self.abstraction.TestDatabase()
        self.assertTrue(not db2.connected)
        with db2.session(test_constants.TEST_DSN):
            self.assertTrue(db2.connected)
            with db2.transaction():
                hdd = db2.new_product(name='hdd')
                db2.add_product(hdd)
            self.assertTrue(db2.connected)
        self.assertTrue(not db2.connected)

        with self.db.transaction():
            self.assertEqual(self.db.find_product(name='hdd').id, hdd.id)

        # Automatic closing of connection in __del__ in no-error condition
        db3 = self.abstraction.TestDatabase()
        db3.connect(test_constants.TEST_DSN)
        self.assertTrue(db3.connected)
        del db3

        # Automatic closing of connection in __del__ after a database error
        db4 = self.abstraction.TestDatabase()
        db4.connect(test_constants.TEST_DSN)
        self.assertTrue(db4.connected)
        with db4.cursor() as cursor:
            self.assertRaises(db4.Error, db4.execute, cursor, 'BAD SQL')
        del db4

    def test_truncate(self):
        self.assertEqual(self.db.get_user_count(), 0)
        with self.db.transaction():
            self.db.truncate_all_tables()
        self.assertEqual(self.db.get_user_count(), 0)
        with self.db.transaction():
            self.load_data()
        self.assertNotEqual(self.db.get_user_count(), 0)
        with self.db.transaction():
            self.db.truncate_all_tables()
        self.assertEqual(self.db.get_user_count(), 0)

    def test_insert_select(self):
        with self.db.transaction():
            self.load_data()
        with self.db.transaction():
            self.verify_data()
        self.db.commit()

    def test_duplicate_insert(self):
        with self.db.transaction():
            self.load_data()
        user_list = self.db.get_user_list()
        with self.db.transaction():
            self.assertRaises(self.db.IntegrityError, self.db.add_user, user_list[0], generate_id=False)
        with self.db.transaction():
            self.assertRaises(self.db.IntegrityError, self.db.add_user_list, user_list, generate_id=False)

    def test_update_delete(self):
        with self.db.transaction():
            self.load_data()
        self.modify_data()
        self.do_failed_transaction()

    def load_data(self, db=None):
        """ Loads test data
        """
        if db is None:
            db = self.db
        if 0:
            assert isinstance(db, self.abstraction.TestDatabase)

        # Users
        viktor = db.new_user(
            email='viktor@ferenczi.eu',
            first_name='Viktor',
            last_name='Ferenczi',
            phone='1234567')
        anna = db.new_user(
            email='anna@cx.hu',
            first_name='Anna',
            last_name='Szili',
            phone='2345678')
        isi = db.new_user(
            email='isi@cx.hu',
            first_name='István',
            last_name='Horváth')
        annacska = db.new_user(
            email='anna@ferenczi.eu',
            first_name='Anna',
            last_name='Ferenczi',
            phone='4567890')
        self.assertTrue(viktor.id is None)
        db.add_user(viktor)
        self.assertFalse(viktor.id is None)
        db.add_user_list([anna, isi])
        self.assertFalse(anna.id is None)
        self.assertFalse(isi.id is None)
        db.add_user_list([annacska])
        self.assertFalse(annacska.id is None)
        self.assertEqual(db.get_user_count(), 4)
        db.add_user_list([])
        self.assertEqual(db.get_user_count(), 4)
        self.assertNotEqual(viktor, 'not a record')
        self.assertNotEqual(viktor, anna)
        self.assertNotEqual(viktor.id, anna.id)
        self.assertNotEqual(viktor.id, isi.id)
        self.assertNotEqual(viktor.id, annacska.id)
        user_list = db.get_user_list()
        self.assertEqual(len(user_list), db.get_user_count())

        # Groups
        admin = db.new_group(
            slug='admin',
            name='Admin')
        provider = db.new_group(
            slug='provider',
            name='Service provider')
        customer = db.new_group(
            slug='customer',
            name='Customer')
        group_list = sorted([admin, provider, customer], key=lambda r: r.slug)
        db.add_group_list(group_list)
        self.assertEqual(len(group_list), db.get_group_count())
        self.assertEqual(group_list, sorted(list(db.get_group_iter()), key=lambda r: r.slug))

        # Associate users with groups
        group_user_list = [
            db.new_group_user(group=admin.id, user=viktor.id),
            db.new_group_user(group=provider.id, user=viktor.id),
            db.new_group_user(group=provider.id, user=annacska.id),
            db.new_group_user(group=customer.id, user=viktor.id),
            db.new_group_user(group=customer.id, user=anna.id),
            db.new_group_user(group=customer.id, user=isi.id)]
        db.add_group_user_list(group_user_list)

        # Verify data
        group_user_id_list = set([group_user.id for group_user in group_user_list])
        self.assertEqual(len(group_user_list), len(group_user_id_list))
        for group_user in db.get_group_user_list():
            self.assertTrue(group_user.id in group_user_id_list)
        self.assertEqual(len(group_user_list), db.get_group_user_count())
        self.assertEqual(viktor, db.find_user(email='viktor@ferenczi.eu'))
        self.assertEqual(anna, db.find_user(last_name='Szili'))

        # Add products
        hdd = db.new_product(name='hdd')
        consulting = db.new_product(name='consulting')
        db.add_product_list([hdd, consulting])

        # Add two invoices
        for m, customer in [(1, anna), (2, isi)]:
            invoice = db.new_invoice(
                serial='2010/%04d' % m,
                seller=viktor.id,
                customer=customer.id,
                net_amount=4600 * m,
                vat_amount=1150 * m,
                gross_amount=5750 * m,
                issued_date=datetime.date(2010, 3, 31),
                due_date=datetime.date(2010, 4, 30))
            db.add_invoice(invoice)
            item1 = db.new_invoice_item(
                invoice=invoice.id,
                product=hdd.id,
                quantity=6 * m,
                net_amount=600 * m,
                vat_percent=2500 * m,
                vat_amount=150 * m,
                gross_amount=750 * m)
            item2 = db.new_invoice_item(
                invoice=invoice.id,
                product=consulting.id,
                quantity=20 * m,
                first_day=datetime.date(2010, 4, 1),
                last_day=datetime.date(2010, 4, 30),
                net_amount=4000 * m,
                vat_percent=25000 * m,
                vat_amount=1000 * m,
                gross_amount=5000 * m)
            db.add_invoice_item_list([item1, item2])

    def verify_data(self, db=None):
        """ Do data verification
        """
        if db is None:
            db = self.db
        if 0:
            assert isinstance(db, self.abstraction.TestDatabase)

        viktor = db.find_user(email='viktor@ferenczi.eu')
        self.assertFalse(viktor is None)
        anna = db.find_user(email='anna@cx.hu')
        self.assertFalse(anna is None)

        viktor_groups = db.find_group_user_list(user=viktor.id)
        self.assertEqual(len(viktor_groups), 3)

        anna_groups = db.find_group_user_list(user=anna.id)
        self.assertEqual(len(anna_groups), 1)

        get_result_list = db.get_user_list()
        find_result_list = db.find_user_list()
        self.assertAlmostEqual(get_result_list, find_result_list)

    def modify_data(self, db=None):
        """ Do data modification
        """
        if db is None:
            db = self.db
        if 0:
            assert isinstance(db, self.abstraction.TestDatabase)

        admin = db.find_group(slug='admin')
        anna = db.find_user(email='anna@cx.hu')
        group_membership = db.new_group_user(group=admin.id, user=anna.id)
        db.add_group_user(group_membership)
        anna_groups = db.find_group_user_list(user=anna.id)
        self.assertEqual(len(anna_groups), 2)

        anna.email = 'wrong@email.address'
        db.update_user(anna)

        anna.email = 'szanna@pmgsz.hu'
        db.update_user(anna)

        anna2 = db.find_user(last_name='Szili')
        self.assertEqual(anna, anna2)
        self.assertEqual(repr(anna), repr(anna2))
        self.assertEqual(repr(anna), str(anna2))

        group_user_count = db.get_group_user_count()
        db.delete_group_user(group_membership)
        self.assertEqual(db.get_group_user_count(), group_user_count - 1)
        db.delete_group_user(group_membership)
        self.assertEqual(db.get_group_user_count(), group_user_count - 1)

        db.add_group_user(group_membership)

        self.assertRaises(
            db.IntegrityError,
            db.add_group_user,
            group_membership)
        db.rollback()

        user_list = db.get_user_list()
        db.update_user_list(user_list)
        db.update_user_list(user_list[:1])
        db.update_user_list([])
        db.rollback()

        group_user_list = db.get_group_user_list()

        db.delete_group_user(group_user_list[0])
        self.assertEqual(db.get_group_user_count(), len(group_user_list) - 1)
        db.rollback()

        db.delete_group_user(group_user_list[0].id)
        self.assertEqual(db.get_group_user_count(), len(group_user_list) - 1)
        db.rollback()

        db.delete_group_user_list(group_user_list[:2])
        self.assertEqual(db.get_group_user_count(), len(group_user_list) - 2)
        db.rollback()

        db.delete_group_user_list([group_user.id for group_user in group_user_list])
        self.assertEqual(db.get_group_user_count(), 0)
        db.rollback()

    def do_failed_transaction(self):
        try:
            with self.db.transaction():
                group_x = self.db.new_group(slug='x', name='y')
                self.db.add_group(group_x)
                raise ValueError()
        except ValueError:
            pass

        self.assertTrue(self.db.find_group(slug='x') is None)

    def test_clauses_class(self):
        """ Tests the Clauses class
        """

        Clauses = self.db.Clauses
        ns = dict(clauses=clauses)

        empty_clauses = Clauses(table_list=('tbl1',))
        self.assertEqual(eval(repr(empty_clauses), ns), empty_clauses)

        full_clauses = Clauses(
            table_list=('tbl2',),
            field_list=('a', 'b', 'c', 'COUNT(*) AS cnt'),
            where='"a" = ?',
            group_by=('d',),
            having='cnt > 3',
            order_by=('b', 'c'),
            limit=10,
            offset=20)
        self.assertEqual(eval(repr(full_clauses), ns), full_clauses)

        empty_hash = hash(empty_clauses)
        self.assertEqual(empty_hash, hash(empty_clauses))
        self.assertEqual(empty_clauses, empty_clauses)

        full_hash = hash(full_clauses)
        self.assertEqual(full_clauses, full_clauses)
        self.assertEqual(full_hash, hash(full_clauses))

        self.assertNotEqual(empty_clauses, 'something else')
        self.assertNotEqual(empty_clauses, full_clauses)
        self.assertNotEqual(empty_hash, full_hash)  # Probably and should be

        clauses_cache = {}
        clauses_cache[empty_clauses] = repr(empty_clauses)
        self.assertEqual(clauses_cache[empty_clauses], str(empty_clauses))
        clauses_cache[full_clauses] = repr(full_clauses)
        self.assertEqual(clauses_cache[full_clauses], str(full_clauses))

    def test_random_id_selection(self):
        """ Tests whether colliding random IDs are properly replaced
        """

        db = self.db
        assert isinstance(db, self.abstraction.TestDatabase)

        # Use a very narrow random ID range for the tests
        original_range = constants.DATABASE_ID_RANGE
        constants.DATABASE_ID_RANGE = (1, 10)
        try:
            group_list = [
                db.new_group(slug='g%d' % n, name='G%d' % n)
                for n in range(*constants.DATABASE_ID_RANGE)]

            # Add all ten possible records multiple times
            for n in range(10):
                # Add ony by one
                with db.transaction():
                    for group in group_list:
                        db.add_group(group)
                    db.delete_group_list(group_list)

                # Add as a list
                with db.transaction():
                    db.add_group_list(group_list)
                    db.delete_group_list(group_list)

            # Add them again
            with db.transaction():
                db.add_group_list(group_list)

            # Try to add one more, it should result in an IntegrityError all the time
            group = db.new_group(slug='gX', name='GX')
            for n in range(10):
                with db.transaction():
                    self.assertRaises(db.IntegrityError, db.add_group, group)

            # Verify the detection function
            try:
                db.add_group_list([group, group])
            except db.IntegrityError as reason:
                self.assertTrue(db.is_primary_key_conflict(reason))
            else:
                self.assertTrue(False)
            db.rollback()

            # Verify that other unique constraint conflicts are not misdetected as an ID conflict
            with db.transaction():
                # Reserve a single ID for the new record
                db.delete_group(group_list[0])
            with db.transaction():
                try:
                    # Conflicting group name, but there's a free ID
                    db.add_group(group_list[1])
                except db.IntegrityError as reason:
                    self.assertTrue(not db.is_primary_key_conflict(reason))
                    db.rollback()
                else:
                    self.assertTrue(False)
        finally:
            constants.DATABASE_ID_RANGE = (1, 11)

    def test_user_contact_query(self):
        """ Test the UserContact query
        """
        db = self.db
        assert isinstance(db, self.abstraction.TestDatabase)

        with db.transaction():
            self.load_data()

        self.assertEqual(db.query_user_contact_count(), 4)
        self.assertEqual(db.query_user_contact_count(phone=None), 1)
        self.assertEqual(db.query_user_contact_count(phone_ne=None), 3)
        self.assertEqual(db.query_user_contact_count(phone=''), 0)
        self.assertEqual(db.query_user_contact_count(phone='1234567'), 1)
        self.assertEqual(db.query_user_contact_count(phone_in=()), 0)
        self.assertEqual(db.query_user_contact_count(phone_in=('1234567',)), 1)
        self.assertEqual(db.query_user_contact_count(phone_in=['1234567', '2345678']), 2)
        self.assertEqual(db.query_user_contact_count(phone_in=('1234567', '2345678')), 2)
        self.assertEqual(db.query_user_contact_count(phone_not_in=['1234567']), 2)
        self.assertEqual(db.query_user_contact_count(phone_not_in=[]), 4)
        # TODO: Add tests for all the other operators here

    def test_product_sale_query(self):
        """ Tests the ProductSale query
        """
        db = self.db
        assert isinstance(db, self.abstraction.TestDatabase)

        with db.transaction():
            self.load_data()

        product_sale_list = db.query_product_sale_list()
        product_sale_list.sort(key=lambda r: r.product_name)

        product_sale_list2 = list(db.query_product_sale_iter())
        product_sale_list2.sort(key=lambda r: r.product_name)

        self.assertEqual(product_sale_list, product_sale_list2)

        self.assertEqual(len(product_sale_list), 2)
        self.assertEqual(db.query_product_sale_count(), 2)

        bar_sales, foo_sales = product_sale_list

        self.assertEqual(bar_sales.product_name, 'consulting')
        self.assertEqual(foo_sales.product_name, 'hdd')

    def test_triggers(self):
        """ Tests whether the triggers are working
        """
        db = self.db
        assert isinstance(db, self.abstraction.TestDatabase)

        # After insert or update trigger
        with db.transaction():
            # Add a product
            hdd = db.new_product(name='hdd')
            self.assertTrue(hdd.last_modified is None)
            db.add_product(hdd)

            # Check whether the last modified time is filled in
            hdd = db.get_product(hdd.id)
            created = hdd.last_modified
            self.assertTrue(created)

            # Modify the product
            hdd.model = 'Samsung'
            hdd.last_modified = created - datetime.timedelta(days=1)
            db.update_product(hdd)

            # Check whether the last modified time is updated
            hdd = db.get_product(hdd.id)
            self.assertTrue(hdd.last_modified >= created)

        # TODO: Test more triggers

    def test_full_text_search(self):
        """ Tests full text search index
        """
        db = self.db
        assert isinstance(db, self.abstraction.TestDatabase)

        with db.transaction():
            self.load_data()

        with db.transaction():
            user_list = db.find_user_list(full_text_search='viktor:*')
        self.assertEqual(len(user_list), 1)
        self.assertEqual(user_list[0].first_name, 'Viktor')

        with db.transaction():
            user_list = db.find_user_list(
                full_text_search='ferenczi', order_by=('first_name', 'last_name'))
        self.assertEqual(len(user_list), 2)
        self.assertEqual(user_list[0].first_name, 'Anna')
        self.assertEqual(user_list[1].first_name, 'Viktor')

    def test_repr_str(self):
        """ Tests whether all the objects can be printed
        """
        obj_list = (
            self.db,
            self.abstraction.TestDatabase,
            model.User,
            model.Product,
            model.ProductSale)
        for obj in obj_list:
            for name in dir(obj):
                if name.startswith('__'):
                    continue
                value = getattr(obj, name)
                self.assertTrue(repr(value))
                self.assertTrue(':' + str(value))

    def test_tuple_dict(self):
        """ Tests whether the field values can be acquired
        """
        with self.db.transaction():
            self.load_data()

        for product in self.db.get_product_iter():
            break
        assert isinstance(product, self.abstraction.ProductRecord)

        field_dict = product.dict
        field_tuple = product.tuple
        self.assertEqual(len(field_dict), len(field_tuple))
        self.assertEqual(field_tuple, tuple(field_dict[name] for name in product._column_name_list))
        self.assertEqual(product, self.db.new_product(*field_tuple))
        self.assertEqual(product, self.db.new_product(**field_dict))

    def test_order_by(self):

        get_result_list = self.db.get_user_list(order_by=('first_name',))
        find_result_list = self.db.find_user_list(order_by=('first_name',))
        self.assertAlmostEqual(get_result_list, find_result_list)

        get_result_list = self.db.get_user_list(order_by=('+first_name',))
        find_result_list = self.db.find_user_list(order_by=('+first_name',))
        self.assertAlmostEqual(get_result_list, find_result_list)

        get_result_list = self.db.get_user_list(order_by=('-first_name',))
        find_result_list = self.db.find_user_list(order_by=('-first_name',))
        self.assertAlmostEqual(get_result_list, find_result_list)

        get_result_list = self.db.get_user_list(order_by=('+id',))
        find_result_list = self.db.find_user_list(order_by=('-id',))
        find_result_list.reverse()
        self.assertAlmostEqual(get_result_list, find_result_list)

    def test_class_formatting(self):

        code = model.TestDatabaseModel.pretty_format_class()
        namespace = {}
        exec(code, namespace, namespace)
        database_model_class = namespace.get('TestDatabaseModel')
        self.assertTrue(issubclass(database_model_class, model.database.Database))

        with open('abstraction.py', 'rt') as module_file:
            old_source = module_file.read()

        model.generate(database_model_class=database_model_class)

        with open('abstraction.py', 'rt') as module_file:
            new_source = module_file.read()

        # Ignore the module docstring, since that contains a timestamp
        RX_TIMESTAMP = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        old_source = RX_TIMESTAMP.subn('<TIMESTAMP>', old_source)[1]
        new_source = RX_TIMESTAMP.subn('<TIMESTAMP>', new_source)[1]

        self.assertEqual(old_source, new_source)

    def test_inspection(self):

        from dblayer.backend.postgresql import inspector

        dsn = test_constants.TEST_DSN
        
        db = inspector.DatabaseInspector()
        database_class = db.inspect(dsn, 'InspectedDatabase')
        self.assertTrue(issubclass(database_class, dblayer.model.database.Database))

        source = database_class.pretty_format_class()

        inspected_model_path = os.path.join(MODEL_DIR, 'inspected_model.py')
        with open(inspected_model_path, 'wt') as inspected_model_file:
            inspected_model_file.write(source)

        from dblayer.test import inspected_model

        inspected_database = inspected_model.InspectedDatabase
        self.assertTrue(inspected_database, dblayer.model.database.Database)

        for table in inspected_database._table_list:
            self.assertIsInstance(table, dblayer.model.table.Table)

        model.generate(
            module_path='inspected_abstraction.py',
            database_model_class=inspected_database,
            abstraction_class_name='InspectedDatabase')

        from dblayer.test import inspected_abstraction

        db = inspected_abstraction.InspectedDatabase()
        with db.session(dsn):

            with db.transaction():
                self.load_data(db)
            with db.transaction():
                self.verify_data(db)
            with db.transaction():
                self.modify_data(db)

            for table in inspected_database._table_list:
                with db.transaction():
                    assert isinstance(table, dblayer.model.table.Table)
                    fn_get = getattr(db, 'get_%s' % table._table_name)
                    fn_find = getattr(db, 'find_%s' % table._table_name)
                    fn_update = getattr(db, 'update_%s' % table._table_name, None)
                    record1 = fn_find()
                    if record1 is None:
                        continue
                    fn_update(record1)
                    record2 = fn_get(id=record1.id)
                    if hasattr(record2, 'last_modified'):
                        record2.last_modified = record1.last_modified
                    self.assertEqual(record1, record2)
