from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName, getTestFileTables

class TestGetSingularTableSuccess(TestCase):

    def setUp(self):
        self.onetable_db_filepath = getTestFileName('single_simple_table')
        self.onetable_db = SqliteDb(self.onetable_db_filepath, debug=True)
        self.tables = getTestFileTables('single_simple_table')
        if len(self.tables) != 1:
            raise Exception('Bad test db setup -- should only contain 1 table')

    def testSuccess(self):
        self.onetable_db.openConnection()
        expected = self.tables[0]
        actual = self.onetable_db.getSingularTable()
        self.onetable_db.closeConnection()
        self.assertEqual(expected, actual)

class TestGetSingularTableZeroTables(TestCase):

    def setUp(self):
        self.zerotable_db_filepath = getTestFileName('zero_tables')
        self.zerotable_db = SqliteDb(self.zerotable_db_filepath, debug=True)
        self.tables = getTestFileTables('zero_tables')
        if len(self.tables) != 0:
            raise Exception('Bad test db setup -- should contain 0 tables')

    def testZeroFail(self):
        self.zerotable_db.openConnection()
        self.assertRaises(Exception, self.zerotable_db.getSingularTable)

class TestGetSingularTableMultipleTables(TestCase):

    def setUp(self):
        self.multitable_db_filepath = getTestFileName('four_tables_cnx_metadata')
        self.multitable_db = SqliteDb(self.multitable_db_filepath, debug=True)
        self.tables = getTestFileTables('four_tables_cnx_metadata')
        if len(self.tables) <= 1:
            raise Exception('Bad test db setup -- should contain >1 tables')

    def testMultiFail(self):
        self.multitable_db.openConnection()
        self.assertRaises(Exception, self.multitable_db.getSingularTable)
