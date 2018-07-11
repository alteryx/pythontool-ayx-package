import os
import sqlite3
from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName



class TestSqliteConnectionFails(TestCase):

    def setUp(self):
        self.invalid_db_filepath = getTestFileName('invalid_sqlite_file')
        self.invalid_db = SqliteDb(self.invalid_db_filepath, debug=True)

    def testUnableToOpenConnection(self):
        self.assertRaises(ConnectionError, self.invalid_db.openConnection)



class TestSqliteFileMissing(TestCase):

    def setUp(self):
        self.nonexistent_db_filepath = getTestFileName('nonexistent_sqlite_file')
        try:
            os.remove(self.nonexistent_db_filepath)
        except:
            pass
        self.nonexistent_db = SqliteDb(self.nonexistent_db_filepath, debug=True)

    def testSqliteFileDoesNotExist(self):
        self.assertRaises(FileNotFoundError, self.nonexistent_db.openConnection)
