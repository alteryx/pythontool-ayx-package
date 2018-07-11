import os
import sqlite3
from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName


class TestSqliteConnectionOpen(TestCase):

    def setUp(self):
        self.existing_db_filepath = getTestFileName('single_simple_table')
        self.existing_db = SqliteDb(self.existing_db_filepath, debug=True)
        self.existing_db.openConnection()

    def testConnectionCreated(self):
        self.assertTrue(
            hasattr(self.existing_db, 'connection') and
            type(self.existing_db.connection) is sqlite3.Connection
            )

    def testConnectionClosed(self):
        self.existing_db.closeConnection()
        self.assertFalse(
            hasattr(self.existing_db, 'connection') and
            type(self.existing_db.connection) is sqlite3.Connection
            )
