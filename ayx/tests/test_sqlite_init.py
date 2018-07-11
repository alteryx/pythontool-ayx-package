import os
from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName


class TestSqliteInit(TestCase):
    def setUp(self):
        self.existing_db_filepath = getTestFileName('single_simple_table')
        self.existing_db = SqliteDb(self.existing_db_filepath)

    def testFilepathString(self):
        self.assertTrue(
            (type(self.existing_db.filepath) is not None) and
            (type(self.existing_db.filepath) is str) and
            (len(self.existing_db.filepath) > 0)
            )

    def testFilepathExists(self):
        self.assertTrue(
            os.path.isfile(self.existing_db.filepath)
            )
