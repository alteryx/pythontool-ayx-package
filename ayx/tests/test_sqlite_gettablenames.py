from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getValidSqliteFiles, getTestFileName, getTestFileTables



class TestSqliteGetTableNames(TestCase):

    def setUp(self):
        self.datafiles = getValidSqliteFiles()

    def testGetTableNames(self):
        for datafile in self.datafiles:
            # create temp db connection
            db = SqliteDb(getTestFileName(datafile))
            db.openConnection()
            # generate expected/actual results
            expected = getTestFileTables(datafile)
            actual = db.getTableNames()
            # close db connection
            db.closeConnection()
            # assert table names match
            self.assertCountEqual(expected, actual)
