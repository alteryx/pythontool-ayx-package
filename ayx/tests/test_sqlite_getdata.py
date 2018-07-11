from unittest import TestCase
from pandas.api.types import is_integer_dtype
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName, getTestFileTables

class TestGetDataOneTable(TestCase):

    def setUp(self):
        self.onetable_db_filepath = getTestFileName('single_simple_table')
        with SqliteDb(self.onetable_db_filepath, debug=True) as db:
            self.data = db.getData()

    def testShape(self):
        expected = (4, 5)
        actual = self.data.shape
        self.assertEqual(expected, actual)

    # def testTypesX(self):
    #     self.assertTrue(
    #         is_integer_dtype(data['x'])
    #     )
