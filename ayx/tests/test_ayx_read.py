import os
import sqlite3
import pandas
from unittest import TestCase
from ayx.Alteryx import read

class TestAlteryxRead(TestCase):

    def setUp(self):
        self.data = read('#1')

    def testReadTypeIsDataframe(self):
        self.assertIsInstance(self.data, pandas.core.frame.DataFrame)

    def testReadNotEmpty(self):
        has_rows = self.data.shape[0] > 0
        has_columns = self.data.shape[1] > 0
        self.assertTrue(has_columns and has_rows)

    def testInvalidInputNumeric(self):
        self.assertRaises(TypeError, read, 1)

    def testInvalidInputNonexistent(self):
        self.assertRaises(ReferenceError, read, '#doesnotexist')
