import os
import sqlite3
import pandas
from unittest import TestCase
from ayx.CachedData import CachedData
from ayx.tests.testdata.datafiles import getTestFileName

class TestCachedDataRead(TestCase):

    def setUp(self):
        self.data = CachedData()
        self.data1 = self.data.read('#1')
        self.data2 = self.data.read('#2')
        self.data3 = self.data.read('1')

    def testReadFail(self):
        self.assertRaises(Exception, self.data.read, '#doesnotexist')

    def testReadOutput1(self):
        self.assertIsInstance(self.data1, pandas.core.frame.DataFrame)

    def testReadOutput2(self):
        self.assertIsInstance(self.data2, pandas.core.frame.DataFrame)

    def testReadOutput3(self):
        self.assertIsInstance(self.data3, pandas.core.frame.DataFrame)

    def testInputsAreNotDups(self):
        self.assertFalse(self.data1.shape == self.data2.shape == self.data3.shape)
