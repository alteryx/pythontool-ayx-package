import os
import sqlite3
from unittest import TestCase
from ayx.CachedData import CachedData
from ayx.tests.testdata.datafiles import getTestFileName

class TestCachedDataRead(TestCase):

    def setUp(self):
        self.data = CachedData()

    def testDefaultConfig(self):
        expected = ['#1','#2','1']
        actual = self.data.getIncomingConnectionNames()
        self.assertCountEqual(expected, actual)


class TestCachedDataSpecificConfig(TestCase):

    def setUp(self):
        config_filepath = getTestFileName('config_valid_2')
        self.data = CachedData(config_filepath)

    def testSpecificConfig(self):
        expected = ['VALID2_#1','VALID2_#2','VALID2_1']
        actual = self.data.getIncomingConnectionNames()
        self.assertCountEqual(expected, actual)
