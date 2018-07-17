import os
import sqlite3
import pandas
from unittest import TestCase
from ayx.Alteryx import getIncomingConnectionNames

class TestAlteryxRead(TestCase):

    def setUp(self):
        self.data = getIncomingConnectionNames()

    def testConnectionNamesIsArray(self):
        pass
