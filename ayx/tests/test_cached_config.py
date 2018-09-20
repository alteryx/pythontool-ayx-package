import os
import sqlite3
from unittest import TestCase
from ayx.CachedData import CachedData
from ayx.tests.testdata.datafiles import getTestFileName
from pathlib import Path

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

class TestNoInputs(TestCase):
    def setUp(self):
        file = getTestFileName('no_input_data_config')
        self.data = CachedData(file)

    def testNoInput(self):
        expected = []
        actual = self.data.getIncomingConnectionNames()
        self.assertEqual(expected, actual)


class TestWorkflowConstants(TestCase):
    def setUp(self):
        self.data  = CachedData()

    def testGetStringConstant(self):
        result = self.data.getWorkflowConstant("Engine.GuiInteraction")
        self.assertEqual("1", result)

    def testGetIntConstant(self):
        result = self.data.getWorkflowConstant("Engine.IterationNumber")
        self.assertIsInstance(result, int)

    def testGetFloatConstant(self):
        result = self.data.getWorkflowConstant("User.number2")
        self.assertIsInstance(result, float)

    def testGetIntConstantValue(self):
        expected = 0
        actual = self.data.getWorkflowConstant("Engine.IterationNumber")
        self.assertEqual(expected, actual)

    def testConstantMissingPrefix(self):
        self.assertRaises(LookupError, self.data.getWorkflowConstant, "IterationNumber")

    def testNonexistentConstant(self):
        self.assertRaises(ReferenceError, self.data.getWorkflowConstant, "Not A Constant")

    def testPathStringConstant(self):
        win_dir = self.data.getWorkflowConstant("Engine.ModuleDirectory")
        self.assertEqual("s:\\Alteryx\\bin_x64\\Debug\\", win_dir)
