import os
import sqlite3
import pandas
from unittest import TestCase
from ayx import Alteryx

class TestAlteryxGetWorkFlowConstant(TestCase):

    def setUp(self):
        self.data = "IDoNotNeedAnyData"

    def testGetWorkflowConstant(self):
        winDir = Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")
        self.assertEqual("s:\\Alteryx\\bin_x64\\Debug\\", winDir)

        unixDir = Alteryx.getWorkflowConstant("Engine.ModuleDirectory", windowsToUnixPath=True)
        self.assertEqual("s:/Alteryx/bin_x64/Debug/", unixDir)