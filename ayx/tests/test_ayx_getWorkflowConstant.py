import os
import sqlite3
import pandas
from unittest import TestCase
from ayx import Alteryx
from pathlib import Path

class TestAlteryxGetWorkFlowConstant(TestCase):

    def testGetWorkflowConstant(self):
        win_dir = Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")
        self.assertEqual("s:\\Alteryx\\bin_x64\\Debug\\", win_dir)

    def testGetWorkflowConstantDebug(self):
        win_dir = Alteryx.getWorkflowConstant("Engine.WorkflowDirectory", debug=True)
        self.assertEqual("s:\\Alteryx\\bin_x64\\Debug\\", win_dir)
