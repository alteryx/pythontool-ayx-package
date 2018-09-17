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

        python_path = Alteryx.getWorkflowConstant("Engine.ModuleDirectory", return_as_path=True)
        expected_path = Path("s:\\Alteryx\\bin_x64\\Debug\\")
        self.assertEqual(expected_path, python_path)

    def testGetWorkflowConstant(self):
        python_path = Alteryx.getWorkflowConstant("Engine.ModuleDirectory", return_as_path=True)
        expected_path_2 = Path('s:/')
        expected_path_2 = expected_path_2 / "Alteryx" / "bin_x64" / "Debug"
        self.assertEqual(expected_path_2, python_path)