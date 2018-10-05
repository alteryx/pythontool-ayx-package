# (C) Copyright 2018 Alteryx, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
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
