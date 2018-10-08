# Copyright (C) 2018 Alteryx, Inc. All rights reserved.
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
