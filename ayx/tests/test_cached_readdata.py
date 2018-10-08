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
