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
from unittest import TestCase
from pandas.api.types import is_integer_dtype
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName, getTestFileTables

class TestGetDataOneTable(TestCase):

    def setUp(self):
        self.onetable_db_filepath = getTestFileName('single_simple_table')
        with SqliteDb(self.onetable_db_filepath, debug=True) as db:
            self.data = db.getData()

    def testShape(self):
        expected = (4, 5)
        actual = self.data.shape
        self.assertEqual(expected, actual)

    # def testTypesX(self):
    #     self.assertTrue(
    #         is_integer_dtype(data['x'])
    #     )
