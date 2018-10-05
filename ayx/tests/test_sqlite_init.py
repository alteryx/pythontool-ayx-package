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
from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName


class TestSqliteInit(TestCase):
    def setUp(self):
        self.existing_db_filepath = getTestFileName('single_simple_table')
        self.existing_db = SqliteDb(self.existing_db_filepath)

    def testFilepathString(self):
        self.assertTrue(
            (type(self.existing_db.filepath) is not None) and
            (type(self.existing_db.filepath) is str) and
            (len(self.existing_db.filepath) > 0)
            )

    def testFilepathExists(self):
        self.assertTrue(
            os.path.isfile(self.existing_db.filepath)
            )
