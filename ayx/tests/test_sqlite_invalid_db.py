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
from unittest import TestCase
from ayx.Datafiles import Datafile
from ayx.tests.testdata.datafiles import getTestFileName



class TestSqliteConnectionFails(TestCase):

    def setUp(self):
        self.invalid_db_filepath = getTestFileName('invalid_sqlite_file')
        self.invalid_db = Datafile(self.invalid_db_filepath, debug=True)

    def testUnableToOpenConnection(self):
        self.assertRaises(ConnectionError, self.invalid_db.openConnection)



class TestSqliteFileMissing(TestCase):

    def setUp(self):
        self.nonexistent_db_filepath = getTestFileName('nonexistent_sqlite_file')
        try:
            os.remove(self.nonexistent_db_filepath)
        except:
            pass
        self.nonexistent_db = Datafile(self.nonexistent_db_filepath, debug=True)

    def testSqliteFileDoesNotExist(self):
        self.assertRaises(ReferenceError, self.nonexistent_db.openConnection)
