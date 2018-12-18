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
from ayx.Datafiles import Datafile
from ayx.tests.testdata.datafiles import getValidSqliteFiles, getTestFileName, getTestFileTables



class TestSqliteGetTableNames(TestCase):

    def setUp(self):
        self.datafiles = getValidSqliteFiles()

    def testGetTableNames(self):
        for datafile in self.datafiles:
            # create temp db connection
            db = Datafile(getTestFileName(datafile))
            db.openConnection()
            # generate expected/actual results
            expected = getTestFileTables(datafile)
            actual = db.getTableNames()
            # close db connection
            db.closeConnection()
            # assert table names match
            self.assertCountEqual(expected, actual)
