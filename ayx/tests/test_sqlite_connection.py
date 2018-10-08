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
# from sqlalchemy.engine.base import Connection as DbConnection
from sqlite3 import Connection as DbConnection
from unittest import TestCase
from ayx.CachedData import SqliteDb
from ayx.tests.testdata.datafiles import getTestFileName


class TestSqliteConnectionOpen(TestCase):

    def setUp(self):
        self.existing_db_filepath = getTestFileName('single_simple_table')
        self.existing_db = SqliteDb(self.existing_db_filepath, debug=True)
        self.existing_db.openConnection()

    def testConnectionCreated(self):
        self.assertTrue(
            hasattr(self.existing_db, 'connection') and
            type(self.existing_db.connection) is DbConnection
            )

    def testConnectionClosed(self):
        self.existing_db.closeConnection()
        self.assertFalse(
            hasattr(self.existing_db, 'connection') and
            type(self.existing_db.connection) is DbConnection
            )
