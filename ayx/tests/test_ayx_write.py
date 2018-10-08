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
from ayx.Alteryx import read, write
from ayx.CachedData import SqliteDb
from ayx.helpers import deleteFile, fileExists

def outputFilename(connection_num):
    return 'output_{}.sqlite'.format(connection_num)


class TestAlteryxWrite(TestCase):

    def setUp(self):
        self.output_connections = [1,2,3,4,5]
        # delete pre-existing output data
        for connection_num in self.output_connections:
            deleteFile(outputFilename(connection_num))
        # create a pandas dataframe to write out
        self.data = read("#1")

    def testAyxWriteData(self):
        # loop through all valid connections
        for connection_num in self.output_connections:
            # get filepath
            filepath = os.path.abspath(outputFilename(connection_num))
            # it shouldn't exist (after setup executed)
            if fileExists(filepath):
                raise FileExistsError('File already exists: {}'.format(filepath))
            # write data
            write(self.data, connection_num)
            result = fileExists(filepath)
            self.assertTrue(result)

    def tearDown(self):
        for connection_num in self.output_connections:
            deleteFile(outputFilename(connection_num))

class TestAlteryxWriteContents(TestCase):

    def setUp(self):
        self.connection = 5
        self.filename = outputFilename(self.connection)
        self.data = read("#2")
        deleteFile(self.filename)
        self.test = True

    def testAyxWriteDataResultType(self):
        result = type(write(self.data, self.connection))
        expected = pandas.core.frame.DataFrame
        self.assertEqual(result, expected)

    def testAyxWriteDataRowCount(self):
        write(self.data, self.connection)
        with SqliteDb(self.filename, create_new=False) as result_db:
            rows = pandas.read_sql_query(
                "select count(*) as count from data",
                result_db.connection
                )['count'].tolist()[0]
        self.assertTrue(rows > 0)

    def testAyxWriteDataContents(self):
        write(self.data, self.connection)
        expected = self.data
        with SqliteDb(self.filename, create_new=False) as result_db:
            actual = result_db.getData()
        print(expected.head())
        print(actual.head())
        pandas.testing.assert_frame_equal(expected, actual)

    def tearDown(self):
        deleteFile(self.filename)
