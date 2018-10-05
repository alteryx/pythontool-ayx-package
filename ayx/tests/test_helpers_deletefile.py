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
from ayx.helpers import deleteFile, fileExists

class TestAlteryxDeleteFile1(TestCase):

    def setUp(self):
        self.filepath = 'delete_test_1.txt'

    def testWriteDeleteBasic(self):
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # does file exist?
        result = fileExists(self.filepath)
        self.assertFalse(result)


class TestAlteryxDeleteFile2(TestCase):
    def setUp(self):
        self.filepath = 'delete_test_1.txt'
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # create new file in its place
        with open(self.filepath, 'w') as file:
            file.write('test')
        try:
            fileExists(self.filepath, throw_error=True)
        except:
            self.fail("deleteFile() raised an error unexpectedly!")


    def testWriteDeleteWriteDelete(self):
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # does file exist?
        result = fileExists(self.filepath)
        self.assertFalse(result)
