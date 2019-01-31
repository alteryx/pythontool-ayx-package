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
from ayx.Alteryx import readMetadata


class TestAlteryxRead(TestCase):
    def setUp(self):
        self.metadata = readMetadata("#1")

    def testMetadataTypeIsDict(self):
        self.assertIsInstance(self.metadata, dict)

    def testMetadataContents(self):
        # a_field = list(self.metadata.keys())[0]
        for column_name in self.metadata:
            column = self.metadata[column_name]
            if "type" not in column:
                self.fail("missing type in metadata for column {}".format(column_name))
            if "length" not in column:
                self.fail(
                    "missing length in metadata for column {}".format(column_name)
                )
            if "source" not in column:
                self.fail(
                    "missing source in metadata for column {}".format(column_name)
                )

    def testInvalidInputNumeric(self):
        self.assertRaises(TypeError, readMetadata, 1)

    def testInvalidInputNonexistent(self):
        self.assertRaises(ReferenceError, readMetadata, "#doesnotexist")
