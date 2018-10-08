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
from ayx.Alteryx import getIncomingConnectionNames

class TestAlteryxRead(TestCase):

    def setUp(self):
        self.data = getIncomingConnectionNames()

    def testConnectionNamesIsArray(self):
        result = isinstance(self.data, list)
        self.assertTrue(result)
