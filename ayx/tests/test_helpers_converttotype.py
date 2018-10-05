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
from unittest import TestCase
from ayx.helpers import convertToType
from pandas import DataFrame
from pathlib import Path


class TestConvertToType(TestCase):

    def testStrToInt(self):
        expected = 1
        actual = convertToType("1", int)
        self.assertEqual(expected, actual)

    def testListToPandas(self):
        result = convertToType([1,2], DataFrame)
        self.assertIsInstance(result, DataFrame)

    def testStrToPath(self):
        result = convertToType("c:\\windows\\temp\\myfile.yxdb", Path)
        self.assertIsInstance(result, Path)

    def testTypeError(self):
        self.assertRaises(TypeError, convertToType, "my string", "int")

    def testNotARealType(self):
        try:
            x = convertToType("my string", NotARealType)
            self.fail('Why didn\'t this error? It\'s not even a real type!')
        except:
            pass

    def testStrToDictFail(self):
        self.assertRaises(ValueError, convertToType, "my string", dict)
