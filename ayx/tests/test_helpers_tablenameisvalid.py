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
from ayx.helpers import tableNameIsValid


class TestSqliteConnectionOpen(TestCase):

    def setUp(self):
        self.valid_table_names = [
            'abcdef',
            'abc_123_',
            'z',
            'Z',
            'a___',
            'a1',
            'A1'
            ]
        self.invalid_table_names = [
            '1abcdef',
            '_abcdef',
            'abcdef!',
            '_',
            '!a',
            '1a',
            'abc def',
            'a~',
            'a-',
            'a.',
            'a.d',
            'aѾ',
            'Ѿ'
        ]

    def testValidNames(self):
        for test_value in self.valid_table_names:
            result = tableNameIsValid(test_value)
            self.assertTrue(
                result[0],
                msg='Value: {}, Expected: Valid, Result: Invalid'.format(test_value)
                )

    def testInvalidNames(self):
        for test_value in self.invalid_table_names:
            result = tableNameIsValid(test_value)
            self.assertFalse(
                result[0],
                msg='Value: {}, Expected: Invalid, Result: Valid'.format(test_value)
                )
