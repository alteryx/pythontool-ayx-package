from unittest import TestCase
from ayx.CachedData import tableNameIsValid


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
            'a.d'
        ]

    def testValidNames(self):
        for test_value in self.valid_table_names:
            result = tableNameIsValid(test_value)
            self.assertTrue(
                result,
                msg='Value: {}, Expected: Valid, Result: Invalid'.format(test_value)
                )

    def testInvalidNames(self):
        for test_value in self.invalid_table_names:
            result = tableNameIsValid(test_value)
            self.assertFalse(
                result,
                msg='Value: {}, Expected: Invalid, Result: Valid'.format(test_value)
                )
