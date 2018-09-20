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
