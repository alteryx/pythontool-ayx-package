from unittest import TestCase
from ayx import __version__ as ayx_version
from ayx.Alteryx import __version__ as Alteryx_version

class TestAyxVersion(TestCase):

    def setUp(self):
        self.version = ayx_version

    def testVersionType(self):
        self.assertIsInstance(self.version, str)

    def testVersionLength(self):
        length = len(self.version)
        self.assertTrue(length > 0)

class TestAlteryxVersion(TestCase):

    def setUp(self):
        self.version = Alteryx_version

    def testVersionType(self):
        self.assertIsInstance(self.version, str)

    def testVersionsMatch(self):
        self.assertTrue(self.version == ayx_version)
