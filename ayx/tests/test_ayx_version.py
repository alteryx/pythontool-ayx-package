from unittest import TestCase
from ayx import __version__

class TestVersion(TestCase):

    def setUp(self):
        self.version = __version__

    def testVersionType(self):
        self.assertIsInstance(self.version, str)

    def testVersionLength(self):
        length = len(self.version)
        self.assertTrue(length > 0)
