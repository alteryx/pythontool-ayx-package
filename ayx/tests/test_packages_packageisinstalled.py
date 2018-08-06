from unittest import TestCase
from ayx.Package import packageIsInstalled


class TestInstalledPackageCheck(TestCase):

    def setUp(self):
        self.installed_packages = [
            'os',
            'pandas',
            'IPython'
            ]
        self.not_installed_packages = [
            'abcdef11'
            ]

    def testInstalled(self):
        for pkg in self.installed_packages:
            result = packageIsInstalled(pkg)
            self.assertTrue(result)

    def testNotInstalled(self):
        for pkg in self.not_installed_packages:
            result = packageIsInstalled(pkg)
            self.assertFalse(result)
