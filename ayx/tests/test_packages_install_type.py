from unittest import TestCase
from ayx.tests.testdata.install_test_package_name import test_package
from ayx.Package import installPackages, packageIsInstalled


class TestPackageStrOrList(TestCase):

    def setUp(self):
        self.valid_values = [
            'abc',
            ['abc'],
            ['abc','def']
            ]
        self.invalid_values = [
            [['xyz']],
            ['x', 'y', 3],
            {'xyz':1},
            1,
            None
        ]

    def testValidType(self):
        for value in self.valid_values:
            try:
                installPackages(value)
            except TypeError as ex:
                self.fail("install() raised TypeError unexpectedly!")

    def testInvalidType(self):
        for value in self.invalid_values:
            self.assertRaises(TypeError, installPackages, value)

class TestPackageUninstallReinstall(TestCase):

    def setUp(self):
        self.test_package = test_package
        # attempt to uninstall package
        try:
            installPackages(self.test_package, ['uninstall', '-y'])
        except:
            pass
        # if the package is still available, throw an error
        if packageIsInstalled(self.test_package):
            self.fail('test requires {} to be uninstalled first'.format(self.test_package))


    def testInstallUninstall(self):
        # install test package
        installPackages(self.test_package)
        install_success = packageIsInstalled(self.test_package)
        if not install_success:
            self.fail("install failed for package {}".format(self.test_package))
        # uninstall test package
        installPackages(self.test_package, ['uninstall', '-y'])
        uninstall_success = not packageIsInstalled(self.test_package)
        if not uninstall_success:
            self.fail("uninstall failed. you should manually uninstall {}".format(self.test_package))
