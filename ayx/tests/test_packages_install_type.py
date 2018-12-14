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
from subprocess import CalledProcessError
from ayx.tests.testdata.install_test_package_name import test_package
from ayx.Package import installPackages, isPackageInstalled


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
            except TypeError:
                self.fail("install() raised TypeError unexpectedly!")
            except CalledProcessError:
                # expected that install process will fail because these are
                # not real packages
                pass


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
        if isPackageInstalled(self.test_package):
            self.fail('test requires {} to be uninstalled first'.format(self.test_package))


    def testInstallUninstall(self):
        # install test package
        installPackages(self.test_package)
        install_success = isPackageInstalled(self.test_package)
        if not install_success:
            self.fail("install failed for package {}".format(self.test_package))
        # uninstall test package
        installPackages(self.test_package, ['uninstall', '-y'])
        uninstall_success = not isPackageInstalled(self.test_package)
        if not uninstall_success:
            self.fail("uninstall failed. you should manually uninstall {}".format(self.test_package))
