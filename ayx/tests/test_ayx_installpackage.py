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
from ayx.tests.testdata.install_test_package_name import test_package
from ayx.Alteryx import installPackage, installPackages
from ayx.Package import packageIsInstalled

class TestAlteryxRead(TestCase):

    def setUp(self):
        self.test_package = test_package
        # attempt to uninstall test package
        try:
            installPackage(self.test_package,'uninstall -y')
        except:
            pass

    def testPackageInstallUninstall(self):
        # attempt to install test package
        try:
            installPackage(self.test_package)
        except:
            self.fail('error installing {}'.format(self.test_package))
        # check if installed
        if not packageIsInstalled(self.test_package):
            self.fail('package was not installed successfully: {}'.format(self.test_package))
        # attempt to uninstall test package
        try:
            installPackage(self.test_package, 'uninstall -y')
        except Exception as e:
            print(e)
            self.fail('error uninstalling {}'.format(self.test_package))
        # check if uninstalled
        if packageIsInstalled(self.test_package):
            self.fail('package was not uninstalled successfully: {}'.format(self.test_package))
