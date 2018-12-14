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
from ayx.Package import isPackageInstalled


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
            result = isPackageInstalled(pkg)
            self.assertTrue(result)

    def testNotInstalled(self):
        for pkg in self.not_installed_packages:
            result = isPackageInstalled(pkg)
            self.assertFalse(result)
