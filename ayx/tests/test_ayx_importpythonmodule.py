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
import os
from unittest import TestCase
from ayx.tests.testdata.datafiles import getTestFileName, getTestFileTables
from ayx.Alteryx import importPythonModule

class TestAlteryxImportPythonFile(TestCase):

    def setUp(self):
        path = getTestFileName('test_python_script')
        self.script_relative_filepath = path
        self.script_absolute_filepath = os.path.abspath(path)

    def testImportPythonScriptRelativePath(self):
        test = importPythonModule(self.script_relative_filepath)
        expected = 1
        result = test.myfunction()
        self.assertEqual(expected, result)

    def testImportPythonScriptAbsolutePath(self):
        test = importPythonModule(self.script_absolute_filepath)
        expected = 1
        result = test.myfunction()
        self.assertEqual(expected, result)


class TestAlteryxImportPythonPackageDir(TestCase):

    def setUp(self):
        path = getTestFileName('test_python_package')
        self.script_relative_filepath = path
        self.script_absolute_filepath = os.path.abspath(path)

    def testImportPythonPackage(self):
        # no submodules
        pkgtest1 = importPythonModule(self.script_absolute_filepath, submodules=False)

        expected = 1
        result = pkgtest1.x
        self.assertEqual(expected, result)

        try:
            pkgtest1.test1.a()
        except AttributeError:
            pass
        else:
            self.fail('shouldve triggered attribute error due to submodule not being loaded')

        # specific submodules
        pkgtest2 = importPythonModule(self.script_absolute_filepath, submodules=['test1','test2'])

        self.assertEqual(pkgtest2.test1.a(), 'test1.a')
        self.assertEqual(pkgtest2.test2.a(), 'test2.a')

        try:
            pkgtest2.xxx.test3.myfunction()
        except AttributeError:
            pass
        else:
            self.fail('shouldve triggered attribute error due to submodule not being loaded')

        # all submodules
        pkgtest3 = importPythonModule(self.script_absolute_filepath)
        self.assertEqual(pkgtest3.xxx.test3.myfunction(), 'xxx.test3.myfunction')
