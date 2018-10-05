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
