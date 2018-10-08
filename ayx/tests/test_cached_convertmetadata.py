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
import os
from unittest import TestCase
from ayx.helpers import deleteFile, fileExists



# TODO: write tests for ConvertMetadata class and its methods.
# __ayxToSqliteTypeDict:
#   * check that all expected keys exist (and no more)
# __sqliteToAyxTypeDict:
#   * check that all expected keys exist (and no more)
# __getLengthOnly:
#   * check that length is getting returned properly: no length, one digit, 2 digits, 3 digits
# __getTypeOnly:
#   * check some valid values and some invalid values
# __isValidType:
#   * check some valid values and some invalid values
# __contextDict
#   * check some valid values ('ayx, sqlite')
#   * check some invalid values (strings and other types)
