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
from ayx.CachedData import MetadataTools
from ayx.tests.testdata.datafiles import getTestFileName


class TestCachedDataMetadataMethods(TestCase):

    def setUp(self):
        self.metadata_tools = MetadataTools()
        self.expected_contexts = ['ayx','sqlite']

    def testContextValues(self):
        expected = self.expected_contexts
        actual = list(self.metadata_tools.columns['context'])
        self.assertCountEqual(expected, actual)

    def checkFieldLookupsLogic(self, contexts):
        for from_context_name in contexts:
            from_context = contexts[from_context_name]
            for from_field_type_name in from_context:
                from_field_type = from_context[from_field_type_name]

                if 'conversion_types' not in from_field_type:
                    raise LookupError('missing conversion_types attribute in field {}'.format(from_field_type_name))
                for to_context_name in from_field_type['conversion_types']:
                    to_field_type_name_list = \
                            from_field_type['conversion_types'][to_context_name]
                    for field_type_name in to_field_type_name_list:

                        try:
                            if to_context_name not in contexts:
                                raise LookupError('context "{}" does not exist'.format(to_context_name))
                                everything_matches = False
                            if (
                                field_type_name not in contexts[to_context_name]
                            ):
                                print(contexts[to_context_name])
                                raise LookupError('field {} does not have a mapping for {}->{}'.format(field_type_name, from_context_name, to_context_name))
                                everything_matches = False
                        except Exception as err:
                            print('from context: {}, from field type: {}, to context: {}, to field type: {}'.format(from_context_name, from_field_type_name, to_context_name, field_type_name))
                            raise
        return True

    def checkFieldLookups(self, contexts):
        try:
            self.checkFieldLookupsLogic(contexts)
            return {'isvalid': True, 'err': None}
        except Exception as err:
            return {'isvalid': False, 'err': err}

    def testValuesInContext(self):
        field_check_outcome = self.checkFieldLookups(self.metadata_tools.columns['context'])
        if not field_check_outcome['isvalid']:
            print('A discrepency exists between field types and conversion mappings')
            self.fail(field_check_outcome['err'])
            # self.fail('A discrepency exists between field types and conversion mappings')

    def testPartialFieldConversionMap(self):
        fake_ayx_field_type = 'AYXtestType'
        fake_sqlite_field_type = 'SQLiteTestType'
        self.metadata_tools.columns['context']['ayx'][fake_ayx_field_type] = {
            'conversion_types': {
                'sqlite': [fake_sqlite_field_type]
                },
            'expected_length_dim': 1,
            'default_length': (2147483647,)
            }
        # does everything look good? it shouldn't...
        field_check_outcome = self.checkFieldLookups(self.metadata_tools.columns['context'])

        if field_check_outcome['isvalid']:
            self.fail('this should cause an error (due to no reverse lookup existing for field {})'.format(fake_ayx_field_type))
        else:
            err = field_check_outcome['err']
            pass

    def testCompleteFieldConversionMap(self):
        fake_ayx_field_type = 'AYXtestType'
        fake_sqlite_field_type = 'SQLiteTestType'
        self.metadata_tools.columns['context']['ayx'][fake_ayx_field_type] = {
            'conversion_types': {
                'sqlite': [fake_sqlite_field_type]
                },
            'expected_length_dim': 1,
            'default_length': (2147483647,)
            }
        self.metadata_tools.columns['context']['sqlite'][fake_sqlite_field_type] = {
            'conversion_types': {
                'ayx': [fake_ayx_field_type]
                },
            'expected_length_dim': 1,
            'default_length': (2147483647,)
            }
        # does everything look good? it shouldn't...
        field_check_outcome = self.checkFieldLookups(self.metadata_tools.columns['context'])

        if field_check_outcome['isvalid']:
            pass
        else:
            self.fail(field_check_outcome['err'])


    # def testNewField(self):
    #     test_field_name = 'new_field'
    #     self.metadata_tools.columns['context']['ayx'][test_field_name] = {
    #         'conversion_types': {
    #             'type': 'V_WString',
    #             'length': '1000'
    #             },
    #         'expected_length_dim': 1,
    #         'default_length': (2147483647,)
    #         }
