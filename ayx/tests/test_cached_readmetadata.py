import os
import sqlite3
import pandas
from unittest import TestCase
from ayx.CachedData import CachedData
from ayx.tests.testdata.datafiles import getTestFileName

def getInputMetadata(cached_data_obj, input_name_list):
    metadata_d = {}
    for input_name in input_name_list:
        metadata_d[input_name] = cached_data_obj.readMetadata(input_name)
    return metadata_d

class TestCachedDataRead(TestCase):

    def setUp(self):
        self.data = CachedData()
        self.expected_inputs = ['#1', '#2', '1']
        self.metadata_dict = getInputMetadata(self.data, self.expected_inputs)


    def testMetadataReadFail(self):
        self.assertRaises(Exception, self.data.readMetadata, '#doesnotexist')

    def testInputMetadataExists(self):
        for input in self.expected_inputs:
            if input not in self.metadata_dict:
                self.fail('Missing expected input ({}) from cached data')
        # self.assertIsInstance(self.metadata1, dict)

    def testInputMetadataType(self):
        for input in self.expected_inputs:
            input_metadata = self.metadata_dict[input]
            self.assertIsInstance(input_metadata, dict)

    def testInputFieldMetadataAttributes(self):
        for input in self.expected_inputs:
            input_metadata = self.metadata_dict[input]
            for field in input_metadata:
                field_metadata = input_metadata[field]

                if 'type' not in field_metadata:
                    self.fail('type attribute is missing for field {}'.format(field))
                else:
                    self.assertIsInstance(field_metadata['type'], str)

                if 'length' not in field_metadata:
                    self.fail('length attribute is missing for field {}'.format(field))
                else:
                    self.assertIsInstance(field_metadata['length'], (int, float))
