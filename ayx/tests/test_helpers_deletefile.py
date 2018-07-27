import os
from unittest import TestCase
from ayx.helpers import deleteFile, fileExists

class TestAlteryxDeleteFile1(TestCase):

    def setUp(self):
        self.filepath = 'delete_test_1.txt'

    def testWriteDeleteBasic(self):
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # does file exist?
        result = fileExists(self.filepath)
        self.assertFalse(result)


class TestAlteryxDeleteFile2(TestCase):
    def setUp(self):
        self.filepath = 'delete_test_1.txt'
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # create new file in its place
        with open(self.filepath, 'w') as file:
            file.write('test')
        try:
            fileExists(self.filepath, throw_error=True)
        except:
            self.fail("deleteFile() raised an error unexpectedly!")


    def testWriteDeleteWriteDelete(self):
        # delete the file to start with clean slate
        deleteFile(self.filepath)
        # does file exist?
        result = fileExists(self.filepath)
        self.assertFalse(result)
