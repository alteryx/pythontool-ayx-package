import os
import sqlite3
import pandas as pd


class InputData:
    def __init__(self):

        self.sqlite_relative_filepath = 'input_data.sqlite'
        self.sqlite_absolute_filepath = os.path.abspath(sqlite_path)


    # check if sqlite file exists. if not, throw error
    def inputDataExists(self):
        if os.path.isfile(self.sqlite_absolute_filepath):
            return True
        else:
            raise FileNotFoundError(
                ''.join([
                    'Input data file does not exist: ',
                    self.sqlite_absolute_filepath
                    ])
                )
            return False


    # open database connection
    def db_connection(self):
        if self.inputDataExists():
            return sqlite3.connect(self.sqlite_absolute_filepath)






Class Connections
    def __init__(self):
        ''' Constructor for this class '''

        # TODO: identify how many inputs are connected
        self.InputConnections = [1]
