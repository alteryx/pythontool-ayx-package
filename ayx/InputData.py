import os
import sqlite3
import pandas as pd


class InputData:
    def __init__(self):

        self.sqlite_relative_filepath = 'input_data.sqlite'
        # self.sqlite_relative_filepath = 'test.sqlite'
        self.input_connection_metadata_table = 'input_connection_metadata'
        self.sqlite_absolute_filepath = os.path.abspath(self.sqlite_relative_filepath)


    # check if sqlite file exists. if not, throw error
    def inputDataExists(self):
        if os.path.isfile(self.sqlite_absolute_filepath):
            return True
        else:
            raise FileNotFoundError(
                self.dbCheckError('Input data file does not exist')
                )
            return False


    # return a msg followed by db absolute path
    def dbCheckError(self, msg):
        return ''.join([msg, ' (', self.sqlite_absolute_filepath, ')'])

    # open database connection
    def openConnection(self):
        if self.inputDataExists():
            try:
                self.db = sqlite3.connect(self.sqlite_absolute_filepath)
            except:
                raise ConnectionRefusedError(
                    self.dbCheckError('Unable to connect to input data')
                    )

    # close database connection
    def closeConnection(self):
        self.db.close()
        del self.db

    # get list of table names in sqlite db
    def getDbTableNames(self):
        self.openConnection()
        # get tables from db -> pandas df -> python list
        try:
            return pd.read_sql_query(
            "select name from sqlite_master where type='table'",
            self.db
            )['name'].tolist()
        except:
            raise TypeError(self.dbCheckError("Not a valid sqlite database"))

    def getDbTableColumns(self, table_name):
        self.openConnection()
        try:
            return pd.read_sql_query(
            'PRAGMA table_info({})'.format(table_name),
            self.db
            )['name'].tolist()
        except:
            raise LookupError(self.dbCheckError(
                'Unable to lookup column names on table "{}"'.format(table_name)
                ))


    # get mapping of connection name to table name from input metadata
    def getInputConnections(self):
        # check if input metadata exists
        if not(self.input_connection_metadata_table in self.getDbTableNames()):
            raise LookupError(self.dbCheckError(
                'Input db is missing connection metadata table: {}'.format(
                    self.input_connection_metadata_table
                    )
                ))
        else:
            # get list of columns in metadata table from db
            metadata_columns = self.getDbTableColumns(self.input_connection_metadata_table)
            # set expected column names in metadata table
            metadata_column_input_connection_name = 'input_connection_name'
            metadata_column_table = 'table'
            expected_columns = [
                metadata_column_input_connection_name,
                metadata_column_table
                ]
            # check that expected columns exist
            for col in expected_columns:
                if not(col in metadata_columns):
                    raise LookupError(self.dbCheckError(
                        'Expected column "{}" not in table "{}"'.format(
                            col,
                            self.input_connection_metadata_table
                            )
                        ))
            # get the input connection metadata values ...
            metadata_df = pd.read_sql_query(
                'select * from {}'.format(self.input_connection_metadata_table),
                self.db
                )
            # and put metadata values in a dictionary {connection name: table}
            metadata_dict = {}
            for row in metadata_df.to_dict('records'):
                input_connection_value = row[metadata_column_input_connection_name]
                table_name_value = row[metadata_column_table]
                metadata_dict[input_connection_value] = table_name_value
            # return lookup dictionary
            return metadata_dict
