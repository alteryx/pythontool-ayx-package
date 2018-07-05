import os
import json
import sqlite3
import pandas as pd


# return a string containing a msg followed by a filepath
def fileErrorMsg(msg, filepath=None):
    if filepath == None:
        raise ReferenceError("No filepath provided")
    return ''.join([msg, ' (', filepath, ')'])


# check if file exists. if not, throw error
def fileExists(filepath, throw_error=None):
    # default is to not throw an error
    if throw_error == None:
        throw_error = False
    # if file exists, return true
    if os.path.isfile(filepath):
        return True
    else:
        if throw_error:
            raise FileNotFoundError(
                self.fileErrorMsg('Input data file does not exist', filepath)
                )
        return False


def tableNameIsValid(table_name):
    valid = True
    stripped = ''.join( chr for chr in table_name if (chr.isalnum() or chr=='_'))
    if stripped != table_name:
        valid = False
    elif not(table_name[0].isalpha()):
        valid = False
    return valid


class SqliteDb:
    def __init__(self, dbPath=None):

        # if no dbPath specified, throw error
        if dbPath == None:
            raise ReferenceError("No database specified to open")

        # set object attributes
        self.filepath = os.path.abspath(dbPath)
        self.connection = None
        self.setConnection() # ==> self.connection


    def setConnection(self):
        if not(hasattr(self, 'connection')) or (type(self.connection) is not sqlite3.Connection):
            self.connection = self.returnConnection()

    # open database connection, verify that we can execute sql statements, confirm only 1 table
    def returnConnection(self):
        if fileExists(self.filepath, throw_error=True):
            # open connection and attempt to run a quick arbitrary query
            # to confirm that it is a valid sqlite db
            try:
                connection = sqlite3.connect(self.filepath)
                connection.execute("select * from sqlite_master limit 1")
                return connection
            except:
                connection.close()
                raise ConnectionError(
                    self.fileErrorMsg('Unable to connect to input data', self.filepath)
                    )


    # return table names in a list
    def getTableNames(self):
        return pd.read_sql_query(
            "select name from sqlite_master where type='table'",
            self.connection
            )['name'].tolist()


    # close database connection
    def closeConnection(self):
        if hasattr(self, 'connection'):
            self.connection.close()
            self.connection = None

    # if one table exists, return its name, otherwise throw error
    def getSingularTable(self):
            tables = self.getTableNames()
            table_count = len(tables)
            # if no tables exist throw error
            if table_count == 0:
                raise ValueError(fileErrorMsg(
                    'Db does not contain any tables',
                    self.filepath
                    ))
            # if multiple tables exist, throw error
            elif table_count > 1:
                raise ValueError(fileErrorMsg(
                    'Db should only contain 1 table, but instead has multiple: {}'.format(tables),
                    self.filepath
                    ))
            # return table name only if only one table exists
            elif table_count == 1:
                return tables[0]


    def getData(self, table=None):
        # if no table specified, check to see if there is only table and use that
        if table == None:
            table = self.getSingularTable()
        # now that the table name has been retrieved, get the data as pandas df
        # (but first check that table name is valid to avoid sql injection)
        if tableNameIsValid(table):
            return pd.read_sql_query(
                'select * from {}'.format(table),
                self.connection,
                )
        else:
            raise NameError('Invalid table name ({})'.format(table))



class InputData:
    def __init__(self):
        self.config_relative_path = 'config.ini'
        self.config_absolute_path = os.path.abspath(self.config_relative_path)





# with open('config.ini') as f:
#     data = json.load(f)
