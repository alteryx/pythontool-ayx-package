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
def fileExists(filepath, throw_error=None, msg=None):
    # default is to not throw an error
    if throw_error == None:
        throw_error = False
    if msg == None:
        msg = 'Input data file does not exist'
    # if file exists, return true
    if os.path.isfile(filepath):
        return True
    else:
        if throw_error:
            raise FileNotFoundError(fileErrorMsg(msg, filepath))
        return False


def tableNameIsValid(table_name):
    stripped = ''.join( chr for chr in table_name if (chr.isalnum() or chr=='_'))
    if stripped != table_name:
        valid = False
        reason = 'invalid characters (only alphanumeric and underscores)'
    elif not(table_name[0].isalpha()):
        valid = False
        reason = 'first character must be a letter'
    else:
        valid = True
    return valid


class SqliteDb:
    def __init__(self, dbPath=None, debug=None):

        # if no dbPath specified, throw error
        if dbPath == None:
            raise ReferenceError("No database specified to open")

        # check debug parameter
        if debug==None:
            self.debug = False
        elif type(debug) is not bool:
            raise TypeError('debug parameter must True or False')
        else:
            self.debug = debug

        # set object attributes
        self.filepath = os.path.abspath(dbPath)
        self.connection = None
        # self.openConnection() # ==> self.connection

    # open the connection
    def openConnection(self):
        if not(self.__isConnectionOpen()):
            if self.debug:
                print('Attempting to open connection to {}'.format(self.filepath))
            self.connection = self.__returnConnection()
        # print connection status
        if self.debug:
            print('Connection is open: {}'.format(
                self.__isConnectionOpen(error_if_closed=False)
                ))

    def __isConnectionOpen(self, error_if_closed=None):
        if self.debug:
            print('Connection status: {}'.format(self.connection))
        if error_if_closed==None:
            error_if_closed = False
        if hasattr(self, 'connection') and type(self.connection) is sqlite3.Connection:
            return True
        else:
            if error_if_closed:
                raise AttributeError(fileErrorMsg('sqlite connection is closed', self.filepath))
            return False

    # open database connection, verify that we can execute sql statements
    def __returnConnection(self):
        if fileExists(self.filepath, throw_error=True, msg='Unable to connect to input data'):
            # open connection and attempt to run a quick arbitrary query
            # to confirm that it is a valid sqlite db
            try:
                connection = sqlite3.connect(self.filepath)
                connection.execute("select * from sqlite_master limit 1")
                return connection
            except:
                try:
                    connection.close()
                except:
                    pass
                raise ConnectionError(self.fileErrorMsg(msg, self.filepath))


    # return table names in a list
    def getTableNames(self):
        if self.debug:
            print('Attempting to get table names from {}'.format(self.filepath))

        self.__isConnectionOpen(error_if_closed=True)

        return pd.read_sql_query(
            "select name from sqlite_master where type='table'",
            self.connection
            )['name'].tolist()


    # close database connection
    def closeConnection(self):
        if self.debug:
            print('Attempting to close connection to {}'.format(self.filepath))
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
        self.__isConnectionOpen(error_if_closed=True)

        # if no table specified, check to see if there is only table and use that
        if table == None:
            table = self.getSingularTable()

        # now that the table name has been retrieved, get the data as pandas df
        # (but first check that table name is valid to avoid sql injection)
        if tableNameIsValid(table):
            if self.debug:
                print('Attempting to get data from table "{}"'.format(table))

            return pd.read_sql_query(
                'select * from {}'.format(table),
                self.connection,
                )
        else:
            raise NameError('Invalid table name ({})'.format(table))


class CachedData:
    def __init__(self, debug=None):

        # check debug parameter
        if debug==None:
            self.debug = False
        elif type(debug) is not bool:
            raise TypeError('debug parameter must True or False')
        else:
            self.debug = debug

        # set attributes
        self.config_relative_path = 'config.ini'
        self.config_absolute_path = os.path.abspath(self.config_relative_path)

    # mapping of connection name to filepath {input_connection_name: filepath}
    def __input_file_map(self):

        if self.debug:
            print('Attempting to read in config file containing input connection filepath map {}'.format(
                self.config_absolute_path
                ))

        # check that config exists
        if fileExists(
            self.config_absolute_path,
            True,
            "Cached data unavailable -- run the workflow to make the input data available in Jupyter notebook"
            ):
            try:
                if self.debug:
                    print('Config file found -- {}'.format(self.config_absolute_path))
                with open(self.config_absolute_path) as f:
                    config = json.load(f)
                    input_map = config['input_connections']
                return input_map
            except:
                print('Error: input connection config file is not in the expected format')
                raise


    def read(self, incoming_connection_name):

        if self.debug:
            print('Attempting to read in cached data for incoming connection "{}"'.format(
                incoming_connection_name
                ))

        msg_prefix = 'Alteryx.read(incoming_connection_name): '
        input_file_map = self.__input_file_map()

        if self.debug:
            print('Attempting to find connection "{}" in {}'.format(
                incoming_connection_name,
                self.config_absolute_path
                ))

        # error if connection name is not a string
        if type(incoming_connection_name) is not str:
            raise TypeError(''.join(
                [msg_prefix,
                'Input connection name must be a string value. (eg, "#1")']
                ))
        # error if connection name is not a named key in the config json (dict)
        elif incoming_connection_name not in input_file_map:
            raise ReferenceError(''.join(
                [msg_prefix,
                 'The input connection "{}" has not been cached'.format(incoming_connection_name),
                 ' -- re-run workflow to refresh the cached data.']
                ))
        # if no errors, then get the filepath of the data, read it in, and return the pandas dataframe
        else:
            input_data_filepath = input_file_map[incoming_connection_name]
            # create custom sqlite object
            db = SqliteDb(input_data_filepath, debug=self.debug)
            # open the connection to the sql db
            db.openConnection()
            # get the data from the sql db (if only one table exists, no need to specify the table name)
            data = db.getData()
            # print message about getting the data
            print(''.join(
                    ['Input connection "{}" read in successfully from cached data'.format(
                        incoming_connection_name
                        )]
                ))
            # close the connection
            db.closeConnection()
            # return the data
            return data

    def write(self, pandas_df, outgoing_connection_number):

        if self.debug:
            print('Attempting to write out cached data to outgoing connection "{}"'.format(
                outgoing_connection_number
                ))

        data_output_successfully = False
        msg_prefix = 'Alteryx.write(int): '
        # error if connection number is not an int
        if type(outgoing_connection_number) is not int:
            raise TypeError(''.join(
                [msg_prefix,
                'The outgoing connection number must be an integer value.']
                ))
        # error if connection number is not between 1 and 5
        elif outgoing_connection_number < 1 or outgoing_connection_number > 5:
            raise ValueError('The outgoing connection number must be an integer between 1 and 5')
        elif pandas_df == None:
            raise TypeError('A pandas dataframe is required for passing data to outgoing connections in Alteryx')
        elif type(pandas_df) is not pandas.core.frame.DataFrame:
            raise TypeError('Currently only pandas dataframes can be used to pass data to outgoing connections in Alteryx')
        else:
            pandas_df.to_sql('data', disk_engine, if_exists='append')

    def getIncomingConnectionNames(self):
        return list(self.__input_file_map().keys())
