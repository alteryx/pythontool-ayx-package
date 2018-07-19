import os
import json
import sqlite3
import pandas as pd
from ayx.helpers import fileErrorMsg, fileExists, tableNameIsValid, deleteFile, isString




class SqliteDb:
    def __init__(self, db_path=None, create_new=None, debug=None):

        # if no db_path specified, throw error
        if db_path is None:
            raise ReferenceError("No database specified to open")

        # default for create_new is false (throw error if file doesnt exist)+++
        if create_new is None:
            self.create_new = False
        elif isinstance(create_new, bool):
            self.create_new = create_new
        else:
            raise TypeError("create_new parameter must be boolean")

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug parameter must True or False')

        # set object attributes
        self.filepath = os.path.abspath(db_path)
        self.connection = None

    def __enter__(self):
        self.openConnection()
        return self

    def __exit__(self, type, value, traceback):
        self.closeConnection()

    # open the connection
    def openConnection(self):
        if not self.__isConnectionOpen():
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
        if error_if_closed is None:
            error_if_closed = False
        if hasattr(self, 'connection') and isinstance(self.connection, sqlite3.Connection):
            return True
        else:
            if error_if_closed:
                raise AttributeError(fileErrorMsg('sqlite connection is closed', self.filepath))
            return False

    # open database connection, verify that we can execute sql statements
    def __returnConnection(self):
        error_msg = 'Unable to connect to input data'
        # if file exists, and not creating a new db, then throw error
        if (
                fileExists(self.filepath, throw_error=not(self.create_new), msg=error_msg) or
                (self.create_new)
            ):
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
                raise ConnectionError(fileErrorMsg(error_msg, self.filepath))


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
        if table is None:
            table = self.getSingularTable()

        # now that the table name has been retrieved, get the data as pandas df
        # (but first check that table name is valid to avoid sql injection)
        if tableNameIsValid(table):
            if self.debug:
                print('Attempting to get data from table "{}"'.format(table))
            try:
                return pd.read_sql_query(
                    'select * from {}'.format(table),
                    self.connection,
                    )
                if self.debug:
                    print(fileErrorMsg(
                        'Success reading input table "{}" '.format(table),
                        self.filepath))
            except:
                print(fileErrorMsg(
                    'Error: unable to read input table "{}"'.format(table),
                    self.filepath))
                raise

        else:
            raise NameError('Invalid table name ({})'.format(table))


    def writeData(self, pandas_df, table):
        if self.debug:
            print('Attempting to write data to table "{}"'.format(table))
        try:
            pandas_df.to_sql(table, self.connection, if_exists='replace', index=False)
            if self.debug:
                print(fileErrorMsg(
                    'Success writing output table "{}"'.format(table),
                    self.filepath))
            return pandas_df
        except:
            print(fileErrorMsg(
                'Error: unable to write output table "{}"'.format(table),
                self.filepath))
            raise


class CachedData:
    def __init__(self, config_filepath=None, debug=None):

        # default value for config filepath
        if config_filepath is None:
            # config_filepath = 'config.ini'
            config_filepath = 'jupyterPipes.json'

        elif not isinstance(config_filepath, str):
            raise TypeError('config filepath must be a string')

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug parameter must True or False')

        # set attributes
        self.config_filepath = config_filepath
        self.config_absolute_path = os.path.abspath(self.config_filepath)

        # throw error if unable to load input file mapping from config
        try:
            self.__input_file_map()
        except:
            print('Unable to load input data connection config')
            raise

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
                # config is the raw json, input_map is specifically the input mapping
                # (eg, if the mapping is nested below the parent node of a larger config)
                input_map = config # currently, config contains only input mappings
                # check if file is in expected format
                self.__verifyInputConfigStructure(input_map)
                return input_map
            except:
                print('Error: input connection config file is not in the expected format')
                raise

    # verify that the config json is in the expected structure
    def __verifyInputConfigStructure(self, d):

        example_structure = '\n'.join([
            '{',
            '  "#1": "tmp1.sqlite", ',
            '  "#2": "tmp2.sqlite", ',
            '  "union": "tmp3.sqlite" ',
            '}'
            ])

        def configStructureErrorMsg(msg):
            return ''.join([
                msg, '\n\n',
                'Example:', '\n',
                example_structure
            ])

        if not isinstance(d, dict):
            raise TypeError('Input config must be a python dict')
        elif not(all(isinstance(item, str) for item in d.keys())):
            raise ValueError('All input connection names must be strings')
        elif not(all(isinstance(d[item], str) for item in d.keys())):
            raise ValueError('All filenames must be strings')
        else:
            return True

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
        if not isinstance(incoming_connection_name, str):
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
            with SqliteDb(input_data_filepath, debug=self.debug) as db:
                msg_action = 'reading input data "{}"'.format(
                    incoming_connection_name
                    )
                try:
                    # get the data from the sql db (if only one table exists, no need to specify the table name)
                    data = db.getData()
                    # print success message
                    print(''.join(['SUCCESS: ', msg_action]))
                    # return the data
                    return data
                except:
                    print(''.join(['ERROR: ', msg_action]))
                    raise


    def write(self, pandas_df, outgoing_connection_number):

        if self.debug:
            print('Attempting to write out cached data to outgoing connection "{}"'.format(
                outgoing_connection_number
                ))

        data_output_successfully = False
        msg_prefix = 'Alteryx.write(int): '
        # error if connection number is not an int
        if not isinstance(outgoing_connection_number, int):
            raise TypeError(''.join(
                [msg_prefix,
                'The outgoing connection number must be an integer value.']
                ))
        # error if connection number is not between 1 and 5
        elif outgoing_connection_number < 1 or outgoing_connection_number > 5:
            raise ValueError('The outgoing connection number must be an integer between 1 and 5')
        elif pandas_df is None:
            raise TypeError('A pandas dataframe is required for passing data to outgoing connections in Alteryx')
        elif not isinstance(pandas_df, pd.core.frame.DataFrame):
            raise TypeError('Currently only pandas dataframes can be used to pass data to outgoing connections in Alteryx')
        else:
            # create custom sqlite object
            with SqliteDb(
                'output_{}.sqlite'.format(outgoing_connection_number),
                create_new=True,
                debug=self.debug
                ) as db:
                msg_action = 'writing outgoing connection data {}'.format(
                    outgoing_connection_number
                    )
                try:
                    # get the data from the sql db (if only one table exists, no need to specify the table name)
                    data = db.writeData(pandas_df, 'data')
                    # print success message
                    print(''.join(['SUCCESS: ', msg_action]))
                    # return the data
                    return data
                except:
                    print(''.join(['ERROR: ', msg_action]))
                    raise


    def getIncomingConnectionNames(self):
        return list(self.__input_file_map().keys())
