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
import sqlite3
import pandas as pd
from ayx.helpers import fileErrorMsg, fileExists, deleteFile, tableNameIsValid


class Datafile:
    def __init__(self, db_path=None, create_new=False, temporary=False, fileformat=None, debug=None):

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug parameter must True or False')


        # if no db_path specified, throw error
        if db_path is None:
            raise ReferenceError("No database specified to open")
        try:
            db_abspath = os.path.abspath(db_path)
            if os.path.exists(db_path):
                # file exists
                self.filepath = db_abspath
            else:
                # file does not exist -- check if directory exists
                db_destination_dir = os.path.dirname(db_abspath)
                if os.path.isdir(db_destination_dir):
                    # directory exists, but do we have write access?
                    if os.access(db_destination_dir, os.W_OK):
                        # the file does not exists but write privileges are given
                        self.filepath = db_abspath
                    else:
                        # directory exists, but can not write to it
                        raise PermissionError(
                            'unable to write to directory: {}'\
                                .format(db_destination_dir)
                            )
                else:
                    # file does not exist and directory does not exist
                    raise NotADirectoryError(
                        'unable to write file -- directory does not exist'\
                                .format(db_destination_dir)
                        )
        except:
            print(
                'db_path value ({}) is not a valid filepath'.format(db_path)
                )
            raise


        # default for create_new is false (throw error if file doesnt exist)+++
        if create_new is None:
            self.create_new = False
        elif isinstance(create_new, bool):
            self.create_new = create_new
        else:
            raise TypeError("create_new parameter must be boolean")

        # default for temporary is false (throw error if file doesnt exist)+++
        if temporary is None:
            self.temporary = False
        elif isinstance(temporary, bool):
            self.temporary = temporary
        else:
            raise TypeError("temporary parameter must be boolean")

        # default for fileformat is yxdb (throw error if invalid format)
        valid_formats = ('yxdb', 'sqlite')
        if fileformat is None:
            file_extension = self.filepath.split('.')[-1].lower()
            if file_extension in ('db'):
                self.fileformat = 'sqlite'
            else:
                self.fileformat = file_extension
        elif isinstance(fileformat, str):
            self.fileformat = fileformat
        else:
            raise TypeError(
                'fileformat parameter is {} but must be a str'\
                        .format(type(fileformat))
                )
        if self.fileformat not in valid_formats:
            raise ValueError(''.join([
                'fileformat provided ({}) is invalid -- '.format(fileformat),
                'it must be one of the following values: {}'.format(
                    ', '.join(valid_formats)
                    )
                ]))




        # set object attributes
        self.connection = None

    def __enter__(self):
        self.openConnection()
        return self

    def __exit__(self, type, value, traceback):
        self.closeConnection()
        # if temporary file, attempt to clean up on exit
        if self.temporary:
            try:
                deleteFile(self.filepath, debug=self.debug)
            except:
                # (this is not the end of the world)
                if self.debug:
                    print(''.join([
                        'Unable to delete temp file (will be cleanded up later',
                        'by asset manager) -- {}'.format(self.filepath)
                        ]))

    # open the connection
    def openConnection(self):
        if not self.__isConnectionOpen():
            if self.debug:
                print('Attempting to open connection to {}'.format(self.filepath))
            try:
                self.connection = self.__returnConnection()
            except FileNotFoundError as e:
                print(e)
                raise ReferenceError("You must run the workflow first in order to make a cached copy of the incoming data available for development purposes within this Jupyter notebook.")
        # print connection status
        if self.debug:
            print('Connection is open: {}'.format(
                self.__isConnectionOpen(error_if_closed=False)
                ))

    def __formatNotSupportedYet(self):
        # if you get this error, then it means you haven't finished coding in a new format
        raise Exception('this format ({}) is not fully supported yet'.format(self.fileformat))

    def __isConnectionOpen(self, error_if_closed=None):
        if self.debug:
            print('Connection status: {}'.format(self.connection))
        if error_if_closed is None:
            error_if_closed = False
        if self.fileformat == 'sqlite':
            if hasattr(self, 'connection') and isinstance(self.connection, sqlite3.Connection):
                return True
            else:
                if error_if_closed:
                    raise AttributeError(fileErrorMsg('sqlite connection is closed', self.filepath))
                return False
        elif self.fileformat == 'yxdb':
            # TODO: is there anything to do with this?
            return fileExists(self.filepath, throw_error=False)
        else:
            self.__formatNotSupportedYet()


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
                if self.fileformat == 'sqlite':
                    connection = sqlite3.connect(self.filepath)
                    # check to see if we can get one record to test if its a valid file
                    connection.execute("select * from sqlite_master limit 1")
                elif self.fileformat == 'yxdb':
                    self.__formatNotSupportedYet()
                else:
                    self.__formatNotSupportedYet()
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

        if self.fileformat == 'sqlite':
            self.__isConnectionOpen(error_if_closed=True)
            return pd.read_sql_query(
                "select name from sqlite_master where type='table'",
                self.connection
                )['name'].tolist()
        elif self.fileformat == 'yxdb':
            # if yxdb, return the filename (without extension) as the one table
            filename = os.path.basename(self.filepath)
            table = os.path.splitext(filename)
            return [table]
        else:
            self.__formatNotSupportedYet()


    # close database connection
    def closeConnection(self):
        if self.debug:
            print('Attempting to close connection to {}'.format(self.filepath))
        if hasattr(self, 'connection'):
            if self.fileformat == 'sqlite':
                self.connection.close()
            elif self.fileformat == 'yxdb':
                self.__formatNotSupportedYet()
            else:
                self.__formatNotSupportedYet()
            self.connection = None

    # if one table exists, return its name, otherwise throw error
    def getSingularTable(self):
        if self.debug:
            print('Attempting to find the name of the table in the db (assuming only one table exists)')
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
            table = tables[0]
            if self.debug:
                print('One table was found in db -- the table name is: {}'.format(table))
            return table

    def getMetadata(self, table=None):
        if self.debug:
            print('Attempting to get metadata from table "{}"'.format(table))

        self.__isConnectionOpen(error_if_closed=True)

        # if no table specified, check to see if there is only table and use that
        if table is None:
            table = self.getSingularTable()

        if self.fileformat == 'sqlite':
            table_valid = tableNameIsValid(table)
            if table_valid[0]:
                try:
                    query_result = pd.read_sql_query(
                        'pragma table_info({})'.format(table),
                        self.connection,
                        )
                    if self.debug:
                        print(fileErrorMsg(
                            'Success reading metadata from table "{}" '.format(table),
                            self.filepath))
                    return query_result
                except:
                    print(fileErrorMsg(
                        'Error: unable to read metadata for table "{}"'.format(table),
                        self.filepath))
                    raise
            else:
                raise NameError(''.join([
                    'Invalid table name ({})'.format(table),
                    'Reason: ',
                    table_valid[1]
                    ]))
        elif self.fileformat == 'yxdb':
            self.__formatNotSupportedYet()
        else:
            self.__formatNotSupportedYet()


    def getData(self, table=None):
        if self.debug:
            print('Attempting to get data from table "{}"'.format(table))

        self.__isConnectionOpen(error_if_closed=True)

        # if no table specified, check to see if there is only table and use that
        if table is None:
            table = self.getSingularTable()

        # now that the table name has been retrieved, get the data as pandas df
        # (but first check that table name is valid to avoid sql injection)
        if self.fileformat == 'sqlite':
            table_valid = tableNameIsValid(table)
            if table_valid[0]:
                try:
                    query_result = pd.read_sql_query(
                        'select * from {}'.format(table),
                        self.connection,
                        )
                    if self.debug:
                        print(fileErrorMsg(
                            'Success reading input table "{}" '.format(table),
                            self.filepath))
                    return query_result
                except:
                    print(fileErrorMsg(
                        'Error: unable to read input table "{}"'.format(table),
                        self.filepath))
                    raise

            else:
                raise NameError(''.join([
                    'Invalid table name ({})'.format(table),
                    'Reason: ',
                    table_valid[1]
                    ]))
        elif self.fileformat == 'yxdb':
            self.__formatNotSupportedYet()
        else:
            self.__formatNotSupportedYet()


    def writeData(self, pandas_df, table, dtype=None):
        if self.debug:
            print('Attempting to write data to table "{}"'.format(table))
        try:
            if self.fileformat == 'sqlite':
                pandas_df.to_sql(table, self.connection, if_exists='replace', index=False, dtype=dtype)
            elif self.fileformat == 'yxdb':
                self.__formatNotSupportedYet()
            else:
                self.__formatNotSupportedYet()
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
