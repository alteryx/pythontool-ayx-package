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
# import sqlalchemy
import sqlite3
from uuid import uuid1
from types import ModuleType as module
import pandas as pd
from matplotlib.figure import Figure
from ayx.helpers import fileErrorMsg, fileExists, deleteFile, tableNameIsValid, convertObjToStr, isDictMappingStrToStr, convertToType
from ayx.DatastreamUtils import MetadataTools, Config, savePlotToFile


class SqliteDb:
    def __init__(self, db_path=None, create_new=False, temporary=False, debug=None):

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

        # default for temporary is false (throw error if file doesnt exist)+++
        if temporary is None:
            self.temporary = False
        elif isinstance(temporary, bool):
            self.temporary = temporary
        else:
            raise TypeError("temporary parameter must be boolean")



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

    def __isConnectionOpen(self, error_if_closed=None):
        if self.debug:
            print('Connection status: {}'.format(self.connection))
        if error_if_closed is None:
            error_if_closed = False
        # if hasattr(self, 'connection') and isinstance(self.connection, sqlalchemy.engine.base.Connection):
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
                # connection = sqlalchemy.create_engine('sqlite:///{}'.format(self.filepath)).connect()
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


    def getData(self, table=None):
        if self.debug:
            print('Attempting to get data from table "{}"'.format(table))

        self.__isConnectionOpen(error_if_closed=True)

        # if no table specified, check to see if there is only table and use that
        if table is None:
            table = self.getSingularTable()

        # now that the table name has been retrieved, get the data as pandas df
        # (but first check that table name is valid to avoid sql injection)
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


    def writeData(self, pandas_df, table, dtype=None):
        if self.debug:
            print('Attempting to write data to table "{}"'.format(table))
        try:
            pandas_df.to_sql(table, self.connection, if_exists='replace', index=False, dtype=dtype)
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
        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug parameter must True or False')

        # obtain input data mappings and workflow constants
        self.config = Config(filepath=config_filepath, debug=debug)


    def __getIncomingConnectionFilepath(self, incoming_connection_name):
        if self.debug:
            print('Attempting to get the cached data filepath for incoming connection "{}"'.format(
                incoming_connection_name
                ))

        input_file_map = self.config.input_file_map

        # error if connection name is not a string
        if not isinstance(incoming_connection_name, str):
            raise TypeError(''.join([
                'Input connection name must be a string value. (eg, "#1")'
                ]))
        # error if connection name is not a named key in the config json (dict)
        elif incoming_connection_name not in input_file_map:
            raise ReferenceError(''.join([
                'The input connection "{}" has not been cached'.format(incoming_connection_name),
                ' -- re-run workflow to refresh the cached data.'
                ]))
        else:
            return input_file_map[incoming_connection_name]



    def getWorkflowConstantNames(self):
        if self.debug:
            print('Attempting to get list of workflow constants')

        return list(self.config.constant_map)

    def getWorkflowConstants(self):
        if self.debug:
            print('Attempting to get list of workflow constants')

        return dict(self.config.constant_map)

    def getWorkflowConstant(self, constant_name):
        if self.debug:
            print('Attempting to get the cached workflow constant "{}"'.format(constant_name))

        # error if constant name is not a string
        if not isinstance(constant_name, str):
            raise TypeError(''.join([
                'Constant name must be a string value, but instead is {}: {}'
                ]).format(
                    type(constant_name),
                    constant_name
                    )
                )

        # if there are no constants
        if len(self.config.constant_map) == 0:
            raise RuntimeError("You must run the workflow first in order to make constants available in this tool")

        # error if connection name is not a named key in the config json (dict)
        # -- but first see if we can offer a suggestion to help the user identify their mistake
        if constant_name not in self.config.constant_map:

            # create a dict mapping of {CONSTANT_NAME: constant_name}
            uppercase_to_actual_constant_names = \
                    {x.upper():x for x in self.config.constant_map}
            # save uppercase constant name to a var so we
            uppercase_constant_name = constant_name.upper()
            # initalize suggested constant name (no suggestion to start)
            suggested = None

            # is it a case error? check to see any keys match with case insensitivity
            if suggested is None:
                if uppercase_constant_name in uppercase_to_actual_constant_names:
                    suggested = uppercase_to_actual_constant_names[uppercase_constant_name]

            # did they forget to add the Engine/Question/User prefix?
            if suggested is None:
                for prefix in ['Engine', 'Question', 'User']:
                    name_with_prefix = '{}.{}'.format(prefix, constant_name).upper()
                    if (name_with_prefix in uppercase_to_actual_constant_names):
                        suggested = uppercase_to_actual_constant_names[uppercase_constant_name]
                        break

            # did they miss some other prefixy part of the constant name?
            if suggested is None:
                for existing_key in self.config.constant_map:
                    if (existing_key.split('.')[-1].upper() ==
                        constant_name.split('.')[-1].upper()
                    ):
                        suggested = existing_key
                        break

            # if we've got a suggestion, throw error and propose suggestion
            if suggested is not None:
                print(' '.join([
                    'The constant "{}" does not exist'.format(constant_name),
                    '-- did you mean "{}" ?'.format(suggested)
                    ]))
            # otherwise, throw a general error that the constant isnt valid
            else:
                print(' '.join([
                    'The constant "{}" does not exist'.format(constant_name),
                    '-- double check the name and run the workflow again to',
                    'refresh the constants available in this tool.'
                    ]))

            raise ReferenceError(
                "Unable to find workflow constant {}".format(constant_name)
                )
        else:
            val = self.config.constant_map[constant_name]
            return val

    def read(self, incoming_connection_name):

        if self.debug:
            print('Attempting to read in cached data for incoming connection "{}"'.format(
                incoming_connection_name
                ))

        # get the filepath of the data
        input_data_filepath = self.__getIncomingConnectionFilepath(
            incoming_connection_name
            )
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

    def __checkOutgoingConnectionNumber__(self, outgoing_connection_number):
        if not isinstance(outgoing_connection_number, int):
            raise TypeError(''.join(
                [msg_prefix,
                 'The outgoing connection number must be an integer value.']
                ))
        # error if connection number is not between 1 and 5
        elif outgoing_connection_number < 1 or outgoing_connection_number > 5:
            raise ValueError('The outgoing connection number must be an integer between 1 and 5')
        return outgoing_connection_number


    def writePlot(self, matplotlib_figure, outgoing_connection_number):
        if self.debug:
            print('Alteryx.writePlot() -- attempting to write out plot to outgoing connection "{}"'.format(
                outgoing_connection_number
                ))

        try:
            outgoing_connection_number = \
                    self.__checkOutgoingConnectionNumber__(
                        outgoing_connection_number
                        )
            if matplotlib_figure is None:
                raise TypeError('A matplotlib plot is required')
            elif not isinstance(matplotlib_figure, Figure):
                raise TypeError('Currently only matplotlib figures can be used to pass plots to outgoing connections in Alteryx')
        except Exception as err:
            print('ERROR: Alteryx.writePlot(matplotlib_figure, outgoing_connection_number):')
            print(err)
            raise

        filename = 'plot_{}'.format(outgoing_connection_number)
        saved_plot_filepath = savePlotToFile(
            matplotlib_figure,
            'plot_{}.png'.format(outgoing_connection_number)
            )


        # create html for img tag
        img_ref_html = '<img src="{}" />'.format(saved_plot_filepath)
        plot_df = pd.DataFrame({'Plot': [img_ref_html]})
        self.write(
            plot_df,
            outgoing_connection_number,
            {'Plot': {'type': 'V_String', 'length': 2147483647}}
            )


    def write(self, pandas_df, outgoing_connection_number, columns=None):

        if self.debug:
            print('Alteryx.write() -- attempting to write out cached data to outgoing connection "{}"'.format(
                outgoing_connection_number
                ))

        try:
            outgoing_connection_number = \
                    self.__checkOutgoingConnectionNumber__(
                        outgoing_connection_number
                        )
            if pandas_df is None:
                raise TypeError('A pandas dataframe is required for passing data to outgoing connections in Alteryx')
            elif not isinstance(pandas_df, pd.core.frame.DataFrame):
                raise TypeError('Currently only pandas dataframes can be used to pass data to outgoing connections in Alteryx')
        except Exception as err:
            print('ERROR: Alteryx.write(pandas_df, outgoing_connection_number):')
            print(err)
            raise


        # get list of columns in input data frame
        pandas_cols = list(pandas_df.columns)

        metadata_tools = MetadataTools(debug=self.debug)
        expected_column_attributes = ['name','type','length']


        renames = {}
        dtypes = {}


        # check optional 'columns' arg
        if columns is None:
            pass
        elif isinstance(columns, dict):
            # loop through each column and put into proper format for
            # corresponding pandas argument
            for col in columns.keys():
                new_column_info = columns[col]

                if 'name' in new_column_info:
                    new_name = new_column_info['name']
                    renames[col] = new_name
                else:
                    new_name = col

                new_type = None
                new_length = None
                if 'type' in new_column_info:
                    # convert Alteryx type to sqlite type
                    new_type = new_column_info['type']
                if 'length' in new_column_info:
                    new_length = new_column_info['length']


                if (new_type is not None):
                    if (new_length is not None):
                        new_type_length = '{} {}'.format(new_type, new_length)
                    else:
                        new_type_length = '{}'.format(new_type)
                elif (new_length is not None):
                    raise ValueError('length cannot be specified without a type')
                else:
                    new_type_length = None


                if new_type_length is not None:
                    db_col_metadata = metadata_tools.convertTypeString(
                        new_type_length,
                        from_context='ayx',
                        to_context='sqlite'
                        )
                    db_col_type_only = db_col_metadata['type']
                    new_length = db_col_metadata['length']


                    # # get length (if any) from metadata update instructions
                    # if 'length' in new_column_info:
                    #     new_length = new_column_info['length']
                    # else:
                    #     new_length = None

                    # concatenate type and length (use default if necessary)
                    db_col_type = metadata_tools.concatTypeLength(
                        db_col_type_only,
                        new_length,
                        context='sqlite'
                        )
                    # set in dtypes dict
                    dtypes[new_name] = db_col_type


        elif isinstance(columns, list):
            pass
        else:
            raise TypeError('columns is optional, but if provided, must be a dict or list')


        if len(dtypes.keys()) == 0:
            dtypes = None

        if len(renames.keys()) == 0:
            renames = None
            pandas_df_out = pandas_df
        else:
            pandas_df_out = pandas_df.rename(columns=renames, inplace=False)


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
                data = db.writeData(pandas_df_out, 'data', dtype=dtypes)
                # print success message
                print(''.join(['SUCCESS: ', msg_action]))
                # return the data
                return data
            except:
                print(''.join(['ERROR: ', msg_action]))
                raise


    def getIncomingConnectionNames(self):
        if self.debug:
            print('Attempting to get all incoming connection names')

        return list(self.config.input_file_map.keys())


    def readMetadata(self, incoming_connection_name):
        if self.debug:
            print('Attempting to get (cached) metadata for for incoming connection "{}"'.format(
                incoming_connection_name
                ))
        # create a flag indicating whether input is a pandas dataframe
        pandas_df_input_flag = \
            isinstance(incoming_connection_name, pd.core.frame.DataFrame)

        # if the input is a dataframe, then write the first row to a temporary
        # sqlite file, and get the metadata from it
        if pandas_df_input_flag:
            input_df_head = incoming_connection_name.head(1)
            temp_table_name = str(uuid1())
            temp_file_path = '.'.join([temp_table_name, 'sqlite'])
            with SqliteDb(
                temp_file_path,
                create_new=True,
                temporary=True,
                debug=self.debug
            ) as db:
                db.writeData(input_df_head, 'data')
                raw_metadata = db.getMetadata()
        # otherwise, if not a dataframe, assume input argument value is a
        # connection name string (function called will validate string type)
        else:
            pandas_df_input_flag = False
            # get the filepath of the data
            input_data_filepath = self.__getIncomingConnectionFilepath(
                incoming_connection_name
                )
            # get the data from the sqlite file
            with SqliteDb(
                input_data_filepath,
                create_new=False,
                debug=self.debug
            ) as db:
                raw_metadata = db.getMetadata()
        # initiate the a MetadataTools object
        metadata_tools = MetadataTools(debug=self.debug)
        metadata_dict = {}
        for index, field in raw_metadata.iterrows():
            if (pandas_df_input_flag):
                if field['name'] == str(input_df_head.columns[index]):
                    field_name = input_df_head.columns[index]
                else:
                    raise ReferenceError(' '.join([
                        'error: pandas dataframe columns appear',
                        'to be in a different order than the correspond',
                        'sqlite data table for some reason...',
                        '> pandas dataframe columns: {}'.format(input_df_head.columns),
                        '> sqlite dataframe columns: {}'.format(list(raw_metadata['name']))
                        ]))
            else:
                field_name = field['name']
            field_type_str = field['type']
            # parse out field type (str) and length (tuple) from string
            field_type_and_length_d = \
                    metadata_tools.parseFieldTypeAndLengthStr(
                        field_type_str,
                        context='sqlite'
                        )
            field_type = field_type_and_length_d['type']
            field_length = field_type_and_length_d['length']
            # set metadata
            conversion = metadata_tools.convertTypeString(
                '{} {}'.format(field_type, field_length),
                from_context='sqlite',
                to_context='ayx'
                )
            metadata_dict[field_name] = {
                'type': conversion['type'],
                'length': conversion['length']
                }
            updated_field_metadata = \
                    metadata_tools.supplementWithDefaultLengths(
                        metadata_dict[field_name]['type'],
                        metadata_dict[field_name]['length'],
                        context='ayx'
                        )
            updated_field_metadata['length'] = \
                    metadata_tools.convertLengthTupleToContext(
                        updated_field_metadata['length'],
                        context='ayx'
                        )
            metadata_dict[field_name] = updated_field_metadata
            metadata_dict[field_name]

        return metadata_dict
