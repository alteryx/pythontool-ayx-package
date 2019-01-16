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


import os, re, builtins, numpy
import sqlite3
import pandas as pd
from ayx.helpers import fileErrorMsg, fileExists, deleteFile, tableNameIsValid
from ayx.Compiled import pyxdb, pyxdbLookupFieldTypeEnum

# from ayx.DatastreamUtils import MetadataTools


class FileFormat:
    def __init__(self, filepath=None, fileformat=None):
        # metadata for data formats
        data_formats = {
            "yxdb": {"connection_class": pyxdb.AlteryxYXDB},
            "sqlite": {"connection_class": sqlite3.Connection},
        }
        self.valid_formats = list(data_formats)

        if filepath is not None:
            # if no fileformat provided, then try to guess based on the file extension
            # (either way, throw an error if invalid format)
            self.extension = filepath.split(".")[-1].lower()
            if fileformat is None:
                if self.extension in ("db"):
                    self.filetype = "sqlite"
                else:
                    self.filetype = self.extension
            elif isinstance(fileformat, str):
                self.filetype = fileformat
            else:
                raise TypeError(
                    "fileformat parameter is {} but must be a str".format(
                        type(fileformat)
                    )
                )

            if self.filetype not in self.valid_formats:
                raise ValueError(
                    "".join(
                        [
                            "fileformat provided ({}) is invalid -- ".format(
                                fileformat
                            ),
                            "it must be one of the following values: {}".format(
                                ", ".join(self.valid_formats)
                            ),
                        ]
                    )
                )
            self.connection_class = data_formats[self.filetype]["connection_class"]


class Datafile:
    def __init__(
        self,
        db_path=None,
        create_new=False,
        temporary=False,
        fileformat=None,
        debug=None,
    ):

        fresh_dir = dir(self)

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError("debug parameter must True or False")

        # default for create_new is false (throw error if file doesnt exist)+++
        if create_new is None:
            # self.create_new = False
            self.create_new = None
        elif isinstance(create_new, bool):
            self.create_new = create_new
        else:
            raise TypeError("create_new parameter must be boolean or None (auto)")

        # if no db_path specified, throw error
        if db_path is None:
            raise ReferenceError("No database specified to open")
        # otherwise, check if it exists or if the path exists
        try:
            db_abspath = os.path.abspath(db_path)
            if os.path.exists(db_path):
                # file exists...
                if self.create_new is None:
                    self.create_new = False
                elif (self.create_new == True) and (self.debug):
                    print(
                        "Datafile WARNING: create_new=True, but file already exists ({})".format(
                            db_abspath
                        )
                    )
                self.filepath = db_abspath
            else:
                # file does not exist...
                if self.create_new is None:
                    self.create_new = True
                elif (self.create_new == False) and (self.debug):
                    print(
                        "Datafile WARNING: create_new=False, but file does not exist ({})".format(
                            db_abspath
                        )
                    )
                # does the directory exist?
                db_destination_dir = os.path.dirname(db_abspath)
                if os.path.isdir(db_destination_dir):
                    # directory exists, but do we have write access?
                    if os.access(db_destination_dir, os.W_OK):
                        # the file does not exists but write privileges are given
                        self.filepath = db_abspath
                    else:
                        # directory exists, but can not write to it
                        raise PermissionError(
                            "unable to write to directory: {}".format(
                                db_destination_dir
                            )
                        )
                else:
                    # file does not exist and directory does not exist
                    raise NotADirectoryError(
                        "unable to write file -- directory does not exist".format(
                            db_destination_dir
                        )
                    )
        except:
            print("db_path provided ({}) is not valid".format(db_path))
            raise

        # default for temporary is false (throw error if file doesnt exist)+++
        if temporary is None:
            self.temporary = False
        elif isinstance(temporary, bool):
            self.temporary = temporary
        else:
            raise TypeError("temporary parameter must be boolean")

        self.fileformat = FileFormat(self.filepath, fileformat)
        self.connection = None

        if self.debug:
            for key in dir(self):
                if key not in fresh_dir:
                    value = eval("self.{}".format(key))
                    print("DataFile.{}: {}".format(key, value))

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
                    print(
                        "".join(
                            [
                                "Unable to delete temp file (will be cleanded up later",
                                "by asset manager) -- {}".format(self.filepath),
                            ]
                        )
                    )

    # open the connection
    def openConnection(self):
        if not self.__isConnectionOpen():
            if self.debug:
                print("Attempting to open connection to {}".format(self.filepath))
            try:
                self.connection = self.__returnConnection()
                if self.connection is None:
                    del self.connection
            except FileNotFoundError as e:
                print(e)
                raise ReferenceError(
                    "You must run the workflow first in order to make a cached copy of the incoming data available for development purposes within this Jupyter notebook."
                )
        # print connection status
        if self.debug:
            print(
                "Connection is open: {}".format(
                    self.__isConnectionOpen(error_if_closed=False)
                )
            )

    def __formatNotSupportedYet(self):
        # if you get this error, then it means you haven't finished coding in a new format
        raise Exception(
            "this format ({}) is not fully supported yet".format(
                self.fileformat.filetype
            )
        )

    def __isConnectionOpen(self, error_if_closed=None):
        if self.debug:
            if hasattr(self, "connection"):
                print("Connection status: {}".format(self.connection))
            else:
                print("Connection status: [doesn't exist]")
        if error_if_closed is None:
            error_if_closed = False
        # if there's not even a connection attribute, return false
        if not hasattr(self, "connection"):
            connection_is_open = False
        elif self.connection is None:
            connection_is_open = False
        else:
            if isinstance(self.connection, self.fileformat.connection_class):
                connection_is_open = True
            else:
                raise AttributeError(
                    "\n".join(
                        [
                            "connection type appears to be invalid:",
                            "> Expected: {}".format(self.fileformat.connection_class),
                            "> Actual: {}".format(type(self.connection)),
                        ]
                    )
                )

        if error_if_closed and not (connection_is_open):
            raise AttributeError(
                fileErrorMsg("datafile connection is closed", self.filepath)
            )

        return connection_is_open

    def __createConnection(self, metadata=None):
        if self.debug:
            print("Attempting to create new data connection: {}".format(self.filepath))
        error_msg = "Unable to create new data connection"
        try:
            if self.fileformat.filetype == "sqlite":
                if metadata is not None:
                    raise ValueError(
                        "metadata not currently supported for creating an empty sqlite file"
                    )
                connection = sqlite3.connect(self.filepath)
                connection.execute("select * from sqlite_master limit 1")
                self.connection = connection
            elif self.fileformat.filetype == "yxdb":
                if metadata is None:
                    raise ValueError(
                        "metadata is currently required for creating an empty yxdb file"
                    )
                # if given a dict, then convert it to list format
                if self.debug:
                    print("DataFile.__createConnection() -- input metadata:")
                    print(metadata)
                if isinstance(metadata, dict):
                    metadata_list = []
                    if self.debug:
                        print("---")
                        print("metadata: {}".format(metadata))
                    for key in metadata:
                        if self.debug:
                            print("key: {}".format(key))
                        metadata_d = {"name": key}
                        metadata_attr = metadata[key]
                        if self.debug:
                            print("metadata_attr: {}".format(metadata_attr))
                        for attr in metadata_attr:
                            metadata_d[attr] = metadata_attr[attr]
                        metadata_list.append(metadata_d)
                    print("---")
                    if self.debug:
                        print("DataFile.__createConnection() -- adjusted metadata:")
                        print(metadata_list)
                else:
                    metadata_list = metadata
                if not isinstance(metadata_list, list):
                    raise TypeError(
                        "AlteryxYXDB().create_from_dict() requires a list (ironically) of metadata, with each element being a dict representing a column"
                    )

                self.connection = pyxdb.AlteryxYXDB()
                self.connection.create_from_dict(self.filepath, metadata)
            else:
                self.__formatNotSupportedYet()

            if self.debug:
                print(
                    "Successfully created new data connection: {}".format(self.filepath)
                )

        except Exception as err:
            try:
                connection.close()
            except:
                pass
            print(err)
            print(fileErrorMsg(error_msg, self.filepath))
            raise

    # open database connection, verify that we can execute sql statements
    def __returnConnection(self):
        error_msg = "Unable to connect to input data"
        try:
            # if file exists, and not creating a new db, then throw error
            if self.create_new:
                if os.path.isfile(self.filepath):
                    try:
                        deleteFile(self.filepath, debug=self.debug)
                    except:
                        raise PermissionError(
                            "unable to delete file: {}".format(self.filepath)
                        )

                    if not os.access(os.path.dirname(self.filepath), os.W_OK):
                        raise PermissionError(
                            "unable to write to filepath: {}".format(self.filepath)
                        )
                    return None

            elif fileExists(
                self.filepath, throw_error=not (self.create_new), msg=error_msg
            ):
                # open connection and attempt to read one row
                # to confirm that it is a valid file

                if self.fileformat.filetype == "sqlite":
                    connection = sqlite3.connect(self.filepath)
                    connection.execute("select * from sqlite_master limit 1")
                elif self.fileformat.filetype == "yxdb":
                    connection = pyxdb.AlteryxYXDB()
                    connection.open(self.filepath)
                    connection.read_record()
                    # now that we've read a record and moved the line pointer
                    # past the first line, we need to ,move it back
                    connection.go_record(0)
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
            print("Attempting to get table names from {}".format(self.filepath))

        self.__isConnectionOpen(error_if_closed=True)
        if self.fileformat.filetype == "sqlite":
            return pd.read_sql_query(
                "select name from sqlite_master where type='table'", self.connection
            )["name"].tolist()
        elif self.fileformat.filetype == "yxdb":
            # if yxdb, return the filename (without extension) as the one table
            filename = os.path.basename(self.filepath)
            table = os.path.splitext(filename)[0]
            return [table]
        else:
            self.__formatNotSupportedYet()

    # close database connection
    def closeConnection(self):
        if self.debug:
            print("Attempting to close connection to {}".format(self.filepath))
        if hasattr(self, "connection"):
            if self.fileformat.filetype == "sqlite":
                self.connection.close()
            elif self.fileformat.filetype == "yxdb":
                try:
                    self.connection.close()
                except:
                    pass
                del self.connection
            else:
                self.__formatNotSupportedYet()
            self.connection = None

    def __validateTableName(self, table):
        if self.fileformat.filetype == "sqlite":
            table_valid = tableNameIsValid(table)
            if not table_valid[0]:
                raise NameError(
                    "".join(
                        [
                            "Invalid table name ({})".format(table),
                            "Reason: ",
                            table_valid[1],
                        ]
                    )
                )

    # if one table exists, return its name, otherwise throw error
    def getSingularTable(self):
        if self.debug:
            print(
                "Attempting to find the name of the table in the Datafile (assuming only one table exists)"
            )
        tables = self.getTableNames()
        table_count = len(tables)
        # if no tables exist throw error
        if table_count == 0:
            raise ValueError(
                fileErrorMsg("Datafile does not contain any tables", self.filepath)
            )
        # if multiple tables exist, throw error
        elif table_count > 1:
            raise ValueError(
                fileErrorMsg(
                    "Datafile should only contain 1 table, but instead has multiple: {}".format(
                        tables
                    ),
                    self.filepath,
                )
            )
        # return table name only if only one table exists
        elif table_count == 1:
            table = tables[0]
            if self.debug:
                print(
                    "One table was found in Datafile -- the table name is: {}".format(
                        table
                    )
                )
            return table

    def getMetadata(self, table=None):
        if self.debug:
            print('Attempting to get metadata from table "{}"'.format(table))

        self.__isConnectionOpen(error_if_closed=True)

        if self.fileformat.filetype == "yxdb" and table is not None:
            raise ValueError(
                " ".join(
                    [
                        "specifying a table name ({})".format(table),
                        "for a yxdb file ({}) does not make sense".format(
                            self.filename
                        ),
                    ]
                )
            )
        # if no table specified, check to see if there is only table and use that
        if table is None:
            table = self.getSingularTable()

        self.__validateTableName(table)

        try:
            if self.fileformat.filetype == "sqlite":
                query = "pragma table_info({})"
                if self.debug:
                    print("getMetadata (sqlite query): {}".format(query))
                query_result = pd.read_sql_query(query.format(table), self.connection)
                if self.debug:
                    print("getMetadata (sqlite query result): {}".format(query_result))
                # now massage that pragma query result into our expected format...
                column_metadata_list = []
                for index, field in query_result.iterrows():
                    field_dict = {
                        "name": field["name"],
                        # type is a string concatenation of "{type} {size}.{scale}"
                        "type": field["type"],
                        # source and description are lost/unavailable with sqlite
                        "source": "",
                        "description": "",
                    }
                    column_metadata_list.append(field_dict)
            elif self.fileformat.filetype == "yxdb":

                # reset pointer back to first line
                self.openConnection()
                raw_metadata = self.connection.get_record_meta()

                # now massage the metadata returned into our expected format...
                column_metadata_list = []
                for column_metadata in raw_metadata:
                    # concatenate type, size, and scale appropriately
                    # (because this is what was expected with sqlite... the value
                    # is then parsed, validated, and transformed. it works, and
                    # it doesn't seem rewriting for this because getting metadata
                    # is something that happens with relative infrequency)
                    type_name = str(column_metadata["type"])
                    size = column_metadata["size"]
                    scale = column_metadata["scale"]
                    if column_metadata["scale"] > 0:
                        length = "{}.{}".format(size, scale)
                    else:
                        length = str(size)
                    type_length = "{} ({})".format(type_name, length)

                    field_dict = {
                        "name": column_metadata["name"],
                        "type": type_length,
                        "source": column_metadata["source"],
                        "description": column_metadata["description"],
                    }
                    column_metadata_list.append(field_dict)
            else:
                self.__formatNotSupportedYet()
            if self.debug:
                print(
                    fileErrorMsg(
                        'Success reading metadata from table "{}" '.format(table),
                        self.filepath,
                    )
                )
            return column_metadata_list
        except:
            print(
                fileErrorMsg(
                    'Error: unable to read metadata for table "{}"'.format(table),
                    self.filepath,
                )
            )
            raise

    def getData(self, table=None):
        if self.debug:
            print('Attempting to get data from table "{}"'.format(table))

        self.__isConnectionOpen(error_if_closed=True)

        # if no table specified, check to see if there is only table and use that
        if table is None:
            table = self.getSingularTable()

        self.__validateTableName(table)

        # now that the table name has been retrieved, get the data as pandas df
        try:
            if self.fileformat.filetype == "sqlite":
                query_result = pd.read_sql_query(
                    "select * from {}".format(table), self.connection
                )
            elif self.fileformat.filetype == "yxdb":
                # (another temporary solution)
                # get metadata (column names)
                # colnames = list(self.getMetadata()["name"])
                colnames = [col["name"] for col in self.getMetadata()]
                # reset pointer back to first line
                self.openConnection()
                self.connection.go_record(0)

                # get the actual data
                try:
                    # try to load all records at once
                    num_records = self.connection.get_num_records()
                    query_result = pd.DataFrame(
                        self.connection.read_records(num_records), columns=colnames
                    )
                except:
                    # if unable to do that, load one record at a time
                    data = []
                    while True:
                        data.append(self.connection.read_record())
                        if data[-1] == []:
                            break
                    query_result = pd.DataFrame(data, columns=colnames)

                # # if unable to do that, load one record at a time
                # data = []
                # while True:
                #     row = self.connection.read_record()
                #     if row == []:
                #         break
                #     else:
                #         data.append(row)
                #
                # query_result = pd.DataFrame(data, columns=colnames)

            else:
                self.__formatNotSupportedYet()

            if self.debug:
                print(
                    fileErrorMsg(
                        'Success reading input table "{}" '.format(table), self.filepath
                    )
                )
            return query_result
        except:
            print(
                fileErrorMsg(
                    'Error: unable to read input table "{}"'.format(table),
                    self.filepath,
                )
            )
            raise

    def writeData(self, pandas_df, table, metadata=None):
        if self.debug:
            print(
                '[CachedData.writeData] Attempting to write data to table "{}"'.format(
                    table
                )
            )
            print("[Datafile.writeData] metadata: {}".format(metadata))
        try:
            if self.fileformat.filetype == "sqlite":
                self.__createConnection()

                # prepare dtype arg for pandas
                dtypes = {}
                if isinstance(metadata, dict):
                    for col in metadata:
                        col_metadata = metadata[col]
                        if "name" in col_metadata:
                            name = col_metadata["name"]
                        else:
                            name = col
                        if "type_length" in col_metadata:
                            type_length = col_metadata["type_length"]
                            if type_length is not None:
                                dtypes[name] = type_length
                if len(dtypes.keys()) == 0:
                    dtypes = None

                if self.debug:
                    print("[Datafile.writeData] dtypes: {}".format(dtypes))

                # write to database
                pandas_df.to_sql(
                    table,
                    self.connection,
                    if_exists="replace",
                    index=False,
                    dtype=dtypes,
                )
            elif self.fileformat.filetype == "yxdb":
                # prepare metadata dict for AlteryxYXDB().create_from_dict (list)
                metadata_list = []
                column_conversions = {}
                pythontool_source = "PythonTool:"
                for index, col in enumerate(metadata.keys()):
                    metadata_col = metadata[col]
                    if self.debug:
                        print(
                            "\n[Datafile.writeData] input column: {}".format(
                                metadata_col
                            )
                        )
                    field_name = col
                    alteryx_type = metadata_col["type"]
                    field_type = pyxdbLookupFieldTypeEnum(alteryx_type)
                    field_length = metadata_col["length"]
                    field_size = field_length[0]
                    if len(field_length) > 1:
                        field_scale = int(field_length[1])
                    else:
                        field_scale = -1
                    # prepare source metadata
                    if "source" in metadata_col:
                        source = metadata_col["source"]
                    else:
                        source = ""
                    if len(source) > 0:
                        if source[-1:] == ":":
                            source = "{}{}".format(source, pythontool_source)
                    else:
                        source = pythontool_source
                    if "description" in metadata_col:
                        description = metadata_col["description"]
                    else:
                        description = ""

                    yxdb_metadata = {
                        "name": field_name,
                        "type": field_type,
                        "size": field_size,
                        "scale": field_scale,
                        "source": source,
                        "description": description,
                    }

                    if alteryx_type == "Boolean":
                        column_conversions[index] = "bool"
                    elif alteryx_type in ("Byte", "Int16", "Int32", "Int64"):
                        column_conversions[index] = "int"
                    elif alteryx_type in ("Float", "Fixed Decimal", "Double"):
                        column_conversions[index] = "float"

                    if self.debug:
                        print(
                            "[Datafile.writeData] yxdb column: {}".format(yxdb_metadata)
                        )

                    metadata_list.append(yxdb_metadata)
                if self.debug:
                    print("\nmetadata_list: {}".format(metadata_list))
                    print("\ncolumn_conversions: {}".format(column_conversions))

                try:
                    self.__createConnection(metadata_list)
                    row_count = pandas_df.shape[0]
                    if self.debug:
                        print("[Datafile.writeData] row count: {}".format(row_count))
                    for i in range(row_count):
                        if self.debug:
                            print("[Datafile.writeData] i: {}".format(i))

                        # get row (as list) from pandas dataframe
                        row = list(pandas_df.iloc[i])
                        # convert numeric types to base python (instead of numpy types, which are used by pandas)
                        for col_i in column_conversions:
                            if pd.isnull(row[col_i]):
                                row[col_i] = None
                            else:
                                row[col_i] = getattr(
                                    builtins, column_conversions[col_i]
                                )(row[col_i])

                        self.connection.append_record(row)
                except:
                    raise
                finally:
                    try:
                        self.connection.close()
                    except:
                        pass
            else:
                self.__formatNotSupportedYet()
            if self.debug:
                print(
                    fileErrorMsg(
                        'Success writing output table "{}"'.format(table), self.filepath
                    )
                )
            return pandas_df
        except:
            print(
                fileErrorMsg(
                    'Error: unable to write output table "{}"'.format(table),
                    self.filepath,
                )
            )
            raise
