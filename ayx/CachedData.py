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


from uuid import uuid1
import pandas as pd
from matplotlib.figure import Figure
from ayx.DatastreamUtils import MetadataTools, Config, savePlotToFile
from ayx.Datafiles import Datafile, FileFormat
from ayx.Settings import default_temp_file_format as temp_format


class CachedData:
    def __init__(self, config_filepath=None, debug=None):
        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError("debug parameter must True or False")

        # obtain input data mappings and workflow constants
        self.config = Config(filepath=config_filepath, debug=debug)
        # output format
        if (
            isinstance(self.config.temp_file_format, str)
            and len(self.config.temp_file_format) > 0
        ):
            temp_output_format = self.config.temp_file_format
        else:
            temp_output_format = temp_format

        # this is where we specify what kind of datafile format we're using
        # for OUTPUT ... input data formats are determined by the filenames
        # specified in the JupyterPipes.json file
        self.output_datafile_format = {
            "filetype": temp_output_format,
            "extension": temp_output_format,
        }

    def __getIncomingConnectionMetadata(self, incoming_connection_name):
        if self.debug:
            print(
                'Attempting to get the cached data filepath for incoming connection "{}"'.format(
                    incoming_connection_name
                )
            )

        input_file_map = self.config.input_file_map

        # error if connection name is not a string
        if not isinstance(incoming_connection_name, str):
            raise TypeError(
                "".join(['Input connection name must be a string value. (eg, "#1")'])
            )
        # error if connection name is not a named key in the config json (dict)
        elif incoming_connection_name not in input_file_map:
            raise ReferenceError(
                "".join(
                    [
                        'The input connection "{}" has not been cached'.format(
                            incoming_connection_name
                        ),
                        " -- re-run workflow to refresh the cached data.",
                    ]
                )
            )
        else:
            return input_file_map[incoming_connection_name]

    def getWorkflowConstantNames(self):
        if self.debug:
            print("Attempting to get list of workflow constants")

        return list(self.config.constant_map)

    def getWorkflowConstants(self):
        if self.debug:
            print("Attempting to get list of workflow constants")

        return dict(self.config.constant_map)

    def getWorkflowConstant(self, constant_name):
        if self.debug:
            print(
                'Attempting to get the cached workflow constant "{}"'.format(
                    constant_name
                )
            )

        # error if constant name is not a string
        if not isinstance(constant_name, str):
            raise TypeError(
                "".join(
                    ["Constant name must be a string value, but instead is {}: {}"]
                ).format(type(constant_name), constant_name)
            )

        # if there are no constants
        if len(self.config.constant_map) == 0:
            raise RuntimeError(
                "You must run the workflow first in order to make constants available in this tool"
            )

        # error if connection name is not a named key in the config json (dict)
        # -- but first see if we can offer a suggestion to help the user identify their mistake
        if constant_name not in self.config.constant_map:

            # create a dict mapping of {CONSTANT_NAME: constant_name}
            uppercase_to_actual_constant_names = {
                x.upper(): x for x in self.config.constant_map
            }
            # save uppercase constant name to a var so we
            uppercase_constant_name = constant_name.upper()
            # initalize suggested constant name (no suggestion to start)
            suggested = None

            # is it a case error? check to see any keys match with case insensitivity
            if suggested is None:
                if uppercase_constant_name in uppercase_to_actual_constant_names:
                    suggested = uppercase_to_actual_constant_names[
                        uppercase_constant_name
                    ]

            # did they forget to add the Engine/Question/User prefix?
            if suggested is None:
                for prefix in ["Engine", "Question", "User"]:
                    name_with_prefix = "{}.{}".format(prefix, constant_name).upper()
                    if name_with_prefix in uppercase_to_actual_constant_names:
                        suggested = uppercase_to_actual_constant_names[
                            uppercase_constant_name
                        ]
                        break

            # did they miss some other prefixy part of the constant name?
            if suggested is None:
                for existing_key in self.config.constant_map:
                    if (
                        existing_key.split(".")[-1].upper()
                        == constant_name.split(".")[-1].upper()
                    ):
                        suggested = existing_key
                        break

            # if we've got a suggestion, throw error and propose suggestion
            if suggested is not None:
                print(
                    " ".join(
                        [
                            'The constant "{}" does not exist'.format(constant_name),
                            '-- did you mean "{}" ?'.format(suggested),
                        ]
                    )
                )
            # otherwise, throw a general error that the constant isnt valid
            else:
                print(
                    " ".join(
                        [
                            'The constant "{}" does not exist'.format(constant_name),
                            "-- double check the name and run the workflow again to",
                            "refresh the constants available in this tool.",
                        ]
                    )
                )

            raise ReferenceError(
                "Unable to find workflow constant {}".format(constant_name)
            )
        else:
            val = self.config.constant_map[constant_name]
            return val

    def read(self, incoming_connection_name, batch_size=1):

        if self.debug:
            print(
                'Attempting to read in cached data for incoming connection "{}"'.format(
                    incoming_connection_name
                )
            )

        # get the filepath of the data
        input_data_metadata = self.__getIncomingConnectionMetadata(
            incoming_connection_name
        )
        input_data_filename = input_data_metadata["filename"]
        input_data_filetype = input_data_metadata["filetype"]
        # create datafile object
        # (by not specifying the fileformat paramter, it will assume the file
        # type from the file's extension)
        with Datafile(
            input_data_filename, fileformat=input_data_filetype, debug=self.debug
        ) as db:
            msg_action = 'reading input data "{}"'.format(incoming_connection_name)
            try:
                # get the data from the sql db (if only one table exists, no need to specify the table name)
                data = db.getData(batch_size=batch_size)
                # print success message
                print("".join(["SUCCESS: ", msg_action]))
                # return the data
                return data
            except:
                print("".join(["ERROR: ", msg_action]))
                raise

    def __checkOutgoingConnectionNumber__(self, outgoing_connection_number):
        if not isinstance(outgoing_connection_number, int):
            raise TypeError(
                "".join(
                    [
                        msg_prefix,
                        "The outgoing connection number must be an integer value.",
                    ]
                )
            )
        # error if connection number is not between 1 and 5
        elif outgoing_connection_number < 1 or outgoing_connection_number > 5:
            raise ValueError(
                "The outgoing connection number must be an integer between 1 and 5"
            )
        return outgoing_connection_number

    def writePlot(self, matplotlib_figure, outgoing_connection_number):
        if self.debug:
            print(
                'Alteryx.writePlot() -- attempting to write out plot to outgoing connection "{}"'.format(
                    outgoing_connection_number
                )
            )

        try:
            outgoing_connection_number = self.__checkOutgoingConnectionNumber__(
                outgoing_connection_number
            )
            if matplotlib_figure is None:
                raise TypeError("A matplotlib plot is required")
            elif not isinstance(matplotlib_figure, Figure):
                raise TypeError(
                    "Currently only matplotlib figures can be used to pass plots to outgoing connections in Alteryx"
                )
        except Exception as err:
            print(
                "ERROR: Alteryx.writePlot(matplotlib_figure, outgoing_connection_number):"
            )
            print(err)
            raise

        filename = "plot_{}".format(outgoing_connection_number)
        saved_plot_filepath = savePlotToFile(
            matplotlib_figure, "plot_{}.png".format(outgoing_connection_number)
        )

        # create html for img tag
        img_ref_html = '<img src="{}" />'.format(saved_plot_filepath)
        plot_df = pd.DataFrame({"Plot": [img_ref_html]})
        self.write(
            plot_df,
            outgoing_connection_number,
            {"Plot": {"type": "V_String", "length": 2147483647}},
        )

    def write(self, pandas_df, outgoing_connection_number, batch_size=1, columns=None):

        if self.debug:
            print(
                'Alteryx.write() -- attempting to write out cached data to outgoing connection "{}"'.format(
                    outgoing_connection_number
                )
            )

        try:
            outgoing_connection_number = self.__checkOutgoingConnectionNumber__(
                outgoing_connection_number
            )
            if pandas_df is None:
                raise TypeError(
                    "A pandas dataframe is required for passing data to outgoing connections in Alteryx"
                )
            elif not isinstance(pandas_df, pd.core.frame.DataFrame):
                raise TypeError(
                    "Currently only pandas dataframes can be used to pass data to outgoing connections in Alteryx"
                )
        except Exception as err:
            print("ERROR: Alteryx.write(pandas_df, outgoing_connection_number):")
            print(err)
            raise

        if columns is None:
            pass
        elif not isinstance(columns, dict):
            raise TypeError(
                "columns (metadata) is optional, but if provided, must be a dict or list"
            )

        # get list of columns in input data frame
        pandas_cols = list(pandas_df.columns)

        if self.debug:
            print("pandas_df.columns:")
            print(pandas_df.columns)
            # print(dir(pandas_df.columns.dtype))
            # print(pandas_df.columns.name)
            for index, colname in enumerate(pandas_df.columns):
                coltype = pandas_df.dtypes[index]
                print("  {}: {}".format(colname, coltype))
                # print("  {}: {}".format(col, pandas_df.columns[col]))

        metadata_tools = MetadataTools(debug=self.debug)
        expected_column_attributes = ["name", "type", "length"]

        cols_tmp = {}

        from_context = "pandas"
        to_context = "yxdb"
        for index, colname in enumerate(pandas_df.columns):
            coltype = str(pandas_df.dtypes[index])
            try:
                db_col_metadata = metadata_tools.convertTypeString(
                    coltype, from_context=from_context, to_context=to_context
                )
                yxdb_type = db_col_metadata["type"]
                yxdb_length = db_col_metadata["length"]
                cols_tmp[colname] = {
                    "name": colname,
                    "type": yxdb_type,
                    "length": yxdb_length,
                }
            except:
                print(
                    'couldn\'t find conversion for {} ("{}") from {} to {} -> skipping'.format(
                        colname, coltype, from_context, to_context
                    )
                )

            # include any metadata provided
            new_column_info = None
            if isinstance(columns, dict) and colname in columns:
                new_column_info = columns[colname]
            elif isinstance(columns, list) and index < len(columns):
                new_column_info = columns[index]
            if new_column_info is not None:
                for updated_attr in new_column_info:
                    cols_tmp[colname][updated_attr] = new_column_info[updated_attr]

            if "type" in cols_tmp:
                new_type = cols_tmp["type"]
            else:
                new_type = None
            if "length" in cols_tmp:
                new_length = cols_tmp["length"]
            else:
                new_length = None

            if self.debug:
                print(
                    "[CachedData.write] name: {}, type/length: {}, from_context: {}, to_context: {} -> type: {}, length: {}".format(
                        colname, coltype, from_context, to_context, new_type, new_length
                    )
                )

        renames = {}
        write_metadata = {}

        from_context = "yxdb"
        to_context = self.output_datafile_format["filetype"]

        for colname in cols_tmp:
            col_metadata = cols_tmp[colname]
            if self.debug:
                print(
                    "[CachedData.write] name: {}, from_context: {}, to_context: {}, metadata: {}".format(
                        colname, from_context, to_context, col_metadata
                    )
                )

            col_name = str(
                col_metadata["name"]
            )  # coerce unnamed (ordered) columns from int to str
            col_type = col_metadata["type"]
            col_length = col_metadata["length"]

            # using the *new* column name for metadata
            write_metadata[col_name] = {}

            # copy any non-name/type/length attributes to write_metadata dict (eg, source, description)
            for attr in col_metadata.keys():
                if attr not in ["name", "type", "length"]:
                    write_metadata[col_name][attr] = col_metadata[attr]

            # if name changed, add to renames dict
            if col_name != colname:
                renames[colname] = col_name

            conversion = metadata_tools.convertTypeString(
                "{} {}".format(col_type, col_length),
                from_context=from_context,
                to_context=to_context,
            )

            # supplement with default column type lengths
            type_lengths = metadata_tools.supplementWithDefaultLengths(
                conversion["type"], conversion["length"], context=to_context
            )
            new_type = type_lengths["type"]
            new_length = type_lengths["length"]

            if self.debug:
                print("\n-----\n{}\n------\n".format(type_lengths))

            # concatenate type and length
            col_type_length = None
            if new_length is not None and len(str(new_length)) > 0:
                if new_type is None or len(str(new_type)) == 0:
                    raise ValueError("cannot set a column length without type")
                col_type_length = "{} {}".format(new_type, new_length)
            elif new_type is not None and len(str(new_type)) > 0:
                col_type_length = new_type

            # convert type/length to output format
            if col_type_length is not None:
                try:
                    db_col_metadata = metadata_tools.convertTypeString(
                        col_type_length, from_context=to_context, to_context=to_context
                    )
                    db_col_type_only = db_col_metadata["type"]
                    db_col_length_only = db_col_metadata["length"]

                    if self.debug:
                        print("\n-----\n{}\n------\n".format(db_col_metadata))

                    # concatenate type and length (use default if necessary)
                    db_col_type = metadata_tools.concatTypeLength(
                        db_col_type_only, db_col_length_only, context=to_context
                    )

                    # set in dtypes dict
                    write_metadata[col_name]["type"] = db_col_type_only
                    write_metadata[col_name]["length"] = db_col_length_only
                    write_metadata[col_name]["type_length"] = db_col_type

                    if self.debug:
                        print(
                            "[CachedData.write] name: {}, from_context: {}, to_context: {}, {} -> {}}".format(
                                colname,
                                from_context,
                                to_context,
                                col_type_length,
                                db_col_type,
                            )
                        )
                except:
                    if self.debug:
                        print(
                            '[CachedData.write] unable to convert {} ("{}") from {} to {} -> skipping'.format(
                                colname, col_type_length, from_context, to_context
                            )
                        )

        if len(write_metadata.keys()) == 0:
            write_metadata = None

        if len(renames.keys()) == 0:
            renames = None
            pandas_df_out = pandas_df
        else:
            if self.debug:
                print("renaming columns before output:")
                print(renames)
            pandas_df_out = pandas_df.rename(columns=renames, inplace=False)

        # create custom sqlite object
        # (TODO: update to yxdb)
        with Datafile(
            "output_{}.{}".format(
                outgoing_connection_number, self.output_datafile_format["extension"]
            ),
            create_new=True,
            debug=self.debug,
        ) as db:
            msg_action = "writing outgoing connection data {}".format(
                outgoing_connection_number
            )
            try:
                # get the data from the sql db (if only one table exists, no need to specify the table name)
                data = db.writeData(
                    pandas_df_out,
                    "data",
                    metadata=write_metadata,
                    batch_size=batch_size,
                )
                # print success message
                print("".join(["SUCCESS: ", msg_action]))
                # return the data
                return data
            except:
                print("".join(["ERROR: ", msg_action]))
                raise

    def getIncomingConnectionNames(self):
        if self.debug:
            print("Attempting to get all incoming connection names")

        return list(self.config.input_file_map.keys())

    def readMetadata(self, incoming_connection_name):
        if self.debug:
            print(
                'Attempting to get (cached) metadata for for incoming connection "{}"'.format(
                    incoming_connection_name
                )
            )
        # create a flag indicating whether input is a pandas dataframe
        pandas_df_input_flag = isinstance(
            incoming_connection_name, pd.core.frame.DataFrame
        )

        # if the input is a dataframe, then write the first row to a temporary
        # sqlite file, and get the metadata from it
        if pandas_df_input_flag:
            input_df_head = incoming_connection_name.head(1)
            temp_table_name = str(uuid1())
            filetype = self.output_datafile_format["filetype"]
            temp_file_path = ".".join(
                [temp_table_name, self.output_datafile_format["extension"]]
            )
            with Datafile(
                temp_file_path,
                create_new=True,
                temporary=True,
                fileformat=filetype,
                debug=self.debug,
            ) as db:
                db.writeData(input_df_head, "data")
                raw_metadata = db.getMetadata()
        # otherwise, if not a dataframe, assume input argument value is a
        # connection name string (function called will validate string type)
        else:
            pandas_df_input_flag = False
            # get the filepath of the data
            input_data_metadata = self.__getIncomingConnectionMetadata(
                incoming_connection_name
            )
            input_data_filename = input_data_metadata["filename"]
            filetype = input_data_metadata["filetype"]
            # get the data from the sqlite file
            with Datafile(
                input_data_filename,
                create_new=False,
                fileformat=filetype,
                debug=self.debug,
            ) as db:
                raw_metadata = db.getMetadata()

        # initiate the a MetadataTools object
        metadata_tools = MetadataTools(debug=self.debug)
        metadata_dict = {}
        # for index, field in raw_metadata.iterrows():
        for index, field in enumerate(raw_metadata):
            if pandas_df_input_flag:
                if field["name"] == str(input_df_head.columns[index]):
                    field_name = input_df_head.columns[index]
                else:
                    raise ReferenceError(
                        " ".join(
                            [
                                "error: pandas dataframe columns appear",
                                "to be in a different order than the correspond",
                                "datafile table for some reason...",
                                "> pandas dataframe columns: {}".format(
                                    input_df_head.columns
                                ),
                                "> datafile dataframe columns: {}".format(
                                    list(raw_metadata["name"])
                                ),
                            ]
                        )
                    )
            else:
                field_name = field["name"]
            field_type_str = field["type"]

            # parse out field type (str) and length (tuple) from string
            field_type_and_length_d = metadata_tools.parseFieldTypeAndLengthStr(
                field_type_str, context=filetype
            )
            field_type = field_type_and_length_d["type"]
            field_length = field_type_and_length_d["length"]
            # set metadata
            conversion = metadata_tools.convertTypeString(
                "{} {}".format(field_type, field_length),
                from_context=filetype,
                to_context="yxdb",
            )
            metadata_dict[field_name] = {
                "type": conversion["type"],
                "length": conversion["length"],
            }
            updated_field_metadata = metadata_tools.supplementWithDefaultLengths(
                metadata_dict[field_name]["type"],
                metadata_dict[field_name]["length"],
                context="yxdb",
            )
            updated_field_metadata[
                "length"
            ] = metadata_tools.convertLengthTupleToContext(
                updated_field_metadata["length"], context="yxdb"
            )
            metadata_dict[field_name] = updated_field_metadata

            # now deal with source...
            if "source" not in field:
                metadata_dict[field_name]["source"] = None
            else:
                metadata_dict[field_name]["source"] = field["source"]
            # ...and description
            if "description" not in field:
                metadata_dict[field_name]["description"] = None
            else:
                metadata_dict[field_name]["description"] = field["description"]

        if self.debug:
            print(
                "CachedData.readMetadata({}): {}".format(
                    incoming_connection_name, metadata_dict
                )
            )

        return metadata_dict
