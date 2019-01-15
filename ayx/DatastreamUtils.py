# (C) Copyright 2018 Alteryx, Inc.
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


import sys, json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from os.path import abspath
from re import findall
from functools import reduce
from ayx.helpers import convertObjToStr, fileExists, isDictMappingStrToStr
from ayx.Datafiles import FileFormat
from ayx.Compiled import pyxdb
from ayx import Settings
from IPython.display import set_matplotlib_close
from IPython import get_ipython


# temp replacement for sys.stdout (write method does nothing)
class NullWriter(object):
    def write(self, arg):
        pass


# setup matplotlib for export to datastream
set_matplotlib_close(False)
try:
    get_ipython().run_line_magic("matplotlib", "inline")
except AttributeError:
    pass  # running outside of jupyter (eg, cmd line tests)


def savePlotToFile(matplotlib_figure, filepath):
    abs_filepath = abspath(filepath)

    # turn off printing (savefig is annoying)
    # nullwrite = NullWriter()
    # oldstdout = sys.stdout
    # sys.stdout = nullwrite
    # save plot as image
    success = False
    try:
        # matplotlib.use('Agg')
        # figure = matplotlib_pyplot.gcf()
        # matplotlib_pyplot.show()
        # matplotlib_pyplot.draw()
        # matplotlib_pyplot.savefig(abs_filepath, format='png')
        # matplotlib_pyplot.plot()

        # figure = matplotlib_pyplot.gcf()
        # figure = matplotlib.pyplot.gcf()
        matplotlib_figure.savefig(
            abs_filepath, format="png", bbox_inches="tight", pad_inches=0
        )

        # display image
        img = mpimg.imread(abs_filepath)
        imgplot = plt.imshow(img)
        plt.axis("off")
        plt.title(None)
        set_matplotlib_close(False)
        plt.show()
        # plt.draw()

        # matplotlib_pyplot.show();
        # matplotlib_pyplot.close()
        success = True
    except Exception as err:
        error = err
    # turn back on printing
    finally:
        # sys.stdout = oldstdout
        pass

    # # turn off printing (savefig is annoying)
    # nullwrite = NullWriter()
    # oldstdout = sys.stdout
    # sys.stdout = nullwrite
    # # save plot as image
    # success = False
    # try:
    #     matplotlib.use('Agg')
    #     matplotlib_pyplot.savefig(abs_filepath)
    #     # matplotlib_pyplot.close()
    #     success = True
    # except Exception as err:
    #     pass
    # # turn back on printing
    # finally:
    #     sys.stdout = oldstdout

    # if success, return filepath
    if success:
        return abs_filepath
    # otherwise, throw error
    # (this control flow is a little funky to ensure printing is turned back on
    #  prior to throwing error)
    else:
        raise error


class MetadataTools:
    def __init__(self, debug=None):
        # initialize the different columns in each context (yxdb, sqlite)
        self.columns = {
            "context": {
                "yxdb": self.__yxdbFieldTypeAttributes(),
                "sqlite": self.__sqliteFieldTypeAttributes(),
                "pandas": self.__pandasFieldTypeAttributes(),
                "python": self.__pythonFieldTypeAttributes(),
            }
        }
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError("debug value must be True or False")

    def __pythonFieldTypeAttributes(self):
        return {
            "bool": {
                "conversion_types": {"yxdb": ["Boolean"], "pandas": ["bool"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "int": {
                "conversion_types": {
                    "yxdb": ["Int64", "Int32", "Int16", "Byte", "Boolean"],
                    "pandas": ["int64"],
                },
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "float": {
                "conversion_types": {
                    "yxdb": ["Float", "Double", "Fixed Decimal"],
                    "pandas": ["float64"],
                },
                "expected_length_dim": 0,
                "default_length": (None,),
            },
        }

    def __pandasFieldTypeAttributes(self):
        return {
            "bool": {
                "conversion_types": {"yxdb": ["Boolean"], "python": ["bool"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "int64": {
                "conversion_types": {"yxdb": ["Int64"], "python": ["int"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "float64": {
                "conversion_types": {"yxdb": ["Double"], "python": ["float"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "object": {
                "conversion_types": {"yxdb": ["V_WString"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
            "datetime64[ns]": {
                "conversion_types": {"yxdb": ["DateTime"]},
                "expected_length_dim": 0,
                "default_length": (None,),
            },
        }

    def __yxdbFieldTypeAttributes(self):
        return {
            "SpatialObj": {
                "conversion_types": {"sqlite": ["AlteryxSpatialObjectBlob"]},
                "expected_length_dim": 0,
                "default_length": (536870911,),
            },
            "Blob": {
                "conversion_types": {"sqlite": ["Blob"]},
                "expected_length_dim": 0,
                "default_length": (2147483647,),
            },
            "String": {
                "conversion_types": {"sqlite": ["CHAR"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "V_String": {
                "conversion_types": {"sqlite": ["varchar"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "WString": {
                "conversion_types": {"sqlite": ["nchar"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "V_WString": {
                "conversion_types": {"sqlite": ["nvarchar", "TEXT"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "Boolean": {
                "conversion_types": {"sqlite": ["boolean"]},
                "expected_length_dim": 0,
                "default_length": (1,),
            },
            "Byte": {
                "conversion_types": {"sqlite": ["tinyint unsigned"]},
                "expected_length_dim": 0,
                "default_length": (1,),
            },
            "Int16": {
                "conversion_types": {"sqlite": ["smallint"]},
                "expected_length_dim": 0,
                "default_length": (2,),
            },
            "Int32": {
                "conversion_types": {"sqlite": ["int"]},
                "expected_length_dim": 0,
                "default_length": (4,),
            },
            "Int64": {
                "conversion_types": {"sqlite": ["bigint", "INTEGER"]},
                "expected_length_dim": 0,
                "default_length": (8,),
            },
            "Float": {
                "conversion_types": {"sqlite": ["float"]},
                "expected_length_dim": 0,
                "default_length": (4,),
            },
            "Double": {
                "conversion_types": {"sqlite": ["double", "REAL"]},
                "expected_length_dim": 0,
                "default_length": (8,),
            },
            "Fixed Decimal": {
                "conversion_types": {"sqlite": ["decimal"]},
                "expected_length_dim": 2,
                "default_length": (19, 6),
            },
            "Date": {
                "conversion_types": {"sqlite": ["date"]},
                "expected_length_dim": 0,
                "default_length": (8,),
            },
            "Time": {
                "conversion_types": {"sqlite": ["time"]},
                "expected_length_dim": 0,
                "default_length": (10,),
            },
            "DateTime": {
                "conversion_types": {"sqlite": ["datetime"]},
                "expected_length_dim": 0,
                "default_length": (19,),
            },
        }

    def __sqliteFieldTypeAttributes(self):
        return {
            "AlteryxSpatialObjectBlob": {
                "conversion_types": {"yxdb": ["SpatialObj"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "Blob": {
                "conversion_types": {"yxdb": ["Blob"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "CHAR": {
                "conversion_types": {"yxdb": ["String"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "varchar": {
                "conversion_types": {"yxdb": ["V_String"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "nchar": {
                "conversion_types": {"yxdb": ["WString"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "nvarchar": {
                "conversion_types": {"yxdb": ["V_WString"]},
                "expected_length_dim": 1,
                "default_length": (2147483647,),
            },
            "TEXT": {
                "conversion_types": {"yxdb": ["V_WString"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "boolean": {
                "conversion_types": {"yxdb": ["Boolean"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "tinyint unsigned": {
                "conversion_types": {"yxdb": ["Byte"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "smallint": {
                "conversion_types": {"yxdb": ["Int16"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "int": {
                "conversion_types": {"yxdb": ["Int32"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "bigint": {
                "conversion_types": {"yxdb": ["Int64"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "INTEGER": {
                "conversion_types": {"yxdb": ["Int64"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "float": {
                "conversion_types": {"yxdb": ["Float"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "double": {
                "conversion_types": {"yxdb": ["Double"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "REAL": {
                "conversion_types": {"yxdb": ["Double"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "decimal": {
                "conversion_types": {"yxdb": ["Fixed Decimal"]},
                "expected_length_dim": 2,
                "default_length": (19, 6),
            },
            "date": {
                "conversion_types": {"yxdb": ["Date"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "time": {
                "conversion_types": {"yxdb": ["Time"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
            "datetime": {
                "conversion_types": {"yxdb": ["DateTime"]},
                "expected_length_dim": 0,
                "default_length": None,
            },
        }

    def concatTypeLength(self, type_str, length=None, context=None):
        if self.debug:
            print(
                "MetadataTools.concatTypeLength(type_str={}, length={}, context={})".format(
                    type_str, length, context
                )
            )

        context_dict = self.__contextDict(context)

        meta_info = self.columns["context"][context][type_str]
        expected_length_dim = meta_info["expected_length_dim"]
        default_length = meta_info["default_length"]

        if length is None:
            length_tuple = default_length
        elif isinstance(length, int):
            length_tuple = (length,)
        elif isinstance(length, float):
            length_tuple = self.__getLengthOnly(str(length))
        elif isinstance(length, str):
            length_tuple = self.__getLengthOnly(length)
        elif isinstance(length, tuple):
            length_tuple = length
        else:
            raise TypeError("field length must be a string or tuple")

        # check if valid
        try:
            if expected_length_dim == 0:
                length_tuple = ()
            self.__isValidFieldTypeLength(
                type_str, length_tuple, context=context, error_if_invalid=True
            )
        except:
            print(
                " ".join(
                    ["length_tuple ({}) is invalid for type `{}`", "in context `{}`"]
                ).format(length_tuple, type_str, context)
            )
            raise

        length_str = self.convertLengthTupleToContext(length_tuple, context)

        return "{} {}".format(type_str, length_str).strip()

    def convertLengthTupleToContext(self, length_tuple, context=None):
        if self.debug:
            print(
                "MetadataTools.convertLengthTupleToContext(length_tuple, context=None)".format(
                    length_tuple, context
                )
            )

        if not isinstance(length_tuple, tuple):
            raise TypeError("length_tuple must be a tuple or None")

        context_dict = self.__contextDict(context)

        if length_tuple is None:
            length_tuple = ()
        elif isinstance(length_tuple, str):
            length_tuple = self.__getLengthOnly(length_tuple)
        else:
            try:
                self.__isValidFieldLengthTuple(length_tuple, error_if_invalid=True)
            except:
                raise

        length_dim = len(length_tuple)

        if context_dict["sqlite"]:
            if length_dim == 0:
                new_length = ""
            elif length_dim == 1:
                new_length = "({})".format(length_tuple[0])
            elif length_dim == 2:
                new_length = "({}, {})".format(length_tuple[0], length_tuple[1])
        if context_dict["yxdb"]:
            if length_dim == 0:
                new_length = None
            elif length_dim == 1:
                new_length = length_tuple[0]
            elif length_dim == 2:
                new_length = float("{}.{}".format(length_tuple[0], length_tuple[1]))
        return new_length

    def convertTypeString(
        self, field_type_and_length, from_context=None, to_context=None
    ):
        msg_tag = "[MetadataTools.convertTypeString] field_type_and_length: {}".format(
            field_type_and_length
        )
        if self.debug:
            print(
                "{}, from_context: {}, to_context: {}".format(
                    msg_tag, from_context, to_context
                )
            )
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError(
                "Invalid field type/length string: {}".format(
                    convertObjToStr(field_type_and_length)
                )
            )
        if not isinstance(from_context, str):
            raise TypeError(
                "Invalid from context (must be string): {}".format(
                    convertObjToStr(from_context)
                )
            )
        if not isinstance(to_context, str):
            raise TypeError(
                "Invalid to context (must be string): {}".format(
                    convertObjToStr(to_context)
                )
            )

        # if valid context provided, convert to dict -- ex/ {'yxdb': True, 'sqlite': False}
        context_dict = self.__contextDict(from_context)
        # # parse type and length
        field_type_and_length_d = self.parseFieldTypeAndLengthStr(
            field_type_and_length, context=from_context
        )
        field_type = field_type_and_length_d["type"]
        field_length = field_type_and_length_d["length"]
        # field_type = self.__getTypeOnly(field_type_and_length)
        # try:
        #     self.__isValidFieldTypeStr(field_type, context=from_context, error_if_invalid=False)
        # except:
        #     raise

        # lookup field type conversion
        if not (from_context in self.columns["context"]):
            raise ReferenceError("from_context is invalid: {}".format(from_context))
        # check if field type exists in context
        elif not (field_type in self.columns["context"][from_context]):
            raise LookupError(
                " ".join(['invalid field type ({}) for context "{}"']).format(
                    field_type, from_context
                )
            )
        # if converting to self, then just send the parsed values back
        elif from_context == to_context:
            return {"type": field_type, "length": field_length}
        # check that conversions are available
        elif not (
            to_context
            in self.columns["context"][from_context][field_type]["conversion_types"]
        ):
            raise LookupError(
                " ".join(
                    ["field ({}) does not have type conversion mapping for {}->{}"]
                ).format(field_type, from_context, to_context)
            )
        # if all is good, then return
        else:
            # get the converted field type
            converted_type = self.columns["context"][from_context][field_type][
                "conversion_types"
            ][to_context][0]
            # get the expected length dimension and default value for converted type
            converted_type_metadata = self.columns["context"][to_context][
                converted_type
            ]
            converted_type_length_dim = converted_type_metadata["expected_length_dim"]
            converted_type_default_length = converted_type_metadata["default_length"]
            # if expected length is 0, use default
            if (converted_type_length_dim == 0) or (len(field_length) == 0):
                field_length = converted_type_default_length

        if self.debug:
            print(
                "{} -> type: {}, length: {}".format(
                    msg_tag, converted_type, field_length
                )
            )
        return {"type": converted_type, "length": field_length}

    # parse out the length from string containing both type and length (eg, 'CHAR(7)')
    # returns tuple (len=2 only in case of fixed decimal)
    def __getLengthOnly(self, field_type_and_length, error_if_invalid=False):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError(
                "Invalid field type/length string: {}".format(
                    convertObjToStr(field_type_and_length)
                )
            )
        # parse field length tuple from string
        field_length_str_list = findall(r"\b\d+", field_type_and_length)
        field_length_tuple = tuple(
            map(lambda length: int(length), field_length_str_list)
        )
        # if invalid tuple, throw an error
        try:
            self.__isValidFieldLengthTuple(field_length_tuple, error_if_invalid=True)
        except:
            print(
                "Invalid field length parsed out of from: {}".format(
                    field_type_and_length
                )
            )
            raise
        # return the tuple representing the field length/precision
        return field_length_tuple

    # parse out the type from string containing both type and length (eg, 'CHAR(7)')
    # returns a string value
    def __getTypeOnly(self, field_type_and_length, error_if_invalid=False):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError(
                "Invalid field type/length string: {}".format(
                    convertObjToStr(field_type_and_length)
                )
            )
        # parse column type out of string containing type and length
        parsed_type_list = findall(
            r"\b[A-Za-z]\w+(?:\s[A-Za-z]\w+)?\b", field_type_and_length
        )
        # there should only ever be exactly 1 result found, otherwise throw error
        if len(parsed_type_list) == 1:
            # get the first (and only) item in the list
            return parsed_type_list[0]
        # if there are 0 (or 2+) strings parsed out that potentially represent
        # the field type value, then throw an error
        else:
            raise ValueError(
                "Invalid column type string: {}".format(field_type_and_length)
            )

    def parseFieldTypeAndLengthStr(
        self, field_type_and_length, context=None, error_if_invalid=False
    ):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError(
                "Invalid field type/length string: {}".format(
                    convertObjToStr(field_type_and_length)
                )
            )
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError(
                "\n".join(
                    [
                        "Invalid error_if_invalid value: {}",
                        "Must be a boolean value (True or False).",
                    ]
                )
            ).format(convertObjToStr(error_if_invalid))
        # convert context to dict
        context_dict = self.__contextDict(context)
        # parse out field type and length, and run any additional validation functions
        try:
            # parse out field type and length
            field_type_uncased = self.__getTypeOnly(
                field_type_and_length
            )  # returns string (errors if invalid)
            # convert to proper case
            field_type = self.__fixTypeCase(field_type_uncased, context=context)
            # get field length from concatenated type/length string
            field_length = self.__getLengthOnly(
                field_type_and_length
            )  # returns tuple (errors if invalid)
            # check if length/precision is appropriate for field type
            self.__isValidFieldTypeLength(field_type, field_length, context=context)
            #
            field_updated = {
                "type": field_type,
                "length": field_length,
                "context": context,
            }
            # field_updated = self.supplementWithDefaultLengths(field_type, field_length, context=context)
            return field_updated
        except:
            raise

    def getFieldTypeAttributes(self, field_type, context=None):
        # check that field_type is a string
        if not isinstance(field_type, str):
            raise TypeError(
                "field_type must be a string that represents a valid field type"
            )
        # convert context to dict
        context_dict = self.__contextDict(context)
        # check if one or many contexts
        context_count = reduce(
            lambda x, key: x + int(context_dict[key]), context_dict, 0
        )
        if context_count != 1:
            raise ValueError("this function is prepared to handle  exactly one context")

        # initialize var
        for context_name in context_dict:
            if context_dict[context_name]:
                return self.columns["context"][context_name][field_type]

        # if we got this far without returning a value, then something went wrong
        raise LookupError(
            "error looking up attributes for field_type {} in context {}".format(
                field_type, context_name
            )
        )

    def supplementWithDefaultLengths(self, field_type, field_length, context=None):
        output_dict = {"type": field_type, "length": field_length}
        if self.debug:
            print(
                "Supplementing field metadata with default lengths ({}) if needed:".format(
                    context
                )
            )
            print(output_dict)

        # convert context to dict
        context_dict = self.__contextDict(context)

        field_length_missing = (field_length is None) or (
            ((isinstance(field_length, tuple)) and (len(field_length) == 0))
            or ((isinstance(field_length, str)) and (len(field_length) == 0))
        )

        if field_length_missing:
            field_type_attributes = self.getFieldTypeAttributes(
                field_type, context=context
            )
            output_dict["length"] = field_type_attributes["default_length"]
            if self.debug:
                print("field metadata updated:")
                print(output_dict)

        elif isinstance(field_length, str):
            output_dict["length"] = self.__getLengthOnly(field_length)

        elif isinstance(field_length, int):
            output_dict["length"] = (field_length,)

        elif isinstance(field_length, float):
            output_dict["length"] = self.__getLengthOnly(str(field_length))

        elif not isinstance(field_length, tuple):
            raise TypeError(
                "field_length must be a tuple of length 0 (static field length), 1 (width), or 2 (precision)"
            )

        if self.debug:
            print("output field metadata:")
            print(output_dict)

        return output_dict

    def __fixTypeCase(self, field_type_uncased, context=None):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_uncased, str):
            raise TypeError(
                "Invalid field type string: {}".format(
                    convertObjToStr(field_type_uncased)
                )
            )
        # check that context is provided and is valid
        if context is None:
            raise TypeError("context must be provided for this function")
        elif not isinstance(context, str):
            raise TypeError("only one context can be provided - must be string")
        # implicitly checks validity
        context_dict = self.__contextDict(context)

        # get list of valid field types
        valid_field_types = list(self.columns["context"][context])
        # look for a match
        matching_type = None
        for valid_field_type in valid_field_types:
            value = field_type_uncased.lower().replace(" ", "")
            valid = valid_field_type.lower().replace(" ", "")[: len(value)]
            if value == valid:
                matching_type = valid_field_type
                break

        if matching_type is None:
            raise LookupError(
                "unable to find matching field type value for {} in {} context".format(
                    field_type_uncased, context
                )
            )

        return matching_type

    def __isValidFieldTypeLength(
        self, field_type_str, field_length_tuple, context=None, error_if_invalid=False
    ):
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError(
                "\n".join(
                    [
                        "Invalid error_if_invalid value: {}",
                        "Must be a boolean value (True or False).",
                    ]
                )
            ).format(convertObjToStr(error_if_invalid))
        # check if field_length_tuple and field_type_str are valid
        # and if not, throw error
        try:
            self.__isValidFieldLengthTuple(field_length_tuple, error_if_invalid=True)
            self.__isValidFieldTypeStr(
                field_type_str, context=context, error_if_invalid=True
            )
        except:
            raise
        # convert context to dict
        context_dict = self.__contextDict(context)
        # intialize vars
        valid = False
        reasons = []
        # check whether field length is appropriate for field type (in the relevant context(s))
        for context in self.columns["context"]:  # eg, yxdb or sqlite
            if (context_dict[context]) and (
                field_type_str in self.columns["context"][context]
            ):
                field_type_metadata = self.columns["context"][context][field_type_str]
                expected_length = field_type_metadata["expected_length_dim"]
                default_length = field_type_metadata["default_length"]
                if len(field_length_tuple) == expected_length:
                    valid = True
                    return valid
                elif expected_length == 0:
                    valid = True
                    return valid
                else:
                    context_reason = {
                        "context": context,
                        "expected_length": expected_length,
                    }
                    reasons.append(context_reason)
        if not valid:
            raise ValueError(
                " ".join(
                    [
                        "Field length tuple ({}) for specified field",
                        "type ({}).",
                        "Expected tuple length: {}",
                    ]
                ).format(field_length_tuple, field_type_str, reasons)
            )
        return valid

    def __isValidFieldLengthTuple(self, field_length_tuple, error_if_invalid=False):
        try:
            # check if input is tuple
            if not isinstance(field_length_tuple, tuple):
                raise TypeError(
                    "".join(
                        ["Invalid field_length_tuple value: {}", "Must be a tuple."]
                    ).format(convertObjToStr(field_length_tuple))
                )
            # check if error_if_invalid is boolean
            if not isinstance(error_if_invalid, bool):
                raise TypeError(
                    "\n".join(
                        [
                            "Invalid error_if_invalid value: {}",
                            "Must be a boolean value (True or False).",
                        ]
                    )
                ).format(convertObjToStr(error_if_invalid))
            # check that field_length_tuple doesn't contain more than two items
            if len(field_length_tuple) > 2:
                raise ValueError(
                    "".join(
                        [
                            "Invalid field_length_tuple value: {}",
                            "Must have length of 0, 1, or 2.",
                        ]
                    ).format(convertObjToStr(field_length_tuple))
                )
            # check if each element is an int
            for element in field_length_tuple:
                if not isinstance(element, int):
                    raise TypeError(
                        "".join(
                            [
                                "Invalid element in field_length_tuple: {}",
                                "Must be an integer.",
                            ]
                        ).format(convertObjToStr(element))
                    )
            # if we made it this far without erroring, then input value is valid
            return True
        # if an error was caught, then an invalid value was provided
        except TypeError as err:
            if error_if_invalid:
                raise
            else:
                print(err)
            return False

    # check if field type string is valid, depending on context (yxdb or sqlite)
    def __isValidFieldTypeStr(
        self, field_type_str, context=None, error_if_invalid=False
    ):
        # check if input is tuple
        if not isinstance(field_type_str, str):
            raise TypeError(
                "\n".join(
                    ["Invalid field_type_str value: {}", "Must be a string."]
                ).format(convertObjToStr(field_type_str))
            )
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError(
                "\n".join(
                    [
                        "Invalid error_if_invalid value: {}",
                        "Must be a boolean value (True or False).",
                    ]
                )
            ).format(convertObjToStr(error_if_invalid))
        # convert context to dict
        context_dict = self.__contextDict(context)

        # default assumption is that type is not valid
        type_is_valid = False
        # check if type exists in each context
        for context in context_dict:
            # for each possible context...
            if (
                # ...if a given context is currently relevant...
                context_dict[context]
                and
                # ...and the field type string is valid in that context
                (field_type_str in self.columns["context"][context])
            ):
                # ...then yes, this field type string is valid!
                type_is_valid = True
        # throw error if match not found (and specified by the 'throw error' argument)
        if error_if_invalid and not type_is_valid:
            raise ValueError(
                "\n".join(
                    [
                        "Invalid column type string <{}>: {}",
                        "Valid Alteryx types: {}",
                        "Valid Sqlite types: {}",
                    ]
                ).format(
                    type(field_type_str),
                    field_type_str,
                    list(self.columns["context"]["yxdb"]),
                    list(self.columns["context"]["sqlite"]),
                )
            )
        # return boolean value indicating whether type is valid
        return type_is_valid

    # borderline pointless function to put context in dict
    def __contextDict(self, context):
        # valid context values
        # valid_contexts = FileFormat().valid_formats
        valid_contexts = list(self.columns["context"])
        # intialize context_dict
        context_dict = {}
        # check conditions -- if None, then allow both
        if context is None:
            # context_dict = {'yxdb': True, 'sqlite': True}
            raise ValueError("Context value must be specified")
        elif not isinstance(context, str):
            raise TypeError("Context must be a string.")
        else:
            if context not in valid_contexts:
                raise ValueError(
                    "Context value must be one of: {}".format(valid_contexts)
                )
            for context_key in valid_contexts:
                context_dict[context_key] = context_key == context
        return context_dict


class Config:
    def __init__(self, filepath=None, debug=None):

        # default value for config filepath
        if filepath is None:
            filepath = "jupyterPipes.json"

        elif not isinstance(filepath, str):
            raise TypeError(
                "\n".join(
                    [
                        "config filepath must be a string, not {}:".format(
                            type(filepath)
                        ),
                        convertObjToStr(filepath),
                    ]
                )
            )

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError("debug parameter must True or False")

        # set attributes
        self.valid_temp_file_formats = FileFormat().valid_formats
        self.filepath = filepath
        self.absolute_path = abspath(self.filepath)
        jupyter_pipes_dict = self.__getInputFileMap()
        self.input_file_map = jupyter_pipes_dict["input_map"]
        self.constant_map = jupyter_pipes_dict["constant_map"]
        self.temp_file_format = jupyter_pipes_dict["temp_file_format"]

        if len(self.constant_map) == 0:
            raise LookupError(
                "You must run the workflow first to make cached data and workflow constants available to the Python tool"
            )

    def setTempFormatAs(self, temp_format):
        if self.debug:
            print("Attempting to write config file ({})".format(self.absolute_path))

        try:
            self.__verifyTempFileFormat(temp_format)
            config = self.__getConfigJSON()
            config["temp_file_format"] = temp_format
            with open(self.absolute_path, "w") as fp:
                json.dump(config, fp)
            if self.debug:
                print(
                    "Successfully wrote config file ({}) with temp_file_format set to {}".format(
                        self.absolute_path, temp_format
                    )
                )
        except:
            print(
                "Unable to write config file with new temp file format -- {}".format(
                    self.absolute_path
                )
            )
            raise

    def __getConfigJSON(self):
        if self.debug:
            print("Attempting to read in config file ({})".format(self.absolute_path))

        try:
            # check that config exists
            if fileExists(
                self.absolute_path,
                True,
                "Cached data unavailable -- run the workflow to make the input data available in Jupyter notebook",
            ):
                if self.debug:
                    print("Config file found -- {}".format(self.absolute_path))
                # read in config file
                with open(self.absolute_path) as f:
                    config = json.load(f)
                # check if config is a dict
                if not isinstance(config, dict):
                    raise TypeError("Input config must be a python dict")
                return config
        except:
            print("Config file error -- {}".format(self.absolute_path))
            raise

    # mapping of connection name to filepath {input_connection_name: filepath}
    def __getInputFileMap(self):

        if self.debug:
            print(
                "Attempting to get input connection filepath map {}".format(
                    self.absolute_path
                )
            )

        try:
            # load the config json as a dict
            config = self.__getConfigJSON()
            # config is the raw json, input_map is specifically the input mapping
            # (eg, if the mapping is nested below the parent node of a larger config)
            input_map = {}
            if "input_connections" in config:
                input_map_raw = config["input_connections"]
                # check if file is in expected format
                try:
                    isDictMappingStrToStr(input_map_raw)
                except:
                    pass
                for connection_name in input_map_raw:
                    filename = input_map_raw[connection_name]
                    connection_metadata = {}
                    connection_metadata["filename"] = filename
                    connection_metadata["filetype"] = FileFormat(filename).filetype
                    input_map[connection_name] = connection_metadata

            if "Constants" in config:
                constant_map = config["Constants"]
                self.__verifyConstantMap(constant_map)
            else:
                constant_map = {}

            if "temp_file_format" in config:
                temp_file_format = config["temp_file_format"]
            else:
                temp_file_format = Settings.default_temp_file_format
                self.setTempFormatAs(temp_file_format)
            self.__verifyTempFileFormat(temp_file_format)

            return {
                "input_map": input_map,
                "constant_map": constant_map,
                "temp_file_format": temp_file_format,
            }
        except:
            print("Config file error -- {}".format(self.absolute_path))
            raise

    def __verifyTempFileFormat(self, temp_file_format):
        if not isinstance(temp_file_format, str):
            raise TypeError("Temp file format value must be a string")
        if temp_file_format not in self.valid_temp_file_formats:
            raise ValueError(
                " ".join(
                    [
                        "temp file format ({}) is invalid".format(
                            self.temp_file_format
                        ),
                        "-- valid formats: {}".format(self.valid_temp_file_formats),
                    ]
                )
            )
        return True

    # verify that the config json is in the expected structure
    def __verifyConstantMap(self, constant_map):

        # Workflow Constants
        if not isinstance(constant_map, dict):
            raise TypeError("Constants value must be a python dict")
        for key, value in constant_map.items():
            if not isinstance(key, str):
                raise TypeError(
                    "\n".join(
                        [
                            "Constants keys must be strings",
                            "Invalid type: {}",
                            "Invalid value: {}",
                        ]
                    ).format(type(key), key)
                )
            elif not isinstance(value, (str, float, int)):
                raise TypeError(
                    "\n".join(
                        [
                            "Constants values must be str, float, or int",
                            "Invalid type: {}",
                            "Invalid value: {}",
                        ]
                    ).format(type(value), value)
                )
        return True
