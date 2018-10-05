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
import os
from pathlib import Path
import json
# import sqlalchemy
import sqlite3
import pandas as pd
from re import findall
from functools import reduce
from ayx.helpers import fileErrorMsg, fileExists, tableNameIsValid, convertObjToStr, isDictMappingStrToStr, convertToType



class MetadataTools:
    def __init__(self, debug=None):
        # initialize the different columns in each context (ayx, sqlite)
        self.columns = \
            {
                'context': {
                    'ayx': self.__ayxFieldTypeAttributes(),
                    'sqlite': self.__sqliteFieldTypeAttributes()
                    }
                }
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug value must be True or False')


    def __ayxFieldTypeAttributes(self):
        return {
            'SpatialObj': {
                'conversion_types': {'sqlite': ['AlteryxSpatialObjectBlob']},
                'expected_length_dim': 0,
                'default_length': (536870911,)
                },
            'Blob': {
                'conversion_types': {'sqlite': ['Blob']},
                'expected_length_dim': 0,
                'default_length': (2147483647,)
        },
            'String': {
                'conversion_types': {'sqlite': ['CHAR']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
        },
            'V_String': {
                'conversion_types': {'sqlite': ['varchar']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
        },
            'WString': {
                'conversion_types': {'sqlite': ['nchar']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
        },
            'V_WString': {
                'conversion_types': {'sqlite': ['nvarchar', 'TEXT']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
        },
            'Boolean': {
                'conversion_types': {'sqlite': ['boolean']},
                'expected_length_dim': 0,
                'default_length': (1,)
        },
            'Byte': {
                'conversion_types': {'sqlite': ['tinyint unsigned']},
                'expected_length_dim': 0,
                'default_length': (1,)
        },
            'Int16': {
                'conversion_types': {'sqlite': ['smallint']},
                'expected_length_dim': 0,
                'default_length': (2,)
        },
            'Int32': {
                'conversion_types': {'sqlite': ['int']},
                'expected_length_dim': 0,
                'default_length': (4,)
        },
            'Int64': {
                'conversion_types': {'sqlite': ['bigint', 'INTEGER']},
                'expected_length_dim': 0,
                'default_length': (8,)
        },
            'Float': {
                'conversion_types': {'sqlite': ['float']},
                'expected_length_dim': 0,
                'default_length': (4,)
        },
            'Double': {
                'conversion_types': {'sqlite': ['double', 'REAL']},
                'expected_length_dim': 0,
                'default_length': (8,)
        },
            'Fixed Decimal': {
                'conversion_types': {'sqlite': ['decimal']},
                'expected_length_dim': 2,
                'default_length': (19,6)
        },
            'Date': {
                'conversion_types': {'sqlite': ['date']},
                'expected_length_dim': 0,
                'default_length': (8,)
        },
            'Time': {
                'conversion_types': {'sqlite': ['time']},
                'expected_length_dim': 0,
                'default_length': (10,)
        },
            'DateTime': {
                'conversion_types': {'sqlite': ['datetime']},
                'expected_length_dim': 0,
                'default_length': (10,)
                }
            }

    def __sqliteFieldTypeAttributes(self):
        return {
            'AlteryxSpatialObjectBlob': {
                'conversion_types': {'ayx': ['SpatialObj']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'Blob': {
                'conversion_types': {'ayx': ['Blob']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'CHAR': {
                'conversion_types': {'ayx': ['String']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
                },
            'varchar': {
                'conversion_types': {'ayx': ['V_String']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
                },
            'nchar': {
                'conversion_types': {'ayx': ['WString']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
                },
            'nvarchar': {
                'conversion_types': {'ayx': ['V_WString']},
                'expected_length_dim': 1,
                'default_length': (2147483647,)
                },
            'TEXT': {
                'conversion_types': {'ayx': ['V_WString']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'boolean': {
                'conversion_types': {'ayx': ['Boolean']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'tinyint unsigned': {
                'conversion_types': {'ayx': ['Byte']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'smallint': {
                'conversion_types': {'ayx': ['Int16']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'int': {
                'conversion_types': {'ayx': ['Int32']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'bigint': {
                'conversion_types': {'ayx': ['Int64']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'INTEGER': {
                'conversion_types': {'ayx': ['Int64']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'float': {
                'conversion_types': {'ayx': ['Float']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'double': {
                'conversion_types': {'ayx': ['Double']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'REAL': {
                'conversion_types': {'ayx': ['Double']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'decimal': {
                'conversion_types': {'ayx': ['Fixed Decimal']},
                'expected_length_dim': 2,
                'default_length': (19, 6)
                },
            'date': {
                'conversion_types': {'ayx': ['Date']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'time': {
                'conversion_types': {'ayx': ['Time']},
                'expected_length_dim': 0,
                'default_length': None
                },
            'datetime': {
                'conversion_types': {'ayx': ['DateTime']},
                'expected_length_dim': 0,
                'default_length': None
                }
            }

    def concatTypeLength(self, type_str, length=None, context=None):
        if self.debug:
            print('MetadataTools.concatTypeLength(type_str={}, length={}, context={})'\
                        .format(type_str, length, context))

        context_dict = self.__contextDict(context)

        meta_info = self.columns['context'][context][type_str]
        expected_length_dim = meta_info['expected_length_dim']
        default_length = meta_info['default_length']

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
            raise TypeError('field length must be a string or tuple')

        # check if valid
        try:
            if ((expected_length_dim == 0) ):
                length_tuple = ()
            self.__isValidFieldTypeLength(type_str, length_tuple, context=context, error_if_invalid=True)
        except:
            print(' '.join([
                'length_tuple ({}) is invalid for type `{}`',
                'in context `{}`'
                ]).format(length_tuple, type_str, context)
                )
            raise

        length_str = self.convertLengthTupleToContext(length_tuple, context)

        return '{} {}'.format(type_str, length_str).strip()



    def convertLengthTupleToContext(self, length_tuple, context=None):
        if self.debug:
            print('MetadataTools.convertLengthTupleToContext(length_tuple, context=None)'\
                        .format(length_tuple, context))

        if not isinstance(length_tuple, tuple):
            raise TypeError('length_tuple must be a tuple or None')

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


        # if length_dim == 0:
        #     return ''
        # elif length_dim == 1:
        #     if context_dict['sqlite']:
        #         return '({})'.format(length_tuple[0])
        #     if context_dict['ayx']:
        #         return '{}'.format(length_tuple[0])
        # elif length_dim == 2:
        #     if context_dict['sqlite']:
        #         return '({}, {})'.format(length_tuple[0], length_tuple[1])
        #     if context_dict['ayx']:
        #         return '{}.{}'.format(length_tuple[0], length_tuple[1])
        # # if we got this far, something went wrong...
        # raise ValueError(' '.join([
        #     'unable to convert {} to context `{}`'
        #     ]).format(length_tuple, context)
        #     )


        if context_dict['sqlite']:
            if length_dim == 0:
                new_length = ''
            elif length_dim == 1:
                new_length = '({})'.format(length_tuple[0])
            elif length_dim == 2:
                new_length = '({}, {})'.format(length_tuple[0], length_tuple[1])
        if context_dict['ayx']:
            if length_dim == 0:
                new_length = None
            elif length_dim == 1:
                new_length = length_tuple[0]
            elif length_dim == 2:
                new_length = float(
                        '{}.{}'.format(length_tuple[0], length_tuple[1])
                        )
        return new_length




    def convertTypeString(self, field_type_and_length, from_context=None, to_context=None):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError('Invalid field type/length string: {}'\
                    .format(convertObjToStr(field_type_and_length)))
        if not isinstance(from_context, str):
            raise TypeError('Invalid from context (must be string): {}'\
                    .format(convertObjToStr(from_context)))
        if not isinstance(to_context, str):
            raise TypeError('Invalid to context (must be string): {}'\
                    .format(convertObjToStr(to_context)))

        # if valid context provided, convert to dict -- ex/ {'ayx': True, 'sqlite': False}
        context_dict = self.__contextDict(from_context)
        # # parse type and length
        field_type_and_length_d = self.parseFieldTypeAndLengthStr(
            field_type_and_length,
            context = from_context
            )
        field_type = field_type_and_length_d['type']
        field_length = field_type_and_length_d['length']
        # field_type = self.__getTypeOnly(field_type_and_length)
        # try:
        #     self.__isValidFieldTypeStr(field_type, context=from_context, error_if_invalid=False)
        # except:
        #     raise

         # lookup field type conversion
        if not(from_context in self.columns['context']):
            raise ReferenceError('from_context is invalid: {}'.format(from_context))
        # check if field type exists in context
        elif not(
            field_type in self.columns['context'][from_context]
        ):
            raise LookupError(' '.join([
                'invalid field type ({}) for context "{}"'\
                ]).format(
                    field_type,
                    from_context
                    )
                )
        # check that conversions are available
        elif not(
            to_context in self.columns['context'][from_context][field_type]['conversion_types']
        ):
            raise LookupError(' '.join([
                'field ({}) does not have type conversion mapping for {}->{}'
                ]).format(
                    field_type,
                    from_context,
                    to_context
                    )
                )
        # if all is good, then return
        else:
            # get the converted field type
            converted_type =  self.columns['context'][from_context][field_type]['conversion_types'][to_context][0]
            # get the expected length dimension and default value for converted type
            converted_type_metadata = self.columns['context'][to_context][converted_type]
            converted_type_length_dim = converted_type_metadata['expected_length_dim']
            converted_type_default_length = converted_type_metadata['default_length']
            # if expected length is 0, use default
            if (converted_type_length_dim == 0) or (len(field_length) == 0):
                field_length = converted_type_default_length

            return {'type': converted_type, 'length': field_length}

    # parse out the length from string containing both type and length (eg, 'CHAR(7)')
    # returns tuple (len=2 only in case of fixed decimal)
    def __getLengthOnly(self, field_type_and_length, error_if_invalid=False):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError('Invalid field type/length string: {}'\
                    .format(convertObjToStr(field_type_and_length)))
        # parse field length tuple from string
        field_length_str_list = findall(r"\b\d+",field_type_and_length)
        field_length_tuple = tuple(
            map(lambda length: int(length), field_length_str_list)
            )
        # if invalid tuple, throw an error
        try:
            self.__isValidFieldLengthTuple(field_length_tuple, error_if_invalid=True)
        except:
            print("Invalid field length parsed out of from: {}".format(
                field_type_and_length
                ))
            raise
        # return the tuple representing the field length/precision
        return field_length_tuple

    # parse out the type from string containing both type and length (eg, 'CHAR(7)')
    # returns a string value
    def __getTypeOnly(self,
        field_type_and_length,
        error_if_invalid=False
    ):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError('Invalid field type/length string: {}'\
                    .format(convertObjToStr(field_type_and_length)))
        # parse column type out of string containing type and length
        parsed_type_list = findall(r"\b[A-Za-z]\w+(?:\s[A-Za-z]\w+)?\b", field_type_and_length)
        # there should only ever be exactly 1 result found, otherwise throw error
        if len(parsed_type_list) == 1:
            # get the first (and only) item in the list
            return parsed_type_list[0]
        # if there are 0 (or 2+) strings parsed out that potentially represent
        # the field type value, then throw an error
        else:
            raise ValueError('Invalid column type string: {}'.format(field_type_and_length))



    def parseFieldTypeAndLengthStr(self,
        field_type_and_length,
        context=None,
        error_if_invalid=False
    ):
        # input must be a string, otherwise throw error
        if not isinstance(field_type_and_length, str):
            raise TypeError('Invalid field type/length string: {}'\
                    .format(convertObjToStr(field_type_and_length)))
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError('\n'.join([
                'Invalid error_if_invalid value: {}',
                'Must be a boolean value (True or False).'
                ])).format(convertObjToStr(error_if_invalid))
        # convert context to dict
        context_dict = self.__contextDict(context)
        # parse out field type and length, and run any additional validation functions
        try:
            # parse out field type and length
            field_type = self.__getTypeOnly(field_type_and_length) # returns string (errors if invalid)
            field_length = self.__getLengthOnly(field_type_and_length) # returns tuple (errors if invalid)
            # check if length/precision is appropriate for field type
            self.__isValidFieldTypeLength(field_type, field_length, context=context)
            #
            field_updated = {'type': field_type, 'length': field_length, 'context': context}
            # field_updated = self.supplementWithDefaultLengths(field_type, field_length, context=context)
            return field_updated
        except:
            raise

    def getFieldTypeAttributes(self, field_type, context=None):
        # check that field_type is a string
        if not isinstance(field_type, str):
            raise TypeError('field_type must be a string that represents a valid field type')
        # convert context to dict
        context_dict = self.__contextDict(context)
        # check if one or many contexts
        context_count = reduce(lambda x, key:x + int(context_dict[key]), context_dict, 0)
        if context_count != 1:
            raise ValueError('this function is prepared to handle  exactly one context')

        # initialize var
        for context_name in context_dict:
            if context_dict[context_name]:
                return self.columns['context'][context_name][field_type]

        # if we got this far without returning a value, then something went wrong
        raise LookupError('error looking up attributes for field_type {} in context {}'.format(field_type, context_name))

    def supplementWithDefaultLengths(self,
        field_type,
        field_length,
        context=None
    ):
        output_dict = {
            'type': field_type,
            'length': field_length
            }
        if self.debug:
            print('Supplementing field metadata with default lengths if needed:')
            print(output_dict)

        # convert context to dict
        context_dict = self.__contextDict(context)

        field_length_missing = \
            (
                (field_length is None) or
                (
                    (
                        (isinstance(field_length, tuple)) and
                        (len(field_length) == 0)
                        )
                    )
                )

        if field_length_missing:
            field_type_attributes = self.getFieldTypeAttributes(field_type, context=context)
            output_dict['length'] = field_type_attributes['default_length']
            if self.debug:
                print('field metadata updated:')
                print(output_dict)
            return output_dict
        elif not isinstance(field_length, tuple):
            raise TypeError('field_length must be a tuple of length 0 (static field length), 1 (width), or 2 (precision)')
        else:
            if self.debug:
                print('> no changes made')
            return output_dict




    def __isValidFieldTypeLength(self, field_type_str, field_length_tuple, context=None, error_if_invalid=False):
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError('\n'.join([
                'Invalid error_if_invalid value: {}',
                'Must be a boolean value (True or False).'
                ])).format(convertObjToStr(error_if_invalid))
        # check if field_length_tuple and field_type_str are valid
        # and if not, throw error
        try:
            self.__isValidFieldLengthTuple(field_length_tuple, error_if_invalid=True)
            self.__isValidFieldTypeStr(field_type_str, context=context, error_if_invalid=True)
        except:
            raise
        # convert context to dict
        context_dict = self.__contextDict(context)
        # intialize vars
        valid = False
        reasons = []
        # check whether field length is appropriate for field type (in the relevant context(s))
        for context in self.columns['context']: # eg, ayx or sqlite
            if (
                (context_dict[context]) and
                (field_type_str in self.columns['context'][context])
            ):
                field_type_metadata = self.columns['context'][context][field_type_str]
                expected_length = field_type_metadata['expected_length_dim']
                default_length = field_type_metadata['default_length']
                if (len(field_length_tuple) == expected_length):
                    valid = True
                    return valid
                elif (
                    (expected_length == 0) and
                    (default_length == field_length_tuple)
                ):
                    valid = True
                    return valid
                else:
                    context_reason = {'context': context, 'expected_length': expected_length}
                    reasons.append(context_reason)
        if not valid:
            raise ValueError(' '.join([
                'Field length tuple ({}) for specified field',
                'type ({}).',
                'Expected tuple length: {}'
                ]).format(
                    field_length_tuple,
                    field_type_str,
                    reasons
                    )
                )
        return valid


    def __isValidFieldLengthTuple(self, field_length_tuple, error_if_invalid=False):
        try:
            # check if input is tuple
            if not isinstance(field_length_tuple, tuple):
                raise TypeError(''.join([
                            'Invalid field_length_tuple value: {}',
                            'Must be a tuple.'
                            ]).format(convertObjToStr(field_length_tuple)))
            # check if error_if_invalid is boolean
            if not isinstance(error_if_invalid, bool):
                raise TypeError('\n'.join([
                    'Invalid error_if_invalid value: {}',
                    'Must be a boolean value (True or False).'
                    ])).format(convertObjToStr(error_if_invalid))
            # check that field_length_tuple doesn't contain more than two items
            if len(field_length_tuple) > 2:
                raise ValueError(''.join([
                    'Invalid field_length_tuple value: {}',
                    'Must have length of 0, 1, or 2.'
                    ]).format(convertObjToStr(field_length_tuple)))
            # check if each element is an int
            for element in field_length_tuple:
                if not isinstance(element, int):
                    raise TypeError(''.join([
                        'Invalid element in field_length_tuple: {}',
                        'Must be an integer.'
                        ]).format(convertObjToStr(element)))
            # if we made it this far without erroring, then input value is valid
            return True
        # if an error was caught, then an invalid value was provided
        except TypeError as err:
            if error_if_invalid:
                raise
            else:
                print(err)
            return False

    # check if field type string is valid, depending on context (ayx or sqlite)
    def __isValidFieldTypeStr(self, field_type_str, context=None, error_if_invalid=False):
        # check if input is tuple
        if not isinstance(field_type_str, str):
            raise TypeError('\n'.join([
                'Invalid field_type_str value: {}',
                'Must be a string.'
                ]).format(convertObjToStr(field_type_str)))
        # check if error_if_invalid is boolean
        if not isinstance(error_if_invalid, bool):
            raise TypeError('\n'.join([
                'Invalid error_if_invalid value: {}',
                'Must be a boolean value (True or False).'
                ])).format(convertObjToStr(error_if_invalid))
        # convert context to dict
        context_dict = self.__contextDict(context)

        # default assumption is that type is not valid
        type_is_valid = False
        # check if type exists in each context
        for context in context_dict:
            # for each possible context...
            if (
                # ...if a given context is currently relevant...
                context_dict[context] and
                # ...and the field type string is valid in that context
                (field_type_str in self.columns['context'][context])
                ):
                # ...then yes, this field type string is valid!
                type_is_valid = True
        # throw error if match not found (and specified by the 'throw error' argument)
        if error_if_invalid and not type_is_valid:
            raise ValueError('\n'.join([
                'Invalid column type string <{}>: {}',
                'Valid Alteryx types: {}',
                'Valid Sqlite types: {}'
                ]).format(
                    type(field_type_str),
                    field_type_str,
                    list(self.columns['context']['ayx']),
                    list(self.columns['context']['sqlite'])
                    )
                )
        # return boolean value indicating whether type is valid
        return type_is_valid


    # borderline pointless function to put context in dict
    def __contextDict(self, context):
        # valid context values
        ayx_context_string = 'ayx'
        sqlite_context_string = 'sqlite'
        # intialize context_dict
        context_dict = {}
        # check conditions -- if None, then allow both
        if context is None:
            # context_dict = {'ayx': True, 'sqlite': True}
            raise ValueError('Context value must be specified')
        elif isinstance(context, str):
            if context == ayx_context_string:
                context_dict = {ayx_context_string: True, sqlite_context_string: False}
            elif context == sqlite_context_string:
                context_dict = {ayx_context_string: False, sqlite_context_string: True}
        else:
            raise TypeError('Context must be a string.')

        # if both context strings ('ayx' and 'sqlite') are not keys in
        # context_dict, then throw an error.
        if not(
            (ayx_context_string in context_dict) and
            (sqlite_context_string in context_dict)
            ):
            # attempt to put the input context argument into a string
            try:
                context_str = '({}) '.format(context)
            except:
                context_str = ''
            # throw the error
            raise ValueError(
                'Invalid context ({}) -- must be either "{}" or "{}" (or None for both)'\
                    .format(context_str, ayx_context_string, sqlite_context_string)
                )
        return context_dict



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





class Config:
    def __init__(self, filepath=None, debug=None):

        # default value for config filepath
        if filepath is None:
            filepath = 'jupyterPipes.json'

        elif not isinstance(filepath, str):
            raise TypeError('\n'.join([
                'config filepath must be a string, not {}:'.format(
                    type(filepath)
                    ),
                convertObjToStr(filepath)
                ]))

        # check debug parameter
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug parameter must True or False')

        # set attributes
        self.filepath = filepath
        self.absolute_path = os.path.abspath(self.filepath)
        jupyter_pipes_dict = self.__getInputFileMap()
        self.input_file_map = jupyter_pipes_dict["input_map"]
        self.constant_map = jupyter_pipes_dict["constant_map"]

        if len(self.constant_map) == 0:
            raise LookupError('You must run the workflow first to make cached data and workflow constants available to the Python tool')

    def __getConfigJSON(self):
        if self.debug:
            print('Attempting to read in config file ({})'.format(
                self.absolute_path
                ))

        try:
            # check that config exists
            if fileExists(
                self.absolute_path,
                True,
                "Cached data unavailable -- run the workflow to make the input data available in Jupyter notebook"
                ):
                    if self.debug:
                        print('Config file found -- {}'.format(self.absolute_path))
                    # read in config file
                    with open(self.absolute_path) as f:
                        config = json.load(f)
                    # check if config is a dict
                    if not isinstance(config, dict):
                        raise TypeError('Input config must be a python dict')
                    return config
        except:
            print('Config file error -- {}'.format(self.absolute_path))
            raise


    # mapping of connection name to filepath {input_connection_name: filepath}
    def __getInputFileMap(self):

        if self.debug:
            print('Attempting to get input connection filepath map {}'.format(
                self.absolute_path
                ))

        try:
            # load the config json as a dict
            config = self.__getConfigJSON()
            # config is the raw json, input_map is specifically the input mapping
            # (eg, if the mapping is nested below the parent node of a larger config)
            if 'input_connections' in config:
                input_map = config['input_connections']
                # check if file is in expected format
                try:
                    isDictMappingStrToStr(input_map)
                except:
                    pass
            else:
                input_map = {}

            if 'Constants' in config:
                constant_map = config['Constants']
                self.__verifyConstantMap(constant_map)
            else:
                constant_map = {}

            return {"input_map":input_map, "constant_map" : constant_map}
        except:
            print('Config file error -- {}'.format(self.absolute_path))
            raise

    # verify that the config json is in the expected structure
    def __verifyConstantMap(self, constant_map):

        #Workflow Constants
        if not isinstance(constant_map, dict):
            raise TypeError('Constants value must be a python dict')
        for key, value in constant_map.items():
            if not isinstance(key, str):
                raise TypeError(
                    '\n'.join([
                        'Constants keys must be strings',
                        'Invalid type: {}',
                        'Invalid value: {}']).format(type(key), key)
                    )
            elif not isinstance(value, (str, float, int)):
                raise TypeError(
                    '\n'.join([
                        'Constants values must be str, float, or int',
                        'Invalid type: {}',
                        'Invalid value: {}']).format(type(value), value)
                    )
        return True



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



    def write(self, pandas_df, outgoing_connection_number, columns=None):



        if self.debug:
            print('Attempting to write out cached data to outgoing connection "{}"'.format(
                outgoing_connection_number
                ))


        msg_prefix = 'Alteryx.write(pandas_df, outgoing_connection_number): '
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
