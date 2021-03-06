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
import string
from IPython.display import display, Markdown

def convertObjToStr(obj):
    try:
        obj_str = '{}'.format(obj)
    except:
        obj_str = '<{}>'.format(type(obj))
    return obj_str

# return a string containing a msg followed by a filepath
def fileErrorMsg(msg, filepath=None):
    if filepath is None:
        raise ReferenceError("No filepath provided")
    return ''.join([msg, ' (', filepath, ')'])


# check if file exists. if not, throw error
def fileExists(filepath, throw_error=None, msg=None, debug=None):
    # default is to not throw an error
    if throw_error is None:
        throw_error = False
    if msg is None:
        msg = 'Input data file does not exist'
    # set default for debug
    if debug is None:
        debug = False
    elif not isinstance(debug, bool):
        raise TypeError('debug value must be True or False')
    # if file exists, return true
    if os.path.isfile(filepath):
        if debug:
            print(fileErrorMsg('File exists', filepath))
        return True
    elif os.path.isdir(filepath):
        if debug:
            print(fileErrorMsg('Directory exists', filepath))
        return True
    else:
        if throw_error:
            raise FileNotFoundError(fileErrorMsg(msg, filepath))
        elif debug:
            print(fileErrorMsg(msg, filepath))
        return False

# check if a string is a valid sqlite table name
def tableNameIsValid(table_name):
    if isString(table_name):
        # stripped = ''.join( char for char in table_name if (char.isalnum() or char=='_'))
        valid_chars = ''.join([string.ascii_letters, string.digits, '_'])
        stripped = ''.join(char for char in table_name if (char in valid_chars))
        if stripped != table_name:
            valid = False
            reason = 'invalid characters (only alphanumeric and underscores)'
        elif not table_name[0].isalpha():
            valid = False
            reason = 'first character must be a letter'
        else:
            valid = True
            reason = None
        return valid, reason
    else:
        raise TypeError('table name must be a string')

# delete a file (if it exists)
def deleteFile(filepath, debug=None):
    # set default for debug
    if debug is None:
        debug = False
    elif not isinstance(debug, bool):
        raise TypeError('debug value must be True or False')

    # if file exists, attempt to delete it
    if fileExists(filepath, throw_error=False):
        try:
            os.remove(filepath)
            if debug:
                print(fileErrorMsg("Success: file deleted", filepath))
        except:
            print(fileErrorMsg("Error: Unable to delete file", filepath))
            raise
    else:
        if debug:
            print(fileErrorMsg("Success: file does not exist", filepath))

def isString(var):
    return isinstance(var, str)


def isDictMappingStrToStr(d):
    try:
        if not isinstance(d, dict):
            raise TypeError('Input must be a python dict')
        elif not all(isinstance(item, str) for item in d.keys()):
            raise TypeError('All keys must be strings')
        elif not all(isinstance(d[item], str) for item in d.keys()):
            raise TypeError('All mapped values must be strings')
        else:
            return True
    except:
        print('Input: {}'.format(convertObjToStr(d)))
        raise

def prepareMultilineMarkdownForDisplay(markdown):
    # split each line of input markdown into items in a list
    md_lines = markdown.splitlines()
    # strip each line of whitespace
    md_lines_stripped = list(map(lambda x: x.strip(), md_lines))
    # add trailing spaces and newline char between lines when joining back together
    markdown_prepared = '  \n'.join(md_lines_stripped)
    # return prepared markdown
    return markdown_prepared


def displayMarkdown(markdown):
    # display the prepared markdown as formatted text
    display(Markdown(markdown))


def convertToType(value, datatype):
    try:
        # special case: string to bool
        if (datatype is bool) and isinstance(value,str):
            if value.strip()[0].upper() in ['T', 'Y']:
                return True
            elif value.strip()[0].upper() in ['F', 'N']:
                return False
        # general case
        return datatype(value)
    except Exception as e:
        msg = "Unable to convert {} value to data type {}: {}\n>> {}"\
                    .format(type(value), datatype, value, e)
        print(msg)
        raise
