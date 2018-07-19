import os, string


# return a string containing a msg followed by a filepath
def fileErrorMsg(msg, filepath=None):
    if filepath is None:
        raise ReferenceError("No filepath provided")
    return ''.join([msg, ' (', filepath, ')'])


# check if file exists. if not, throw error
def fileExists(filepath, throw_error=None, msg=None):
    # default is to not throw an error
    if throw_error is None:
        throw_error = False
    if msg is None:
        msg = 'Input data file does not exist'
    # if file exists, return true
    if os.path.isfile(filepath):
        return True
    else:
        if throw_error:
            raise FileNotFoundError(fileErrorMsg(msg, filepath))
        return False

# check if a string is a valid sqlite table name
def tableNameIsValid(table_name):
    if isString(table_name):
        # stripped = ''.join( chr for chr in table_name if (chr.isalnum() or chr=='_'))
        valid_chars = ''.join([string.ascii_letters, string.digits])
        stripped = ''.join( chr for chr in table_name if (chr in valid_chars or chr=='_'))
        if stripped != table_name:
            valid = False
            reason = 'invalid characters (only alphanumeric and underscores)'
        elif not(table_name[0].isalpha()):
            valid = False
            reason = 'first character must be a letter'
        else:
            valid = True
        return valid
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
        except:
            print(fileErrorMsg("Error: Unable to delete file", filepath))
            raise
    else:
        if debug:
            print(fileErrorMsg("Success: file does no", filepath))

def isString(var):
    return isinstance(var, str)
