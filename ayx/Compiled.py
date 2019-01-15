## from ayx.compiled_modules import Python_AlteryxYXDB as pyxdb
# from ayx.compiled_modules import InPythonAlteryxDBReader as pyxdb
# ugly temporary solution to the above not working... pyd file for some reason has to be in this hardcoded location


import PyYXDBReader as pyxdb


def pyxdbLookupFieldTypeEnum(fieldtype, debug=None):
    # input argument error checking
    if not isinstance(fieldtype, str):
        raise TypeError(
            "a string is expected, not a {} (value: {})".format(
                type(fieldtype), fieldtype
            )
        )
    # elif fieldtype not in self.columns["context"]["yxdb"].keys():
    #     raise ValueError("not a valid alteryx field type")
    # prepare lookupvalue to find enumeration based on its name
    lookupvalue = fieldtype.lower().replace(" ", "")
    if lookupvalue == "boolean":
        lookupvalue = "bool"
    # return enumeration
    return pyxdb.FieldType.names[lookupvalue]
