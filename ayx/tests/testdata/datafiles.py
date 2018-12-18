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


datafiles = {
    'single_simple_table': {
        'filename': 'one_table.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': ['tmp'],
        'notes': 'one simple 5c x 4r table w/ coltypes (str, int, double, date, datetime)'
    },
    'invalid_sqlite_file': {
        'filename': 'invalid_db.sqlite',
        'type': 'sqlite',
        'valid_file': False,
        'tables': [],
        'notes': 'not a real sqlite file (just plain text)'
    },
    'nonexistent_sqlite_file': {
        'filename': '__does_not_exist__.sqlite',
        'type': 'sqlite',
        'valid_file': False,
        'tables': [],
        'notes': 'file does not exist'
    },
    'zero_tables': {
        'filename': 'zero_tables.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': [],
        'notes': 'valid sqlite db, but contains zero tables'
    },
    'four_tables_cnx_metadata': {
        'filename': 'input_data.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': ['input_connection_metadata','ayx137028f06553292c2fbe34041601684b','ayx6bc5d8e5e737c4dd07872b2bcf3ae2dd','ayxc4ca4238a0b923820dcc509a6f75849b'],
        'notes': 'contains 3 data tables and 1 table with mock metadata'
    },
    'main_config_file': {
        'filename': 'config.ini',
        'type': 'json',
        'valid_file': True,
        'tables': [],
        'notes': 'config file that points to 3 input sqlite files'
    },
    'main_connection #1': {
        'filename': 'input_ayx6bc5d8e5e737c4dd07872b2bcf3ae2dd.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': ['data'],
        'notes': 'test data for input connection #1'
    },
    'main_connection #2': {
        'filename': 'input_ayx137028f06553292c2fbe34041601684b.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': ['data'],
        'notes': 'test data for input connection #2'
    },
    'main_connection 1': {
        'filename': 'input_ayxc4ca4238a0b923820dcc509a6f75849b.sqlite',
        'type': 'sqlite',
        'valid_file': True,
        'tables': ['data'],
        'notes': 'test data for input connection 1'
    },
    'config_valid_2': {
        'filename': 'config_valid_tmp.ini',
        'type': 'json',
        'valid_file': True,
        'tables': [],
        'notes': 'config file that points to 3 input sqlite files'
    },
	'jupyter_pipes_config': {
        'filename': 'jupyterPipes.json',
        'type': 'json',
        'valid_file': True,
        'tables': [],
        'notes': 'config file that points to 3 input sqlite files (#1 written by engine code)'
    },
    'no_input_data_config':{
        'filename':'no_inputs.ini',
        'type':'json',
        'valid_file':True,
        'tables':[],
        'notes':'only Constants, no inputs connected yet'
    },
    'test_python_script':{
        'filename':'_test_script.py',
        'type':'python',
        'valid_file':True,
        'tables':[],
        'notes':'test python script file wth function myfunction()'
    },
    'test_python_package':{
        'filename':'mypkg',
        'type':'directory',
        'valid_file':True,
        'tables':[],
        'notes':'python package directory'
    },
}

def getValidSqliteFiles():
    return list(filter(
        lambda x: datafiles[x]['type']=='sqlite' and datafiles[x]['valid_file'],
        datafiles.keys()
        ))

def getTestFileName(data_ref_name):
    return datafiles[data_ref_name]['filename']

def getTestFileTables(data_ref_name):
    tables = datafiles[data_ref_name]['tables']
    if type(tables) is dict:
        tables = tables.keys()
    return tables

# def getColumnType(data_ref_name, table_name, column_name):
