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
from ayx.CachedData import CachedData as __CachedData__
from ayx.Help import Help as __Help__
from ayx.Package import installPackages as __installPackages__
from ayx.version import version as __version__
from ayx.Utils import ExternalModuleLoader as __ExternalModuleLoader__


def help(debug=None, **kwargs):
    '''
    Run this function to get a quick overview of useful functions in the Alteryx library.
    '''
    __Help__(debug=debug).display()

def read(incoming_connection_name, debug=None, **kwargs):
    '''
    When running the workflow in Alteryx, this function will convert incoming data streams to pandas dataframes when executing the code written in the Python tool. When called from the Jupyter notebook interactively, it will read in a copy of the incoming data that was cached on the previous run of the Alteryx workflow.
    '''
    return __CachedData__(debug=debug).read(incoming_connection_name, **kwargs)

def readMetadata(incoming_connection_name, debug=None, **kwargs):
    '''
    This function will return a dict having field names as keys being mapped to attributes that describe the input data stream (currently just 'type' and 'length'). For example,

        metadata_dict = Alteryx.readMetadata("#1")
        print(metadata_dict)
        >>> {
                'field_1': {
                    'type': 'V_WString',
                    'length': 1000
                    },
                'field_2': {
                    'type': 'Int64',
                    'length': 16
                    }
                }

    Like the Alteryx.read() function, when this function is executed by the user in the interactive configuration panel (through the Jupyter notebook interface), Alteryx.readMetadata() accesses cached data. Run the workflow once to cache the incoming data for the first time so that it can be used by the Python tool, and as a best practice, re-run the workflow after making workflow changes upstream of the Python tool, so that the Python script being written in the Python tool is using the same data that will be used when the workflow is executed in full. When refreshing the Python tool cache, you may want to put downstream tools into a disabled Tool Container.

    This can also be with a pandas dataframe to generate its default output metadata (which can then be manipulated). For example:

        import pandas as pd
        pandas_df = pd.DataFrame([['a', 1], ['b', 0], ['c', 0]])
        pandas_md = Alteryx.readMetadata(pandas_df)
        print(pandas_md)
        >>> {
                0: {
                    'type': 'V_WString',
                    'length': 2147483647
                    },
                1: {
                    'type': 'Int64',
                    'length': 8
                    }
                }

        pandas_md[0]['name'] = 'Field_1'
        pandas_md[1] = {'name': 'Field_2', 'type': 'Boolean'}
        Alteryx.write(pandas_df, 1, pandas_md)

    [Note that Field_2 will only appear as Boolean in Alteryx datastream 1 flowing out of the tool, but will still appear as an integer in the pandas dataframe displayed by the Jupyter interface in the Python tool) ]
    '''
    return __CachedData__(debug=debug).readMetadata(incoming_connection_name, **kwargs)

def write(pandas_df, outgoing_connection_number, columns=None, debug=None, **kwargs):
    '''
    When running the workflow in Alteryx, this function will convert a pandas data frame to an Alteryx data stream and pass it out through one of the tool's five output anchors. When called from the Jupyter notebook interactively, it will display a preview of the pandas dataframe. An optional 'columns' argument allows column metadata to specify the field type, length, and name of columns in the output data stream.
    '''
    return __CachedData__(debug=debug).write(pandas_df, outgoing_connection_number, columns=columns, **kwargs)

def writePlot(matplotlib_pyplot, outgoing_connection_number, debug=None, **kwargs):
    '''
    When running the workflow in Alteryx, this function will convert a plot created with matplotlib to an Alteryx data stream and pass it out through one of the tool's five output anchors. When called from the Jupyter notebook interactively, it will display a preview of the plot.
    '''
    return __CachedData__(debug=debug).writePlot(matplotlib_pyplot, outgoing_connection_number, **kwargs)


def getIncomingConnectionNames(debug=None, **kwargs):
    '''
    This function will return a list containing the names of all incoming connections. As with the read function, a cached version of incoming data connections is referenced when running interactively in the Jupyter notebook. (If the list of incoming connections shown does not match the connections in your workflow, simply run the workflow to refresh the cache.)
    '''
    return __CachedData__(debug=debug).getIncomingConnectionNames(**kwargs)

def getWorkflowConstant(constant_name, debug=None, **kwargs):
    '''
    This function will return the Alteryx workflow constant as a string, int, or float, depending on the value and whether the "#" checkbox is checked for the constant in the Alteryx GUI. Like the read function, getting workflow constants in the Python tool depends on the workflow being run first so that the Python tool will have cached the data and constants and made them available in the Jupyter notebook.
    '''
    return __CachedData__(debug=debug).getWorkflowConstant(constant_name, **kwargs)

def getWorkflowConstantNames(debug=None, **kwargs):
    '''
    This function will return a list of the Alteryx workflow constants. Like the read function, getting workflow constants in the Python tool depends on the workflow being run first so that the Python tool will have cached the data and constants and made them available in the Jupyter notebook.
    '''
    return __CachedData__(debug=debug).getWorkflowConstantNames(**kwargs)

def getWorkflowConstants(debug=None, **kwargs):
    '''
    This function will return a dict of the Alteryx workflow constants and corresponding values. Like the read function, getting workflow constants in the Python tool depends on the workflow being run first so that the Python tool will have cached the data and constants and made them available in the Jupyter notebook.
    '''
    return __CachedData__(debug=debug).getWorkflowConstants(**kwargs)


def importPythonModule(path, submodules=True, debug=None, **kwargs):
    '''
    This function will import a python script or directory module and returns it as a module.
    '''
    return __ExternalModuleLoader__(path=path, debug=debug).importPythonModule(if_pkg_load_submodules=submodules, **kwargs)


def installPackage(package, install_type=None, debug=None, **kwargs):
    '''
    This function will install a package or list of packages into the virtual environment used by the Python tool. If using an admin installation of Alteryx, you must run Alteryx as administrator in order to use this function and install packages.
    '''
    __installPackages__(package, install_type=install_type, debug=debug, **kwargs)

# these are the same function.
installPackages = installPackage
