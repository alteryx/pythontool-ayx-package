from ayx.CachedData import CachedData as __CachedData__
from ayx.Help import Help as __Help__
from ayx.Package import installPackages as __installPackages__
from ayx.version import version as __version__


def help(debug=None, **kwargs):
    '''
    Run this function to get a quick overview of useful functions in the Alteryx library.
    '''
    __Help__(debug=debug).display()

def getWorkflowConstant(constantName,debug=None, return_as_path=False, **kwargs):
    return __CachedData__(debug=debug).getWorkflowConstant(constantName, return_as_path, **kwargs)

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
                    'length': (1000,)
                    },
                'field_2': {
                    'type': 'Int64',
                    'length': (16,)
                    }
                }

    Like the Alteryx.read() function, when this function is executed by the user in the interactive configuration panel (through the Jupyter notebook interface), Alteryx.readMetadata() accesses cached data. Run the workflow once to cache the incoming data for the first time so that it can be used by the Python tool, and as a best practice, re-run the workflow after making workflow changes upstream of the Python tool, so that the Python script being written in the Python tool is using the same data that will be used when the workflow is executed in full. When refreshing the Python tool cache, you may want to put downstream tools into a disabled Tool Container.
    '''
    return __CachedData__(debug=debug).readMetadata(incoming_connection_name, **kwargs)

def write(pandas_df, outgoing_connection_number, debug=None, **kwargs):
    '''
    When running the workflow in Alteryx, this function will convert a pandas data frame to an Alteryx data stream and pass it out through one of the tool's five output anchors. When called from the Jupyter notebook interactively, it will display a preview of the pandas dataframe.
    '''
    return __CachedData__(debug=debug).write(pandas_df, outgoing_connection_number, **kwargs)

def getIncomingConnectionNames(debug=None, **kwargs):
    '''
    This function will return a list containing the names of all incoming connections. As with the read function, a cached version of incoming data connections is referenced when running interactively in the Jupyter notebook. (If the list of incoming connections shown does not match the connections in your workflow, simply run the workflow to refresh the cache.)
    '''
    return __CachedData__(debug=debug).getIncomingConnectionNames(**kwargs)

def installPackage(package, install_type=None, debug=None, **kwargs):
    '''
    This function will install a package or list of packages into the virtual environment used by the Python tool. If using an admin installation of Alteryx, you must run Alteryx as administrator in order to use this function and install packages.
    '''
    __installPackages__(package, install_type=install_type, debug=debug, **kwargs)

# these are the same function.
installPackages = installPackage
