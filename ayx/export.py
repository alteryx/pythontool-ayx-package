from ayx.CachedData import CachedData as __CachedData__
from ayx.Help import Help as __Help__
from ayx.Package import installPackages as __installPackages__
from ayx.version import version as __version__


def help():
    '''
    Run this function to get a quick overview of useful functions in the Alteryx library.
    '''
    __Help__().display()

def read(incoming_connection_name):
    '''
    When running the workflow in Alteryx, this function will convert incoming data streams to pandas dataframes when executing the code written in the Python tool. When called from the Jupyter notebook interactively, it will read in a copy of the incoming data that was cached on the previous run of the Alteryx workflow.
    '''
    return __CachedData__().read(incoming_connection_name)

def write(pandas_df, outgoing_connection_number):
    '''
    When running the workflow in Alteryx, this function will convert a pandas data frame to an Alteryx data stream and pass it out through one of the tool's five output anchors. When called from the Jupyter notebook interactively, it will display a preview of the pandas dataframe.
    '''
    return __CachedData__().write(pandas_df, outgoing_connection_number)

def getIncomingConnectionNames():
    '''
    This function will return a list containing the names of all incoming connections. As with the read function, a cached version of incoming data connections is referenced when running interactively in the Jupyter notebook. (If the list of incoming connections shown does not match the connections in your workflow, simply run the workflow to refresh the cache.)
    '''
    return __CachedData__().getIncomingConnectionNames()

def installPackage(package, *args, **kwargs):
    '''
    This function will install a package or list of packages into the virtual environment used by the Python tool. If using an admin installation of Alteryx, you must run Alteryx as administrator in order to use this function and install packages.
    '''
    __installPackages__(package, *args, **kwargs)

# these are the same function.
installPackages = installPackage
