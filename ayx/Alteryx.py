from ayx.CachedData import CachedData


def read(incoming_connection_name):
    return CachedData().read(incoming_connection_name)

def write(pandas_df, outgoing_connection_number):
    return CachedData().write(pandas_df, outgoing_connection_number)

def getIncomingConnectionNames():
    return CachedData().getIncomingConnectionNames()
