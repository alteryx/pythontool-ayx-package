from ayx.CachedData import CachedData


def read(incoming_connection_name):
    data = CachedData()
    return data.read(incoming_connection_name)

def write(pandas_df, outgoing_connection_number):
    data = CachedData()
    return data.write(pandas_df, outgoing_connection_number)

def getIncomingConnectionNames():
    data = CachedData()
    return data.getIncomingConnectionNames()
