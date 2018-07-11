

datafiles = {
    'single_simple_table': {
        'filename': 'one_table.sqlite',
        'type': 'sqlite',
        'tables': ['tmp'],
        'notes': 'one simple 2x2 table (one string field, one numeric field)'
    }
}

def getTestFileName(data_ref_name):
    return datafiles[data_ref_name]['filename']
