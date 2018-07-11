import os

# set working directory to testdata subfolder 
def set_wd():

    return os.chdir(
        os.path.join(
            os.path.dirname(__file__),
            'testdata'
            )
        )
