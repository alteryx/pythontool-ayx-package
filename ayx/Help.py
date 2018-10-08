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
from ayx.helpers import prepareMultilineMarkdownForDisplay, displayMarkdown

class Help:
    def __init__(self, debug=None):

        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            raise TypeError('debug must be True or False')

        self.markdown_text = '''

        ### Code snippets for passing data between Alteryx and Jupyter
        *(See help documentation for additional details.)*


        * **Alteryx.read(&nbsp;**<span style="color:blue">"*&lt;input connection name&gt;"&nbsp;*</span>**)**
        *Input data will be returned as pandas dataframe. [<span style="font-weight:bold">Note:</span> You must run the workflow to cache the incoming data and make it accessible within this interactive notebook.]*
        > ```df = Alteryx.read("#1")```
        > ► <span style="color:grey">*SUCCESS: reading input data "#1"*</span>
        * **Alteryx.write(&nbsp;**<span style="color:blue">*&lt;pandas dataframe&gt;*</span>**<span>, </span>**<span style="color:blue">*&lt;output anchor&gt;*</span>&nbsp;**)**
        *A preview of the data will be displayed in Jupyter, but the full dataframe will be passed to Alteryx when the workflow is executed.*
        > ```Alteryx.write(df, 1)```
        > ► <span style="color:grey">*SUCCESS: writing outgoing connection data 1*</span>
        * **Alteryx.getIncomingConnectionNames(&nbsp;)**
        *A list containing all incoming data connections will be returned. If the connections look out of sync, re-run the Alteryx workflow. (As with the read function, a snapshot of the data from the previous run is used when the function is called interactively.)*
        > ```Alteryx.getIncomingConnectionNames()```
        > ► <span style="color:grey">*["#1", "#2", "model"]*</span>
        * **Alteryx.installPackages(&nbsp;**<span style="color:blue">"*&lt;package name or list of package names&gt;"&nbsp;*</span>**)**
        *Package(s) will be installed from PyPI. [<span style="font-weight:bold">Note:</span> An internet connection is required. Also, if using an admin install of Alteryx, Alteryx must be opened in admin mode to install packages. Non-admin installs do not have this restriction.]*
        > ```Alteryx.installPackages("tensorflow")```<br/>```Alteryx.installPackages(["keras","theano","gensim"])```
        * **Alteryx.getWorkflowConstant( &nbsp;**<span style="color:blue">"*&lt;const_name,return_path=False&gt;"&nbsp;*</span>**)**
        *The value of an Alteryx workflow constant (as shown on the Workflow Configuration's **Workflow** tab in Alteryx) will be returned as a string, int, or float (depending the value and whether the constant is set to numeric).
        > ```Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")```
        > ► <span style="color:grey">*'C:\\Program Files\\Alteryx\\bin'*</span>
        '''



    # the motivation behind splitting these functions out to such a degree
    # is for testing purposes
    def __prepareHelpForDisplay(self):
        return prepareMultilineMarkdownForDisplay(self.markdown_text)

    # display the markdown (renders in ipython/jupyter notebook, but does
    # not return a value)
    def display(self, return_markdown_text=None):
        prepared_help_markdown = self.__prepareHelpForDisplay()
        displayMarkdown(prepared_help_markdown)

        # return markdown text if return_markdown_text is true
        if isinstance(return_markdown_text, bool):
            if return_markdown_text:
                return prepared_help_markdown
        elif return_markdown_text is None:
            pass
        else:
            raise TypeError('return_markdown_text parameter must True or False')
