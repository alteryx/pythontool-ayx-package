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
        *The Alteryx GUI Workflow Constant will be returned (if it exists) as either a string or a double or an int, depending on what is put in the AlteryxGUI. If the const_name does not exist, it throws a ReferenceException. If you are SURE it is in the Alteryx GUI and it isn't showing up in python... make sure you didn't try to pass a string ("foobar") as a number.<br/> return_path will return a python 3.4 pathlib instead of a string (please use this so linux/windows slash direction problems go away). The default value is False*
        > ```d = Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")```<br/>```print(d)```
        >► <span style="color:grey">*s:\\Alteryx\\bin_x64\\Debug\\*</span>
        >```ud  = Alteryx.getWorkflowConstant("Engine.WorkflowDirectory",return_path=True)```<br/>```ud```
         >► <span style="color:grey">*WindowsPath('s:/Alteryx/bin_x64/Debug')*</span>
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
