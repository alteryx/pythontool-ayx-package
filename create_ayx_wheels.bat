:: SETUP
REM :: delete virtual env used to generate dependency list
if exist ".\depsenv\" rd /s /q ".\depsenv\"
REM :: delete the output folder containing the wheels and requirements.txt file
del ".\deps\*" /s /q
REM :: create a new virtualenv
virtualenv depsenv
REM :: activate the virtualenv
CALL "depsenv/scripts/activate.bat"
@echo on

:: PIP INSTALL
REM :: install the ayx package into the virtualenv (this takes a few mins)
pip install .
REM :: create a wheel for the ayx package
python setup.py sdist bdist_wheel -d "deps\wheels"
REM :: create a requirements.txt file that contains 'ayx' and all dependent packages
pip freeze > deps\requirements.txt
REM :: remove ayx from the packages installed on the virtualenv
pip uninstall -y ayx
REM :: create a requirements file that contains only the dependent packages (but not 'ayx')
pip freeze > deps\deps.txt
REM :: generate wheels for all dependent packages
pip wheel -r deps\deps.txt -w deps\wheels
REM :: delete deps.txt
del ".\deps\deps.txt" /s /q
REM :: install nosetests to virtualenv so that we can run all tests on the package
pip install nosetests
REM :: run nosetests
nosetests

:: PROMPT TO COPY FILES
@echo off
ECHO ==================================================================
ECHO All wheels have been created and tests have been run! 
ECHO Check logs above and review the contents of the deps folder (%cd%\deps).
ECHO ---------------------
ECHO The next step will copy the wheels and requirements to 
ECHO S:\3rdParty\Python\Miniconda3\PythonTool_Config
ECHO ---------------------
ECHO Do you want to continue?
ECHO ==================================================================
@echo on
pause 

:: COPY FILES
REM :: set path to destination
SET dest=S:\3rdParty\Python\Miniconda3\PythonTool_Config
REM :: get drive from destination path
SET drive=%dest:~0,2%
REM :: get path to origin
SET origin=%cd%\deps
REM :: delete all files in destination folder (retain folder structure though)
del "%dest%\deps\*" /s /q
REM :: copy files from origin to destination
xcopy "%origin%\*" "%dest%" /s /i

:: COMPLETE
@echo off
ECHO ==================================================================
ECHO Files have been copied! 
ECHO Check the logs above for copy errors and hit continue to execute a quick cleanup.
ECHO ==================================================================
@echo on
pause 
