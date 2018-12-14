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

### NOTE! Keep this lightweight -- NO THIRD-PARTY PACKAGE REFERENCES!!!
### (eg, pandas, etc -- otherwise, it will be impossible to update packages
###  loaded here!) 
from sys import executable as python_exe
import io, subprocess
from ayx.Utils import runSubprocess
# from ayx.packages import required as required_packages


def packageIsInstalled(pkg, debug=False):
    # check arg types
    if not isinstance(pkg, str):
        raise TypeError('package name must be a string')
    if debug is None:
        debug = False
    elif not isinstance(debug, bool):
        raise TypeError('debug must be True or False')
    # TODO: in progress -- check if package is installed (ideally without loading it)
    # "python -c 'import pkgutil; print(1 if pkgutil.find_loader(\"${pkg}\") else 0)'"
    # result = runSubprocess([python_exe, '-c', 'import {}'.format(pkg)])
    # run an external subprocess to check if we can import the package
    result = runSubprocess([python_exe, '-c', 'import {}'.format(pkg)])
    # if ModuleNotFoundError, then check what the module name is
    if (
        result['err_type'] is not None and
        result['err_type'] == 'ModuleNotFoundError'
    ):
        # get the missing package name that triggered the exception
        error_package = result['msg'].split(' ')[-1][1:-1]
        # is the missing module that triggered the error the one being checked?
        # example: when executing __import__('keras') ==> "No module named 'tensorflow'"
        # because tensorflow is an optional dependency that doesn't get installed with
        # keras (user is required to install tensorflow or change setting to not use tensorflow)
        return error_package != pkg
    # if not moduleNotFoundError, then return whether subprocess was success or not
    return result['success']




def installPackages(package, install_type=None, debug=None):
    if debug is None:
        debug = False
    elif not isinstance(debug, bool):
        raise TypeError('debug must be True or False')

    # default install_type is install
    if install_type is None:
        install_type_list = ['install']
    elif isinstance(install_type, str):
        if install_type == 'uninstall':
            install_type_list = ['uninstall', '-y']
        else:
            install_type_list = install_type.split(' ')
    elif isinstance(install_type, list):
        if len(install_type) == 1 and install_type[0] == 'uninstall':
            install_type_list = ['uninstall', '-y']
        else:
            install_type_list = list(map(
                lambda x: '{}'.format(x).strip(),
                install_type
                ))
    # otherwise, attempt to concatenate params together
    # eg, ['install','--user'] --> 'install --user'
    else:
        raise TypeError('install_type must be a string or list of strings')

    # similarly, if package list is a string, then good to go
    # but if it is a list of string, concatenate together
    package_argtype_error_msg = 'Package name argument must be a string or list of strings.'
    if isinstance(package, str):
        pkg_list = package.split(' ')
    elif isinstance(package, list):
        for pkg in package:
            if not isinstance(pkg, str):
                raise TypeError(package_argtype_error_msg)
        pkg_list = package
    else:
        raise TypeError(package_argtype_error_msg)


    # put all pip args into a list
    pip_args_list = install_type_list + pkg_list
    pip_args_str = ' '.join(pip_args_list)

    ## attempt to install -- approach #1 (not thread safe)
    # try:
    #     from pip import main
    # except:
    #     from pip._internal import main
    # main(pip_args)

    ## attempt to install -- approach #2
    pip_install_result = runSubprocess(
            [python_exe, "-m", "pip"] + pip_args_list,
            debug=debug
            )

    print(pip_install_result['msg'])
    if not pip_install_result['success']:
        raise pip_install_result['err']
