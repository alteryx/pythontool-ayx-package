# (C) Copyright 2018 Alteryx, Inc.
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

### NOTE! This submodule is required by the Package submodule.
### Keep this lightweight -- NO THIRD-PARTY PACKAGE REFERENCES!!!
### (eg, pandas, matplotlib, etc -- otherwise, it will be impossible
###  to update packages loaded here!)
import sys, json, subprocess
from os.path import abspath
from re import findall
from functools import reduce


def runSubprocess(args_list, debug=None):
    if debug is None:
        debug = False
    elif not isinstance(debug, bool):
        raise TypeError('debug must be True or False')
    if isinstance(args_list, str):
        args_list = [args_list]
    elif not isinstance(args_list, list):
        raise TypeError('args_list must be a list')

    if debug:
        print(' '.join(['[Executing subprocess:'] + args_list + [']']))

    try:
        result = subprocess.check_output(
            args_list,
            stderr = subprocess.STDOUT
            )
        if debug:
            print('[Subprocess success!]')
        result_decoded_lines = result.decode("utf-8").strip().split("\n")
        # if a line starts with any of the following, exclude it
        exclude_msg_prefixes = [
            "You are using pip version",
            "You should consider upgrading via the 'python -m pip install --upgrade pip' command."
            ]
        # now actually exclude all lines starting with above strings
        for prefix in exclude_msg_prefixes:
            result_decoded_lines = list(filter(
                lambda line: (
                    line[:len(prefix)] != prefix
                    ),
                result_decoded_lines
                ))
        # print the output
        output_msg = '\n'.join(result_decoded_lines)
        success = True
        error = None
        error_type = None
        if debug:
            print(output_msg)
    # if any errors, print them to output
    except subprocess.CalledProcessError as e:
        output_msg = e.output.decode("utf-8").strip()
        if debug:
            print('[Subprocess failed.]')
            print(output_msg)
        success = False
        error = e
        error_type = output_msg.split('\n')[-1].split(':')[0]

    return {
        'msg': output_msg,
        'success': success,
        'err': error,
        'err_type': error_type
        }



from pathlib import Path
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
from contextlib import contextmanager
from pkgutil import walk_packages
from hashlib import blake2b
import os.path
import sys


# temporarily add a directory to sys.path, do stuff, and then revert back sys.path
# this is called with:
#
#        with add_to_path('/d/my_dir'):
#            do_stuff()
#
@contextmanager
def add_to_path(p):
    import sys
    old_path = sys.path
    sys.path = sys.path[:]
    sys.path.insert(0, p)
    try:
        yield
    finally:
        sys.path = old_path

# this is the class used for the Alteryx.importPythonModule() function
# (which allows users to import a local package dir or script file)
class ExternalModuleLoader(object):
    def __init__(self, path=None, debug=None):
        # argument type checks
        if debug is None:
            self.debug = False
        elif isinstance(debug, bool):
            self.debug = debug
        else:
            msg = 'debug arg must be True or False'
            print(msg)
            raise TypeError(msg)
        if not isinstance(path, (str, Path)):
            msg = 'path arg must be a string or Path object'
            print(msg)
            raise TypeError(msg)

        if self.debug:
            print('-- ExternalModuleLoader().init(path={}) -- '.format(path))
        self.path = Path(os.path.abspath(path))
        # is path an existing file or directory?
        self.is_file = os.path.isfile(self.path)
        self.is_dir = os.path.isdir(self.path)
        if self.debug:
            print('self.path = {}'.format(self.path))
            print('self.is_file = {}'.format(self.is_file))
            print('self.is_dir = {}'.format(self.is_dir))
        # if neither, throw error
        if not(self.is_file or self.is_dir):
            msg = 'Unable to import module -- file does not exist ({})'.format(path)
            print(msg)
            raise ImportError(msg)
        # get containing directory and module name
        self.containing_dir = os.path.dirname(self.path)
        self.module = None
        self.ipython_notebook = None
        # if file, then get extension
        if self.is_file:
            if self.debug:
                print('file exists! {}'.format(self.path))
            filename = os.path.basename(self.path)
            (prefix, extension) = os.path.splitext(filename)
            self.extension = extension.lower()
            if self.extension == '.py':
                self.module = prefix
            elif self.extension == '.ipynb':
                self.ipython_notebook = prefix
            else:
                msg = 'Unable to import module -- file must have .py or .ipynb extension ({})'.format(path)
                print(msg)
                raise ImportError(msg)
        elif self.is_dir:
            if self.debug:
                print('dir exists! {}'.format(self.path))
            self.extension = ''
            self.module = os.path.basename(self.path)


    def makePathClean(self, path):
        if self.debug:
            print('makePathClean(): standardize a path (does not have to actually exist)...')
        full_path_str = str(Path(os.path.abspath(path)))
        if self.debug:
            print('input path: {}'.format(path))
            print('cleaned path: {}'.format(full_path_str))
        return full_path_str

    def hashString(self, string):
        if self.debug:
            print('hashString(): standardize a path (does not have to actually exist)...')
        path_hash = blake2b(digest_size=16)
        path_hash.update(string.encode('UTF-8'))
        path_hash_str = path_hash.hexdigest()
        if self.debug:
            print('input string: {}'.format(string))
            print('hashed string: {}'.format(path_hash_str))
        return path_hash_str

    def moduleNameComponents(self, path):
        if self.debug:
            print('moduleName(): breaking down module name into useful components...')

        fullpath = self.makePathClean(path)
        directory = os.path.dirname(fullpath)
        filename = os.path.basename(fullpath)
        (module_name, extension) = os.path.splitext(filename)
        path_hash = self.hashString(fullpath)

        components = {
            'fullpath': fullpath,
            'filename': filename,
            'directory': directory,
            'module_name': module_name,
            'extension': extension.lower(),
            'path_hash': path_hash
            }

        if self.debug:
            print(components)

        return components


    def importPythonFile(self, path):
        if self.debug:
            print('importPythonFile(): attempting to import file as python script...')

        module_name_components = self.moduleNameComponents(path)
        fullpath = module_name_components['fullpath']
        directory = module_name_components['directory']
        module_name = module_name_components['module_name']
        extension = module_name_components['extension']
        path_hash = module_name_components['path_hash']

        if not os.path.isfile(self.path):
            msg = 'Unable to import module -- file does not exist ({})'.format(fullpath)
            print(msg)
            raise ImportError(msg)

        if extension.lower() != '.py':
            msg = 'Unable to import module ({}) -- file must have .py extension'.format(fullpath)
            print(msg)
            raise ImportError(msg)

        module_name_w_path_hash = '{}_{}'.format(module_name, path_hash)

        # how to import a script dynamically (from the python docs):
        # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
        #
        # a problem we run into: import statements in the script for modules that
        # live in same dir will return a ModuleNotFoundError when the script
        # is run through exec_module :( Here's a great thread about it:
        # https://stackoverflow.com/questions/41861427/python-3-5-how-to-dynamically-import-a-module-given-the-full-file-path-in-the
        #
        with add_to_path(directory):
            spec = spec_from_file_location(module_name_w_path_hash, fullpath)
            output_module = module_from_spec(spec)
            spec.loader.exec_module(output_module)
            return output_module



    # import all submodules
    # (borrowed from https://stackoverflow.com/a/25562415)
    def import_submodules(self, package, recursive=True, include_submodules=None):
        """ Import all submodules of a module, recursively, including subpackages

        :param package: package (name or actual module)
        :type package: str | module
        :rtype: dict[str, types.ModuleType]
        """
        if isinstance(package, str):
            package = import_module(package)
        results = {}
        for loader, name, is_pkg in walk_packages(package.__path__):
            full_name = package.__name__ + '.' + name
            # submodule_name is full name, without top level name
            submodule_name = '.'.join(full_name.split('.')[1:])
            if (
                not isinstance(include_submodules, list) or
                submodule_name in include_submodules
            ):
                results[full_name] = import_module(full_name)
                if recursive and is_pkg:
                    results.update(self.import_submodules(full_name))
        return results


    def importPythonPackageDirectory(self, path, load_submodules=True):

        module_name_components = self.moduleNameComponents(path)
        fullpath = module_name_components['fullpath']
        directory = module_name_components['directory']
        module_name = module_name_components['module_name']
        path_hash = module_name_components['path_hash']

        if not os.path.isdir(self.path):
            msg = 'Unable to import module -- directory does not exist ({})'.format(fullpath)
            print(msg)
            raise ImportError(msg)

        module_name_w_hash = '{}_{}'.format(module_name, path_hash)

        if self.debug:
            print('attempting to import directory as python module...')
        # insert directory to path (at start, so any naming collisions will
        # not obscure our target dir)
        sys.path.insert(0, directory)
        exec_str = 'import {0} as {1}; output_module = {1};'.format(module_name, module_name_w_hash)
        if self.debug:
            print('exec\'ing: {}'.format(exec_str))
        # save local variables created during exec to a variable
        exec_locals = dict()
        # run import statement
        exec(exec_str, None, exec_locals)

        # get the output_module object from the local vars returned by
        # the exec statement
        output_module = exec_locals['output_module']
        if isinstance(load_submodules, bool):
            if load_submodules:
                submodules_imported = self.import_submodules(output_module)
        elif isinstance(load_submodules, list):
            submodules_imported = self.import_submodules(
                output_module,
                include_submodules=load_submodules
                )
        else:
            raise TypeError('load_submodules must be boolean (True loads all modules) or a list of submodules')


        # remove containing dir from paths
        if (
            str(Path(os.path.abspath(sys.path[0]))) ==
            str(Path(os.path.abspath(self.containing_dir)))
        ):
            del sys.path[0]
        else:
            raise LookupError('\n'.join([
                'the first value in sys.path is an unexpected value: ',
                '  * expected: {}'.format(self.containing_dir),
                '  * actual: {}'.format(sys.path[0])
                ]))

        return output_module

    def importPythonModule(self, if_pkg_load_submodules=True):

        if self.debug:
            print('-- ExternalModuleLoader().importPythonModule() --')
            print('self.containing_dir: {}'.format(self.containing_dir))
            print('self.module: {}'.format(self.module))

        # if path is to a file, we can load file directly
        # (without having to touch sys.path)
        if self.is_file:
            output_module = self.importPythonFile(self.path)
        # if path is to a directory, SourceFileLoader has issues, so we'll
        # temporarily add the containing directory to sys.path, import the
        # package, then remove the dir from sys.path
        elif self.is_dir:
            output_module = self.importPythonPackageDirectory(
                self.path,
                load_submodules=if_pkg_load_submodules
                )


        if self.debug:
            print('success!')
        return output_module
