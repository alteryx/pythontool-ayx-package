import sys, io, subprocess



def packageIsInstalled(pkg):
    if not isinstance(pkg, str):
        raise TypeError('package name must be a string')
    try:
        # if we can import the package, then good to go
        __import__(pkg)
        return True
    except ModuleNotFoundError as e:
        # get the missing package name that triggered the exception
        error_package = e.msg.split(' ')[-1][1:-1]
        # is the missing module that triggered the error the one being checked?
        # example: when executing __import__('keras') ==> "No module named 'tensorflow'"
        # because tensorflow is an optional dependency that doesn't get installed with
        # keras (user is required to install tensorflow or change setting to not use tensorflow)
        return error_package != pkg


def installPackages(package, install_type=None):
    # default install_type is install
    if install_type is None:
        install_type = 'install'
    elif isinstance(install_type, str):
        pass
    # otherwise, attempt to concatenate params together
    # eg, ['install','--user'] --> 'install --user'
    else:
        try:
            install_type = ' '.join(install_type)
        except:
            raise TypeError('install_type must be a string or list of strings')

    # similarly, if package list is a string, then good to go
    # but if it is a list of string, concatenate together
    package_argtype_error_msg = 'Package name argument must be a string or list of strings.'
    if isinstance(package, str):
        pkg_list = package
    elif isinstance(package, list):
        try:
            pkg_list = ' '.join(package)
        except TypeError:
            raise TypeError(package_argtype_error_msg)
    else:
        raise TypeError(package_argtype_error_msg)


    # put all pip args into a list
    pip_args_str = '{} {}'.format(install_type, pkg_list)
    pip_args_list = pip_args_str.split(' ')

    ## attempt to install -- approach #1 (not thread safe)
    # try:
    #     from pip import main
    # except:
    #     from pip._internal import main
    # main(pip_args)

    ## attempt to install -- approach #2
    try:
        # run "pip install <package>"
        result = subprocess.check_output(
            [sys.executable, "-m", "pip"] + pip_args_list,
            stderr = subprocess.STDOUT
            )
        # print the output
        print(result.decode("utf-8"))
    # if any errors, print them to output
    except subprocess.CalledProcessError as e:
        print(e.output.decode("utf-8"))
        raise
