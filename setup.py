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
from setuptools import setup
from ayx.version import version
from ayx.packages import all_packages

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='ayx',
      version=version,
      description='python package for alteryx designer',
      long_description=readme(),
      url='',
      author='alteryx',
      author_email='alteryxanalyticproducts@alteryx.com',
      tests_require=['nose'],
      test_suite='nose.collector',
      packages=['ayx'],
      install_requires=all_packages,
      zip_safe=False
      )
