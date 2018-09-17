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
