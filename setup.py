from setuptools import setup
from ayx.version import version

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
      install_requires=[
          # packages required by ayx
          'pandas',
          'IPython',
          # additional packages included with Alteryx install
          'jupyter',
    	  'SQLAlchemy',
          'pywin32',
          'matplotlib',
          'numpy',
          'requests',
          'scikit-learn',
          'scipy',
          'six',
          'statsmodels'
          ],
      zip_safe=False
      )
