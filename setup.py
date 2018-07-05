from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='ayx',
      version='0.0.1',
      description='python package for alteryx designer',
      long_description=readme(),
      url='',
      author='alteryx',
      author_email='alteryxanalyticproducts@alteryx.com',
      tests_require=['nose'],
      test_suite='nose.collector',
      packages=['ayx'],
      install_requires=[
          'markdown',
      ],
      zip_safe=False)
     )