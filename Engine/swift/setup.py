from setuptools import setup, find_packages

paste_factory = ['vertigo_handler = '
                 'vertigo_middleware.handler:filter_factory']

setup(name='swift_vertigo',
      version='1.0.2',
      description='Micro-controllers for OpenStack Swift',
      author='Josep Sampe',
      packages=find_packages(),
      requires=['swift(>=1.4)', 'storlets(>=1.0)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
