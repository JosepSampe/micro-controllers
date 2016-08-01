from setuptools import setup, find_packages

paste_factory = ['vertigo_handler = '
                 'vertigo_middleware.vertigo_handler:filter_factory']

setup(name='swift_vertigo',
      version='0.0.4',
      description='Crystal filter middleware for OpenStack Swift',
      author='Josep Sampe',
      url='http://iostack.eu',
      packages=find_packages(),
      requires=['swift(>=1.4)','storlets(>=1.0)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
