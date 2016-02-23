from setuptools import setup
paste_factory = ['vertigo_handler = '
                 'swift_vertigo.vertigo_handler:filter_factory']

setup(name='swift_vertigo',
      version='0.0.1',
      packages=['swift_vertigo'],
      entry_points={'paste.filter_factory': paste_factory}
      )