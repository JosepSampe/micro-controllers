from setuptools import setup
paste_factory = ['vertigo_handler = '
                 'swift_vertigo.vertigo_handler:filter_factory']

setup(name='swift_vertigo',
      version='0.0.2',
      author='Josep Sampé',
      packages=['swift_vertigo'],
      requires=['swift(>=1.4)'],
      install_requires=['storlets_swift>=1.0'],
      entry_points={'paste.filter_factory': paste_factory}
      )
