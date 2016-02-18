from setuptools import setup
paste_factory = ['controller_handler = '
                 'swift_microcontroller.mc_handler:filter_factory']

setup(name='swift_microcontroller',
      version='0.0.1',
      packages=['swift_microcontroller'],
      entry_points={'paste.filter_factory': paste_factory}
      )
