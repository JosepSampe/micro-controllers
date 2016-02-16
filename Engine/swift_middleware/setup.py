from setuptools import setup
paste_factory = ['controller_handler = '
                 'swift_controller.controller_handler:filter_factory']

setup(name='swift_controller',
      version='0.1',
      packages=['swift_controller'],
      entry_points={'paste.filter_factory': paste_factory}
      )
