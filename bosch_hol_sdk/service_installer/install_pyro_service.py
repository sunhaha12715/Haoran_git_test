#!/usr/bin/env python3.9
'''
The script helps to install service for Pyro Server on replay PC2 for Bosch project PP16699

Requirements: Python3.9

by XiangL, DSC, 2023/8/21

##

Change log:

rev_0_3:
- No positional argument required any more. The script installs the drapi
  provided in its repo/python-package.

rev_0_2:
- bugfix of the positional argument of path_to_replay_api
- minor change to disable/stop the service by default

rev_0_1:
- The first version

##
Usage: See python3.9 install_pyro_service.py -h
'''
import argparse
import pathlib

try:
    # This is necessary for when executing the script directly.
    from service_installer import PythonServiceInstaller, ArgumentCustomizer
except ImportError:
    # This is how it would work when used from the entry point.
    from .service_installer import PythonServiceInstaller, ArgumentCustomizer


class PyroAddressCustomizer(ArgumentCustomizer):
    def add_argument(self, parser):
        parser.add_argument(
            '--pyro-host',
            default=argparse.SUPPRESS,
            help='The hostname/ip the Pyro-Server would listen to',
            type=str,
            metavar='HOST',
        )
        parser.add_argument(
            '--pyro-port',
            default=argparse.SUPPRESS,
            help='The port the Pyro-Server would listen to',
            type=int,
            metavar='PORT',
        )

    def create_service_argument(self, parser_args):
        args_dict = vars(parser_args)
        host = args_dict.get('pyro_host', None)
        port = args_dict.get('pyro_port', None)

        arguments = []
        if host is not None:
            arguments.append(f'--host {host}')
        if port is not None:
            arguments.append(f'--port {port}')
        if not arguments:
            return None
        return ' '.join(arguments)


def install_service():
    # Find service path.
    cwd = pathlib.Path(__file__).parent
    service_path = cwd.parent / 'PyroServer'
    executable_path = service_path / 'server.py'

    # Create service installer object
    installer = PythonServiceInstaller(
        name='pyro_server_replay_api',
        executable_path=executable_path,
        description='Pyro server for dSPACE Replay API',
    )
    installer.add_custom_argument(PyroAddressCustomizer())

    # Check the existence of components of replay API
    COMPONENTS_REPLAY_API = ["rtmaps.py"]

    for component in COMPONENTS_REPLAY_API:
        path_to_component = service_path / component
        if not path_to_component.exists():
            installer.logger.error(
                f"The component of Pyro server for replay API is missing: <{component}>!"
            )
            installer.logger.error("Aborting installing the service.")
            raise FileNotFoundError(path_to_component)

    # Install the service.
    installer.run()


if __name__ == '__main__':
    install_service()
