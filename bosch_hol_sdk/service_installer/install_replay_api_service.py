#!/usr/bin/env python3.9
'''
The script helps to install service for replay API for replay PC1 for Bosch project PP16699

Requirements: Python3.9

by XiangL, DSC, 2023/8/17

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
Usage: See python3.9 install_replay_api_service.py -h
'''
import argparse
import pathlib

try:
    # This is necessary for when executing the script directly.
    from service_installer import PythonServiceInstaller, ArgumentCustomizer
except ImportError:
    # This is how it would work when used from the entry point.
    from .service_installer import PythonServiceInstaller, ArgumentCustomizer


class DrapiAddressCustomizer(ArgumentCustomizer):
    def add_argument(self, parser):
        parser.add_argument(
            '--grpc-address',
            default=argparse.SUPPRESS,
            help='The address the gRPC would listen to',
            type=str,
            metavar='ADDRESS',
        )

    def create_service_argument(self, parser_args):
        args_dict = vars(parser_args)
        address = args_dict.get('grpc_address', None)
        if address is None:
            return None
        return f'--address {address}'

class DrapiStatusCustomizer(ArgumentCustomizer):  
    def add_argument(self, parser): 
        parser.add_argument(
            '--status-request-delay',
            '-d',
            default=argparse.SUPPRESS,
            help='The time in seconds between the calls to get_progress ',
            type=float,
            metavar='DELAY',
        )
    
    def create_service_argument(self, parser_args):
        args_dict = vars(parser_args)
        status_request_delay = args_dict.get('status_request_delay')
        if status_request_delay is None:
            return None
        return f'--status-request-delay {status_request_delay}'


def install_service():
    # Find service path.
    cwd = pathlib.Path(__file__).parent
    service_path = cwd.parent / 'DataReplayAPI' / 'drapi'
    executable_path = service_path / 'drapi.py'

    # Create service installer object
    installer = PythonServiceInstaller(
        name='replay_api_server',
        executable_path=executable_path,
        description='dSPACE Replay API server (a gRPC server)',
    )
    installer.add_custom_argument(DrapiAddressCustomizer())
    installer.add_custom_argument(DrapiStatusCustomizer())

    # Check the existence of components of replay API
    COMPONENTS_REPLAY_API = [
        "replay_executor.py",
        "datareplay_pb2.py",
        "datareplay_pb2_grpc.py",
        "rtmaps.py",
        "system_helper.py",
        "helper/cmdloader.py",
        "helper/scalexio_control.py",
        "helper/tcpdump_interface.py",
        "helper/xilapi_maport.py",
    ]

    for component in COMPONENTS_REPLAY_API:
        path_to_component = service_path / component
        if not path_to_component.exists():
            installer.logger.error(
                f"The component of replay API is missing: <{component}>!"
            )
            installer.logger.error("Aborting installing the service.")
            raise FileNotFoundError(path_to_component)

    # Install the service.
    installer.run()


if __name__ == '__main__':
    install_service()
