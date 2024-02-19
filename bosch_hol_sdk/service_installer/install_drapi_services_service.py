#!/usr/bin/env python3.9
"""
A script to install the DRAPI services service for Bosch project PP16699

@copyright
    Copyright 2023, dSPACE Mechatronic Control Technology (Shanghai) Co., Ltd.
    All rights reserved.
"""
import argparse
import pathlib

try:
    # This is necessary for when executing the script directly.
    from service_installer import PythonServiceInstaller, ArgumentCustomizer
except ImportError:
    # This is how it would work when used from the entry point.
    from .service_installer import PythonServiceInstaller, ArgumentCustomizer


class AddressCustomizer(ArgumentCustomizer):
    def add_argument(self, parser):
        parser.add_argument(
            '--grpc-host',
            default=argparse.SUPPRESS,
            help='The hostname/ip the gRPC-Server would listen to',
            type=str,
            metavar='HOST',
        )
        parser.add_argument(
            '--grpc-port',
            default=argparse.SUPPRESS,
            help='The port the gRPC-Server would listen to',
            type=int,
            metavar='PORT',
        )

    def create_service_argument(self, parser_args):
        args_dict = vars(parser_args)
        host = args_dict.get('grpc_host', None)
        port = args_dict.get('grpc_port', None)

        arguments = []
        if host is not None:
            arguments.append(f'--host {host}')
        if port is not None:
            arguments.append(f'--port {port}')
        if not arguments:
            return None
        return ' '.join(arguments)


class VerbosityCustomizer(ArgumentCustomizer):
    def add_argument(self, parser):
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            '--verbose',
            help='Increase the service logs to debug messages',
            action='store_true',
            default=False,
        )
        group.add_argument(
            '--quiet',
            help='limit the service logs to only warnings and errors',
            action='store_true',
            default=False,
        )

    def create_service_argument(self, parser_args):
        args_dict = vars(parser_args)
        verbosity = 2
        if args_dict.get('quiet'):
            verbosity -= 1
        if args_dict.get('verbose'):
            verbosity += 1
        return f'-{"v"*verbosity}'


def install_service():
    # Find service path.
    cwd = pathlib.Path(__file__).parent
    service_path = cwd.parent / 'DrapiServices'
    executable_path = cwd.parent / 'DrapiServices' / 'DrapiServices.py'

    # Create service installer object
    installer = PythonServiceInstaller(
        name='drapi_services',
        executable_path=executable_path,
        description='dSPACE Replay API services (a gRPC server)',
    )
    installer.add_custom_argument(AddressCustomizer())
    installer.add_custom_argument(VerbosityCustomizer())
    # Install the service.
    installer.run()


if __name__ == '__main__':
    install_service()
