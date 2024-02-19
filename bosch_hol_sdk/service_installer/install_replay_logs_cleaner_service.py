#!/usr/bin/env python3.9
"""
The script helps to install service for automatically deleting old drapi logs.
"""
import argparse
import pathlib

try:
    # This is necessary for when executing the script directly.
    from service_installer import TimerServiceInstaller, ArgumentCustomizer
except ImportError:
    # This is how it would work when used from the entry point.
    from .service_installer import TimerServiceInstaller, ArgumentCustomizer


class ExpirationCustomizer(ArgumentCustomizer):
    def add_argument(self, parser):
        parser.add_argument(
            '--expiration-duration',
            default=30,
            help='The expiration duration of logs files in days',
            type=int,
            metavar='DAYS',
        )

    def create_service_argument(self, parser_args):
        duration = parser_args.expiration_duration
        return f'-mtime +{duration} -print -delete'


def install_service():
    # Create service installer object
    installer = TimerServiceInstaller(
        name='replay_logs_cleaner',
        frequency='daily',
        exec_start='date && find /var/log/dspace/',
        description='Timer service to delete old drapi logs',
    )
    installer.add_custom_argument(ExpirationCustomizer())

    # Install the service.
    installer.run()


if __name__ == '__main__':
    install_service()
