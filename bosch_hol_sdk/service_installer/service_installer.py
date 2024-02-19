"""
A helper class to generically install systemd-services.

Copyright 2023, dSPACE GmbH. All rights reserved.
"""
# Dependencies
import abc
import logging
import argparse
from pathlib import Path
import os
import subprocess
import sys


class ArgumentCustomizer(abc.ABC):
    """ This is an abstract class that can only be subclassed to be used. """
    @abc.abstractmethod
    def add_argument(self, parser: argparse.ArgumentParser) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def create_service_argument(self, parser_args: argparse.Namespace) -> str:
        raise NotImplementedError()


class ServiceInstaller:
    def __init__(self, name, description, logging_level=logging.DEBUG):
        self._name = name
        self._description = description
        self._logging_level = logging_level
        self._arg_customizers = []

        self._init_logging()

        self._create_cli()

    def _init_logging(self):
        """ Initialization of logging module. """
        self._logger = logging.getLogger(f'DRAPI.ServiceInstaller.{self._name}')
        self._logger.setLevel(self._logging_level)

        logFormatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s : %(message)s"
        )
        logStreamer = logging.StreamHandler()
        logStreamer.setLevel(logging.DEBUG)
        logStreamer.setFormatter(logFormatter)

        self._logger.addHandler(logStreamer)

    @property
    def logger(self):
        return self._logger

    def _create_cli(self):
        """ Initialize the cli arguments """
        self._cli_parser = argparse.ArgumentParser(
            description=f"Installer script for the '{self._description}' service.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )

        installer_group = self._cli_parser.add_argument_group(
            'Service installer arguments'
        )
        installer_group.add_argument(
            "--service_log",
            "-l",
            default=Path(f'/var/log/dspace/{self._name}.log'),
            help="The path to the service log file",
            type=Path,
            metavar="FILE",
        )
        installer_group.add_argument(
            "--enable_service",
            "-e",
            help="Enable the service after installation",
            action="store_true",
        )
        installer_group.add_argument(
            "--start_service",
            "-s",
            help="Start the service after installation",
            action="store_true",
        )
        installer_group.add_argument(
            "--suffix",
            help=argparse.SUPPRESS,
        )

        self._service_group = self._cli_parser.add_argument_group(
            'Custom service arguments',
            'NOTE: Use wisely and at own risk',
        )

    def add_custom_argument(self, argument_customizer: ArgumentCustomizer):
        # argument_customizer.add_argument(self._cli_parser)
        argument_customizer.add_argument(self._service_group)
        self._arg_customizers.append(argument_customizer)

    def run(self):
        # Parse arguments first (in case we only passed -h or there was an error)
        cli_args = self._cli_parser.parse_args()

        # Check for super-user rights.
        self._check_execution_rights()

        # Start installation.
        self._logger.info(
            f"Installing the service {self._description} ({self._name})..."
        )

        self._install_service(cli_args)
        self._configure_logrotate(cli_args.service_log)

        self._logger.info(
            f"The installation of service '{self._name}' is successfully completed."
        )

    def _check_execution_rights(self):
        """ Elevate rights if missing. """
        if os.getuid() != 0:
            self._logger.warning(
                "Super user rights missing! Re-running as super user!",
            )
            sys.exit(os.system(" ".join(("sudo", "-E", sys.executable, *sys.argv))))

    def _add_eol(self, lines: list) -> list:
        """ The function helps to add EoL to each line of the given list. """
        for idx in range(len(lines)):
            lines[idx] += "\n"

        return lines

    def _execute_command(self, cmd: str, description: str) -> int:
        """
        The function helps to execute the shell command and handles the error
        at the minimum efforts.
        """
        ret = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if ret.returncode != 0:
            self._logger.error(f"{ret.stdout}")
            raise RuntimeError(f'Failed to {description} (command: {cmd})')

    def _configure_logrotate(self, logfile):
        """ Configure the logrotate to manage the log files. """
        self._logger.info("Generating the configuration for logrotate...")

        archive_num = 7
        archive_freq = "daily"
        log_archive_folder = logfile.parent / f"archived_{logfile.stem}"

        log_config_lines = [
            f"{logfile}",
            "{",
            f"{archive_freq}",
            f"rotate {archive_num}",
            f"olddir {log_archive_folder}",
            "missingok",
            "copytruncate",
            "}"
        ]

        tmp_log_config_file = Path(f"/tmp/logConfig_{self._name}")
        tmp_log_config_file.unlink(missing_ok=True)
        with tmp_log_config_file.open("w") as config_file:
            config_file.writelines(self._add_eol(log_config_lines))
        self._logger.info("The log configuration file is successfully drafted.")

        # Create the archive folder for the old log
        log_archive_folder.mkdir(parents=True, exist_ok=True)
        self._logger.info(f"The archive log folder is successfully created: {log_archive_folder}")

        # Switch the owner to configure the access rights for the archive log folder
        self._execute_command(
            f'chown -R dspace:dspace {logfile}',
            'changing the owner of the archive folder',
        )

        tmp_log_config_file.chmod(0o600)

        # Move the log configuration file to logrotate management directory
        path_to_log_config = Path(f"/etc/logrotate.d/logConfig_{self._name}")

        path_to_log_config.unlink(missing_ok=True)
        tmp_log_config_file.rename(path_to_log_config)
        self._logger.info("The log configuration file is successfully installed in the logrotate.")

    def _install_service(self, cli_args):
        enable = cli_args.enable_service
        start = cli_args.start_service

        service_filename = self._create_service_file(cli_args)

        # Update the systemd daemons
        self._execute_command(
            'systemctl daemon-reload',
            'reloading the systemd daemons',
        )
        self._logger.info('The systemd daemons are successfully reloaded.')

        # Configure the systemd
        if enable:
            self._execute_command(
                f'systemctl enable {service_filename}',
                'enabling the service',
            )
            self._logger.info(
                'The service is successfully enabled and will auto start after each system reboots.'
            )

        if start:
            self._execute_command(
                f'systemctl restart {service_filename}',
                'starting the service',
            )
            self._logger.info(
                'The service is successfully started and ready to work.'
            )

    def _create_service_file(self, cli_args):
        raise NotImplementedError()


class PythonServiceInstaller(ServiceInstaller):
    def __init__(self, *args, executable_path, **kwargs):
        super().__init__(*args, **kwargs)
        self._executable_path = Path(executable_path).resolve()
        if not self._executable_path.exists():
            raise FileNotFoundError(self._executable_path)


    def _create_service_file(self, cli_args):
        logfile = cli_args.service_log
        suffix = cli_args.suffix

        if suffix:
            self._logger.warning(
                'The debugging feature "suffix" is activated.'
            )
            service_filename = f'{self._name}_{suffix}.service'
            self._logger.warning(f'Writing to service file: {service_filename}')
            msg = '!!!!! REMEMBER to remove this debugging service !!!!!'
            self._logger.warning('!' * len(msg))
            self._logger.warning(msg)
            self._logger.warning('!' * len(msg))
            logfile = logfile.parent / f'{logfile.stem}_{suffix}{logfile.suffix}'
        else:
            service_filename = f'{self._name}.service'

        path_to_service = f'/lib/systemd/system/{service_filename}'


        # Check application's completeness
        working_directory = self._executable_path.parent
        path_to_python = sys.executable

        # Check the existence of the service log file
        if not logfile.exists():
            # To be more flexible, use shell command to create directory and
            # file to avoid permission restrictions
            self._execute_command(
                f'mkdir -p {logfile.parent}',
                'creating the folder for the service log file',
            )
            self._execute_command(
                f'touch {logfile}',
                'creating the service log file',
            )

        # Switch the owner to configure the access rights for the log file
        self._execute_command(
            f'chown dspace:dspace {logfile.parent}',
            'changing the owner of the log directory',
        )
        self._execute_command(
            f'chown dspace:dspace {logfile}',
            'changing the owner of the log file',
        )

        # Build service command.
        def arg_getter(customizer):
            return customizer.create_service_argument(cli_args)

        svc_start_cmd = ' '.join([
            path_to_python,
            str(self._executable_path),
            *[arg for arg in map(arg_getter, self._arg_customizers) if arg],
        ])

        # Create service file of systemd
        self._logger.info('Generating the service file...')

        self._logger.info(f'Working directory: {working_directory}')
        self._logger.info(f'Python installation: {path_to_python}')
        self._logger.info(f'Service log file: {logfile}')
        self._logger.info(f'Service start command: {svc_start_cmd}')

        service_lines = [
            '[Unit]',
            f'Description={self._description}',
            'After=network-online.target',
            'Wants=network-online.target',
            '',
            '[Service]',
            'Type=simple',
            'User=dspace',
            'Group=dspace',
            f'WorkingDirectory={working_directory}',
            f'ExecStart={svc_start_cmd}',
            f'StandardOutput=append:{logfile}',
            f'StandardError=append:{logfile}',
            f'SyslogIdentifier={self._name}',
            'Restart=on-failure',
            'RestartMode=normal',
            '',
            '[Install]',
            'WantedBy=multi-user.target'
        ]

        tmp_service_file = f'/tmp/{service_filename}'
        with open(tmp_service_file, 'w') as service_file:
            service_file.writelines(self._add_eol(service_lines))
        self._logger.info('The service file is successfully drafted.')

        # Move the service file to systemd directory
        self._execute_command(
            f'mv {tmp_service_file} {path_to_service}',
            'installing the service file in the systemd',
        )
        self._logger.info(
            'The service file is successfully installed in the systemd.'
        )

        return service_filename


class TimerServiceInstaller(ServiceInstaller):
    def __init__(self, *args, exec_start, frequency, **kwargs):
        super().__init__(*args, **kwargs)
        self._exec_start = exec_start
        self._frequency = frequency

    def _create_service_file(self, cli_args):
        logfile = cli_args.service_log
        suffix = cli_args.suffix

        if suffix:
            self._logger.warning(
                'The debugging feature "suffix" is activated.'
            )
            service_name = f'{self._name}_{suffix}'
            self._logger.warning(f'Writing to service file: {service_name}')
            msg = '!!!!! REMEMBER to remove this debugging service !!!!!'
            self._logger.warning('!' * len(msg))
            self._logger.warning(msg)
            self._logger.warning('!' * len(msg))
            logfile = logfile.parent / f'{logfile.stem}_{suffix}{logfile.suffix}'
        else:
            service_name = f'{self._name}'

        systemd_path = Path('/') / 'lib' / 'systemd' / 'system'
        unit_file = (systemd_path / service_name).with_suffix('.service')
        timer_file = unit_file.with_suffix('.timer')

        # Check the existence of the service log file
        if not logfile.exists():
            # To be more flexible, use shell command to create directory and
            # file to avoid permission restrictions
            self._execute_command(
                f'mkdir -p {logfile.parent}',
                'creating the folder for the service log file',
            )
            self._execute_command(
                f'touch {logfile}',
                'creating the service log file',
            )

        # Switch the owner to configure the access rights for the log file
        self._execute_command(
            f'chown dspace:dspace {logfile.parent}',
            'changing the owner of the log directory',
        )
        self._execute_command(
            f'chown dspace:dspace {logfile}',
            'changing the owner of the log file',
        )

        # Build service command.
        def arg_getter(customizer):
            return customizer.create_service_argument(cli_args)

        svc_start_cmd = ' '.join([
            self._exec_start,
            *[arg for arg in map(arg_getter, self._arg_customizers) if arg],
        ])

        unit_content = [
            '[Unit]',
            f'Description={self._description}',
            '',
            '[Service]',
            'Type=simple',
            'User=dspace',
            'Group=dspace',
            f'ExecStart=/bin/sh -c "{svc_start_cmd}"',
            f'StandardOutput=append:{logfile}',
            f'StandardError=append:{logfile}',
            f'SyslogIdentifier={self._name}',
        ]

        timer_content = [
            '[Unit]',
            f'Description=systemd timer file for {unit_file}',
            '',
            '[Timer]',
            f'OnCalendar={self._frequency}',
            'Persistent=true',
            '',
            '[Install]',
            'WantedBy=timers.target',
        ]


        # Create service file of systemd
        self._logger.info('Generating the service files...')

        self._logger.info(f'Service log file: {logfile}')
        self._logger.info(f'Service start command: {svc_start_cmd}')

        tmp_unit_file = f'/tmp/{unit_file.name}'
        with open(tmp_unit_file, 'w') as fhandle:
            fhandle.writelines(self._add_eol(unit_content))

        tmp_timer_file = f'/tmp/{timer_file.name}'
        with open(tmp_timer_file, 'w') as fhandle:
            fhandle.writelines(self._add_eol(timer_content))

        self._logger.info('The timer service files are successfully drafted.')

        # Move the service file to systemd directory
        self._execute_command(
            f'mv {tmp_unit_file} {unit_file}',
            'installing the unit file in the systemd',
        )
        self._execute_command(
            f'mv {tmp_timer_file} {timer_file}',
            'installing the timer file in the systemd',
        )
        self._logger.info(
            'The service file is successfully installed in the systemd.'
        )

        return timer_file.name
