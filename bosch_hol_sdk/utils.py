'''
Random utilities and helper functions.

@copyright
    Copyright 2023, dSPACE Mechatronic Control Technology (Shanghai) Co., Ltd.
    All rights reserved.
'''
import logging
import pathlib
import shlex
import subprocess

import paramiko


def ping(ip, logger=None):
    if logger is None:
        root_logger = logging.getLogger()
    else:
        root_logger = logger
    logger = root_logger.getChild(f"ping_{ip}")

    try:
        subprocess.check_output(['ping', '-c', '1', ip], text=True)
    except subprocess.CalledProcessError as exc:
        # Attempt to extract the failure reason if it exists.
        reason = ""
        if exc.stdout:
            for line in exc.stdout.splitlines():
                if line.startswith('From'):
                    try:
                        *_, reason = line.split(" ", 3)
                    except IndexError:
                        continue
                    else:
                        reason = f': {reason}'
                        break
        logger.info(f"Ping statistics for {ip}: Lost=100%{reason}")
        return False
    else:
        logger.info(f"Ping statistics for {ip}: Lost=0%")
        return True


def run_file_remotely(
    file,
    *args,
    remote_ip,
    username,
    password,
    sudo_password=None,
    paramiko_log_level=logging.INFO,
) -> tuple[str, str]:
    """ Copies a file to a remote host, executes it and then deletes it. """
    file = pathlib.Path(file)

    # Limit paramiko's logs.
    logging.getLogger('paramiko').setLevel(paramiko_log_level)

    with paramiko.SSHClient() as ssh:
        # Open connect to remote.
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=remote_ip,
            username=username,
            password=password,
        )

        remote_path = f'/tmp/{file.name}'
        # Copy the file to the remote host and make it executable.
        sftp = ssh.open_sftp()
        sftp.put(file, remote_path)
        sftp.chmod(remote_path, 0o755)

        # Make sure we have unix line endings (dos2unix is not installed).
        _, _, stderr = ssh.exec_command(f"sed -i 's/\\r$//' {remote_path}")
        exit_status = stderr.channel.recv_exit_status()
        if exit_status != 0:
            error = stderr.read().decode('utf-8')
            raise RuntimeError(f'Failed to run sed: {error}')

        # Run the file.
        cmd = [remote_path, *args]
        if sudo_password is None:
            _, stdout, stderr = ssh.exec_command(shlex.join(cmd))
            outputs = (
                stdout.read().decode('utf-8'),
                stderr.read().decode('utf-8'),
            )
        else:
            cmd_str = f'echo {sudo_password} | sudo --stdin {shlex.join(cmd)}'
            _, stdout, stderr = ssh.exec_command(cmd_str)
            stderr.read().decode('utf-8')
            if stderr.startswith('[sudo]'):
                if 'incorrect password attemp' in stderr:
                    raise ValueError('Wrong sudo password')
                else:
                    _, stderr = stderr.split(':', 1)
            outputs = (
                stdout.read().decode('utf-8'),
                stderr,
            )

        # Delete the file.
        ssh.exec_command(f'rm -f {remote_path}')

        return outputs
