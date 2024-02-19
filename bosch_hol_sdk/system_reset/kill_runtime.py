'''
Descripttion: 
Author: Juan Li
Email: juli@dspace.com
version: 
Date: 2023-08-07 13:40:53
LastEditors: Juan Li
LastEditTime: 2023-08-08 17:16:41
'''

import logging
import paramiko
import shlex
import subprocess
import time


def local_exec(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    stdout, stderr = proc.communicate()
    return stdout.decode("utf-8"), stderr.decode("utf-8")


def kill_runtimes(remote_credentials=None):
    if remote_credentials:
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(**remote_credentials)

            def remote_exec(cmd):
                _, stdout, stderr = ssh.exec_command(shlex.join(cmd))
                return stdout.read().decode('utf-8'), stderr.read().decode('utf-8')

            remote_result = kill_runtime_internal(remote_exec)
    local_result = kill_runtime_internal(local_exec)
    return local_result and remote_result


def get_number_of_processes(name, execute_fcn):
    stdout, _ = execute_fcn(["pgrep", "-c", name])
    return int(stdout)


def kill_runtime_internal(execute_fcn):
    proc_name = "rtmaps_runtime"
    logging.info(f"Killing all instances of {proc_name}")

    # Check process count.
    count = get_number_of_processes(proc_name, execute_fcn)
    logging.info(f"Found {count} processes.")
    if count == 0:
        return True

    # Gracefully terminate the process.
    execute_fcn(["pkill", proc_name])
    # Wait 2 seconds for the process to terminate gracefully.
    time.sleep(2)
    # Check process count.
    count = get_number_of_processes(proc_name, execute_fcn)
    logging.info(f"After terminating, {count} processes alive")
    if count == 0:
        return True

    # Forcefully kill the process.
    execute_fcn(["pkill", "-9", proc_name])
    # Wait 2 seconds for the process to die.
    time.sleep(2)
    # Check process count.
    count = get_number_of_processes(proc_name, execute_fcn)
    logging.info(f"After killing, {count} processes alive")
    if count == 0:
        return True

    return False


def pc_ssh_restart(host: str, username: str, passwd: str):
    try:
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, port=22, username=username, password=passwd)

            def remote_exec(cmd):
                _, stdout, stderr = ssh.exec_command(shlex.join(cmd))
                return stdout.read().decode('utf-8'), stderr.read().decode('utf-8')

            kill_result = kill_runtime_internal(remote_exec)
            if not kill_result:
                logging.warning(
                    'Failed to terminate all remote RTMaps runtimes.'
                )

            stdin, stdout, stderr = ssh.exec_command(
                f'echo {passwd} | sudo -S systemctl restart pyro_server_replay_api.service'
            )
            # Make sure the process finishes execution first.
            _ = stdout.read()

            # Give the serivce a chance to initialize
            time.sleep(0.5)

            stdin, stdout, stderr = ssh.exec_command(
                'systemctl status pyro_server_replay_api.service'
            )
            status_output = stdout.read().decode('utf-8')
            if 'active (running)' not in status_output:
                logging.error('Restarting the pyro-server remoted failed.')
                for line in status_output.splitlines():
                    logging.error(line)
                return False

            return kill_result
    except paramiko.AuthenticationException:
        logging.exception("Authentication failed. Please check your credentials.")
    except paramiko.SSHException as ssh_ex:
        logging.exception(f"SSH connection error: {ssh_ex}")
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
    return False


def restart_replay_api_service():
    kill_result = kill_runtime_internal(local_exec)
    if not kill_result:
        logging.warning('Failed to terminate all local RTMaps runtimes.')

    result = subprocess.run(
        [
            "sudo",
            "-S",
            "systemctl",
            "restart",
            "replay_api_server.service"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        input="dspace",
    )

    # Give the serivce a chance to initialize
    time.sleep(1)

    result = subprocess.run(
        [
            "systemctl",
            "status",
            "replay_api_server.service",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    output = result.stdout
    if "active (running)" not in output:
        logging.error('Restarting the replay-api service failed.')
        for line in output.splitlines():
            logging.error(line)
        return False

    return kill_result


if __name__ == "__main__":
    credentials = {
        "hostname": "192.168.140.60",
        "username": "usename",
        "password": "passwd",
    }

    if kill_runtimes(credentials):
        print("Process closed successfully.")
    else:
        print("No process can be closed.")
