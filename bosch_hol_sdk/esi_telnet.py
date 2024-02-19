#!/usr/bin/env python3.9
import datetime
import logging
from pathlib import Path
import telnetlib


class TelnetLoginError(Exception):
    def __init__(self, host, username, password):
        self._msg = (
            f"Login to {host} with username {username} and password {password} failed!"
        )

    def __str__(self):
        return self._msg


class Telnet(telnetlib.Telnet):
    def __init__(self, host, *args, **kwargs):
        super().__init__(host, *args, **kwargs)
        self._logger = logging.getLogger(f"Telnet_{host}")
        self._host = host

    def login(self, username, password):
        logging.debug(f"Logging in to {self._host}")
        self.read_until(b"login: ", 10)
        self.write(self._make_input(username))
        self.read_until(b"Password: ", 10)
        self.write(self._make_input(password))
        idx, _, text = self.expect([b"# ", b"$ "], 10)
        if idx < 0:
            raise TelnetLoginError(self._host, username, password)
        self._prompt_msg = text.splitlines()[-1]
        logging.debug(f"Detected prompt message: {self._prompt_msg}")

    def _make_input(self, text):
        return f"{text}\n".encode('ascii')

    def execute_command(self, command, timeout=10):
        logging.info(f"Executing command: {command}")
        output = self._exec(command, timeout)
        return_code = int(self._exec("echo $?", timeout))
        logging.info(f"Command '{command}' returned {return_code}")
        return output, return_code

    def _exec(self, command, timeout):
        if not isinstance(command, (str, bytes)):
            command = " ".join(command)
        self.write(self._make_input(command))
        output = self.read_until(self._prompt_msg, timeout).decode('ascii')
        # Return the output without the command or the prompt after.
        return output[len(command):-len(self._prompt_msg)]


def get_esi_stats(tn, dst):
    commands = [
        "uptime",
        "ps",
        "ip link",
        "ip addr",
        "ip route",
        "netstat -a",
        "netstat -r",
    ]
    dst = Path(dst).resolve()
    dst.mkdir(parents=True, exist_ok=True)
    for command in commands:
        file = (dst / command.replace(" ", "_")).with_suffix(".log")
        with file.open("w") as file_handle:
            output, ret_code = tn.execute_command(command)
            file_handle.write(
                f"Command: {command}\n\nReturn code: {ret_code}\n\n"
            )
            if output.strip():
                file_handle.write(f"Console output:\n{output}\n")
            else:
                file_handle.write(f"(No consolte output)\n")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "esi_ip",
        metavar="ESI-IP",
        nargs="+",
        type=str,
        help="ip address of the ESI unit.",
    )
    args = parser.parse_args()

    for esi in set(args.esi_ip):
        dest = Path() / f"{datetime.datetime.now().isoformat()}_esi_{esi}"
        with Telnet(esi, 23) as tn:
            tn.login("root", "100%esiu")
            get_esi_stats(tn, dest)


if __name__ == "__main__":
    main()
