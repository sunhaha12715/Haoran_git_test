import subprocess
import logging

logging.basicConfig(level=logging.INFO)

def log_subprocess_output(pipe, level):
    for line in iter(pipe.readline, b''): # b'\n'-separated lines
        logging.log(level, line.decode().strip())


class TcpdumpInterface:
    """
    Helper class to integrade tcpdump in a python environment.
    """
    def start_logging(self, interface, log_file):
        """
        Starts logging of interface to log_file in via tcpdump in a subprocess, so this methods returns immidietly.

        interface - the interface to be logged, e.g. eth0
        log_file - the file where tcpdump should log the traffic to
        """
        self._process = subprocess.Popen(['tcpdump', '-i', interface, '-w', log_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self._interface = interface
        self._log_file = log_file
        self._r = None
        try:
            # If the process terminates immidietly, tcpdump did not start properly
            r = self._process.wait(1)
            with self._process.stdout:
                logging.fatal(f'Logging {interface} to {log_file} failed')
                log_subprocess_output(self._process.stdout, logging.FATAL)
            return r
        except subprocess.TimeoutExpired:
            logging.info(f'Logging {interface} to {log_file} started successfully')
            return 0

    def stop_logging(self):
        """
        Stops logging
        """
        self._process.terminate()
        r = self._process.wait()
        with self._process.stdout:
            logging.info(f'Logging {self._interface} to {self._log_file} finished')
            log_subprocess_output(self._process.stdout, logging.INFO)
        return r


if __name__ == '__main__':
    import sys
    import time

    if len(sys.argv) != 4:
        print('Usage: python tcpdump_interface.py <interface> <full_path.pcap> <duration>')
        sys.exit(-1)

    tcpdump_interface = TcpdumpInterface()
    r = tcpdump_interface.start_logging(sys.argv[1], sys.argv[2])
    if r != 0:
        sys.exit(r)

    time.sleep(float(sys.argv[3]))
    tcpdump_interface.stop_logging()