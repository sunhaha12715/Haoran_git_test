"""
The script helps to check whether ESI is ready by means of checking the XCP
interface status

Requirements: Python3, ControlDesk

by XiangL, DSC, 2023/7/10

Change log:

rev_0_1:
- The first version

Usage:
    see Main
"""

# Dependencies
import logging
import socket


# Global varaibles
ESI_1 = {
    "IP": "192.168.141.21",
    "Port": 30303
}

ESI_2 = {
    "IP": "192.168.141.22",
    "Port": 30303
}

SUCCESS = 1
FAIL = 0

DEFAULT_BUF_SIZE = 256


# Classes
class esi_xcp_if:
    def __init__(self, ip, port, logger=None) -> None:
        self.reset_socket()
        self.server = (ip, port)
        if logger is None:
            # Just get the root logger in that case.
            logger = logging.getLogger()
        log_ip = ip.replace(".", "_")
        cls_name = self.__class__.__name__
        self._logger = logger.getChild(f"{cls_name}.{log_ip}")

    def __enter__(self):
        self.socket_connect()
        if self.socket_available:
            return self
        else:
            return None

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def reset_socket(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(2)
        self.socket_available = False

    def socket_connect(self) -> None:
        # Initialization of the socket
        if not self.socket_available:
            try:
                self.socket.connect(self.server)
            except Exception as exc:
                self._logger.warning(
                    f'Failed to connect to the ESI XCP server: {exc}'
                )
            else:
                self.socket_available = True
        else:
            self._logger.info('The ESI XCP port is already connected.')

    def close(self) -> None:
        # Close the socket
        if self.socket_available:
            self.socket.close()
            # The purpose is to be able to re-connect for the next run: it
            # seems a closed socket cannot be re-connect directly again.
            self.reset_socket()

    def send_cmd(self, cmd_bytes, msg) -> int:
        # Send XCP command
        if not self.socket_available:
            self._logger.error("Cannot send the command to ESI XCP interface.")
            return FAIL
        try:
            self.socket.send(bytes(cmd_bytes))
        except Exception as exc:
            self._logger.error(f'Failed to send XCP data: {exc}')
            return FAIL
        else:
            self._logger.debug(f"Successfully sent message: '{msg}'")
            return SUCCESS

    def get_resp(self, buf_size) -> bytes:
        # Receive the response from XCP slave
        resp = self.socket.recv(buf_size)
        return resp

    def xcp_connect(self) -> int:
        # Connect the XCP interface
        cmd_bytes = [0x02, 0x00, 0x01, 0x00, 0xFF, 0x00]
        msg = "STD: CONNECT"
        success = self.send_cmd(cmd_bytes, msg)
        if success:
            try:
                ret = self.get_resp(DEFAULT_BUF_SIZE)
            except Exception as exc:
                self._logger.error(f'Failed to read XCP response: {exc}')
                return FAIL

            if len(ret) == 12 and ret[4] == 0xFF:
                return SUCCESS
            else:
                return FAIL
        else:
            return FAIL


# Main
if __name__ == "__main__":
    # Instantiate the ESI XCP
    esi_xcp_1 = esi_xcp_if(ESI_1["IP"], ESI_1["Port"])
    esi_xcp_2 = esi_xcp_if(ESI_2["IP"], ESI_2["Port"])

    # Connect to the ESIs' XCP ports
    esi_xcp_1.socket_connect()
    esi_xcp_2.socket_connect()

    if esi_xcp_1.socket_available:
        # Execute XCP connect
        ret = esi_xcp_1.xcp_connect()
        if ret:
            print("ESI1's XCP interface is connected.")
        else:
            print("ESI1's XCP interface cannot be connected.")

    if esi_xcp_2.socket_available:
        # Execute XCP connect
        ret = esi_xcp_2.xcp_connect()
        if ret:
            print("ESI2's XCP interface is connected.")
        else:
            print("ESI2's XCP interface cannot be connected.")

    # Close the socket
    esi_xcp_1.close()
    esi_xcp_2.close()
