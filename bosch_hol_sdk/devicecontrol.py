"""
Abstraction for different devices, used mostly to reboot them.

@copyright
    Copyright 2023, dSPACE Mechatronic Control Technology (Shanghai) Co., Ltd.
    All rights reserved.
"""
import dataclasses
import logging
import time

from dspace.bosch_hol_sdk.DataReplayAPI.drapi.helper.scalexio_control import (
    ScalexioControl,
)
from dspace.bosch_hol_sdk.netio_api_json_via_http import NetioSocket
from dspace.bosch_hol_sdk.netio_api_json_via_http import NetioState  # noqa:F401
from dspace.bosch_hol_sdk.simple_esi_check import esi_xcp_if
from dspace.bosch_hol_sdk.utils import ping


class BaseDataClass:
    """
    This class serves as a dummy base for other dataclasses to avoid trouble
    with propagating __post_init__ calls to the parents.
    """
    def __post_init__(self):
        pass


@dataclasses.dataclass
class DeviceControlInterface(BaseDataClass):
    name: str

    def wait_till_online(self, timeout: float = 60):
        raise NotImplementedError(
            f'{self.__class__.__name__}.wait_till_online'
        )

    def reboot(self, delay: float = 3):
        raise NotImplementedError(f'{self.__class__.__name__}.reboot')

    def is_ready(self):
        raise NotImplementedError(f'{self.__class__.__name__}.is_ready')

    @property
    def _logger(self):
        return logging.getLogger(f'DeviceControl.{self.name}')

    def __str__(self):
        return f'<{self.__class__.__name__} {self.name}>'


@dataclasses.dataclass
class BasicDeviceControl(DeviceControlInterface):
    """
    Abstraction of a basic device that has at least a name and ip address.
    """
    ip: str

    def wait_till_online(self, timeout: float = 60):
        self._logger.info(
            f'Waiting for device to be reachable via ping (timeout={timeout}).'
        )
        # Calculate the timeout.
        timeout = time.perf_counter_ns() + timeout * 1000000000
        while timeout > time.perf_counter_ns():
            if self.ping():
                break
            time.sleep(1)
        else:
            raise TimeoutError(self)

    def ping(self):
        return ping(self.ip, logger=self._logger)

    def is_ready(self):
        return self.ping()

    def __str__(self):
        return f'<{self.__class__.__name__} {self.name} ({self.ip})>'

    def __hash__(self):
        return hash((self.name, self.ip))


# NOTE: The order of inheritance in the following class should not be changed!
@dataclasses.dataclass
class ScalexioDeviceControl(ScalexioControl, BasicDeviceControl):
    """ Abstraction for a Scalexio device with the webapi functionalities. """

    def __post_init__(self):
        super().__post_init__()
        ScalexioControl.__init__(self, self.ip)


@dataclasses.dataclass
class NetioDeviceControl(BasicDeviceControl):
    """ Abstraction for a generic device connected to a netio power socket. """
    netio_socket: NetioSocket

    def turn_on(self):
        self._logger.info('Turning on power socket.')
        return self.netio_socket.turn_on()

    def turn_off(self):
        self._logger.info('Turning off power socket.')
        return self.netio_socket.turn_off()

    def reboot(self, delay: float = 3):
        self._logger.info(f'Rebooting device with a delay of {delay} seconds.')
        self.turn_off()
        time.sleep(delay)
        self.turn_on()

    @property
    def power_state(self):
        return self.netio_socket.state


@dataclasses.dataclass
class EcuDeviceControl(NetioDeviceControl):
    KL30: NetioSocket

    def __post_init__(self):
        super().__post_init__()
        self.KL15 = self.netio_socket


@dataclasses.dataclass
class EsiDeviceControl(NetioDeviceControl):
    """ Abstraction an ESI device (special handling when waiting for it). """
    port: int = None

    def __post_init__(self):
        super().__post_init__()
        self._xcp_if = esi_xcp_if(self.ip, self.port)

    # TODO include scaleio loading application!

    def wait_till_online(self, timeout: float = 300):
        # Calculate the timeout.
        end_of_times = time.perf_counter_ns() + timeout * 1000000000

        # Let's first wait for it to reachable by ping.
        super().wait_till_online(timeout)

        self._logger.info('Starting XCP connection test.')
        while end_of_times > time.perf_counter_ns():
            if self.test_xcp_connection():
                break
            # We wait 10 seconds between the retries since this process
            # can take quite some time and we don't want to flood the logs.
            time.sleep(10)
        else:
            raise TimeoutError(self)
        self._logger.info('Device ready.')

    def test_xcp_connection(self):
        with self._xcp_if as handle:
            if handle is not None and handle.xcp_connect():
                return True
            else:
                return False

    def is_ready(self):
        return self.test_xcp_connection()


@dataclasses.dataclass
class AggregateEsiDeviceControls(DeviceControlInterface):
    devices: list[EsiDeviceControl]

    def turn_on(self):
        # The ESI units share one power control socket so we can turn only one
        # of them of.
        self.devices[0].turn_on()

    def turn_off(self):
        # The ESI units share one power control socket so we can turn only one
        # of them off.
        self.devices[0].turn_off()

    def reboot(self, delay: float = 3):
        # The ESI units share one power control socket so we can reboot only
        # one of them.
        self._logger.info(f'Rebooting devices {self.devices}')
        self.devices[0].reboot(delay)

    def wait_till_online(self, timeout: float = 300):
        start_time = time.perf_counter()
        try:
            for device in self.devices:
                remaing_timeout = timeout - (time.perf_counter() - start_time)
                self._logger.info(
                    f'Waiting for {device} to be ready. '
                    f'Remaining timeout: {remaing_timeout:0.3f} seconds'
                )
                device.wait_till_online(remaing_timeout)
        except TimeoutError:
            self._logger.warning(
                'Timeout occured. Checking all device statuses...'
            )
            for device in self.devices:
                ping_status = 'OK' if device.ping() else 'FAIL'
                xcp_status = 'OK' if device.test_xcp_connection() else 'FAIL'
                self._logger.info(
                    f'{device}: ping={ping_status}; XCP={xcp_status}'
                )
            raise

    def is_ready(self):
        return all(map(lambda esi: esi.is_ready(), self.devices))

    @property
    def power_state(self):
        return self.devices[0].power_state

    def __str__(self):
        devices = '|'.join(map(str, self.devices))
        return f'<{self.__class__.__name__} {self.name} [{devices}]>'
