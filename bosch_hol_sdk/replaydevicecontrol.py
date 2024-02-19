"""
An convinience interface for getting instances of the power control object for
the differenct replay devices.

@copyright
    Copyright 2023, dSPACE Mechatronic Control Technology (Shanghai) Co., Ltd.
    All rights reserved.
"""
import enum
import typing

from dspace.bosch_hol_sdk.devicecontrol import (
    ScalexioDeviceControl, EsiDeviceControl, EcuDeviceControl,
    AggregateEsiDeviceControls,
)
from dspace.bosch_hol_sdk.netio_api_json_via_http import (
    netio_powerdin_4pz_ctrl, NetioSocket,
)


class ReplayDevice(str, enum.Enum):
    ECU = 'ECU'
    SCALEXIO1 = 'SCALEXIO1'
    SCALEXIO2 = 'SCALEXIO2'
    ESI = 'ESI'


NETIO_CFG_50 = {
    'ip': '192.168.140.50',
    'port': '80',
    'uname': 'netio',
    'pwd': 'netio',
}

NETIO_CFG_51 = {
    'ip': '192.168.140.51',
    'port': '80',
    'uname': 'netio',
    'pwd': 'netio',
}

_netio_dev_50 = netio_powerdin_4pz_ctrl(**NETIO_CFG_50)
_netio_dev_51 = netio_powerdin_4pz_ctrl(**NETIO_CFG_51)

_replay_devices = {
    ReplayDevice.ECU: EcuDeviceControl(
        name=ReplayDevice.ECU,
        ip='',
        netio_socket=NetioSocket(_netio_dev_50, 4),
        KL30=NetioSocket(_netio_dev_50, 3),
    ),
    ReplayDevice.SCALEXIO1: ScalexioDeviceControl(
        name=ReplayDevice.SCALEXIO1,
        ip='192.168.140.10',
    ),
    ReplayDevice.SCALEXIO2: ScalexioDeviceControl(
        name=ReplayDevice.SCALEXIO2,
        ip='192.168.140.11',
    ),
    ReplayDevice.ESI: AggregateEsiDeviceControls(
        name=ReplayDevice.ESI,
        devices=[
            EsiDeviceControl(
                name='ESI1',
                ip='192.168.141.21',
                port=30303,
                netio_socket=NetioSocket(_netio_dev_51, 4),
            ),
            EsiDeviceControl(
                name='ESI2',
                ip='192.168.141.22',
                port=30303,
                netio_socket=NetioSocket(_netio_dev_51, 4),
            ),
        ],
    ),
}


def get_replay_device(device: typing.Union[ReplayDevice, str]):
    device = ReplayDevice[device.upper()]
    return _replay_devices[device]
