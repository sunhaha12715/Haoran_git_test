"""
The script configure the rtmaps diagram and control desk model for data replay
Requirements: Python3, Replay_pc1
by QianweiY, DSC, 2023/8/14

v2.0:
-The second demo version
- Ubuntu 20.04
-Test data:
    copy7-pc1:"/mnt/ext_ssd/auto/20230809_155456_DSU1_auto@20230809_155456973558/DSU1_auto_20230809_155456@20230809_155456973558.rec"
    copy7-pc2:"/mnt/ext_ssd/auto/20230809_155456_DSU0_auto@20230809_155456809596/DSU0_auto_20230809_155456@20230809_155456809596.rec"
-Test rtmaps diagram:
    copy7-pc1: "/home/dspace/workspace/replay_test/Complete_DSU1_Chery_API.rtd"
    copy7-pc2: "/home/dspace/workspace/replay_test/Complete_DSU0_Chery_API.rtd"
-Main package version:
    esi-unit-encoder-v2.pck(2.0)
    fillterCAN.pck(1.0.0)
    rtmaps_custom_data.pck(1.11.0)
    CANContainer.pck(0.999)
    DDRP.pck(2.0)
    EthernetContainer.pck(2.1.0)
    rtmaps_get_timestamp.pck(1.0)
    rtmaps_pcap.pck (3.0.0)
-Main component version:
    Image2ESI 2.0.12
    MAPS_ToStream8 1.1.1
    dDRP_Offset_Sender 1.0
    dDrp_Frame_Encoder 1.1
    filter_CAN_1.0.1
    get_timestamp 1.0
    pcap_filter 2.0.0
"""
import dataclasses
import enum
from pathlib import Path
import time
from datetime import datetime
import logging
import subprocess
import json
import re
import shutil
import tarfile
import typing

# "third-party" modules.
from ASAM.XIL.Interfaces.Testbench.Common.Error.TestbenchPortException import TestbenchPortException
import Pyro5.api

from dspace.bosch_hol_sdk import get_version_tuple
from dspace.bosch_hol_sdk import __version__ as full_version_str
from dspace.bosch_hol_sdk.bus_manipulation import (
    DataManipulation, reload_xil_paths, XilManipulationBase,
)
from dspace.bosch_hol_sdk.configuration import (
    deserialize_data_manipulation, check_data_manipulation_objects,
    ReplayTimeConfiguration,
)
from dspace.bosch_hol_sdk.DataReplayAPI.drapi.helper.xilapi_maport import XILAPIMAPort
from dspace.bosch_hol_sdk.DataReplayAPI.drapi import datareplay_pb2
from dspace.bosch_hol_sdk.dsmessagereader import (
    DsMessagesReader, DsMessageType,
)
from dspace.bosch_hol_sdk.esi_ftp_client import esi_ftp_client
from dspace.bosch_hol_sdk import esi_telnet
from dspace.bosch_hol_sdk import idx_rec_compare
from dspace.bosch_hol_sdk.netio_api_json_via_http import NetioState
from dspace.bosch_hol_sdk.port_connection_config import (
    PortConnectionManager, PlayerLocation,
)
from dspace.bosch_hol_sdk.prepare_connection import RtmapsConnection
from dspace.bosch_hol_sdk.replay_plugin_exceptions import (
    RTAppDownloadError, ESIRestartTimeoutError, XilApiError, RTMapsError,
    ECURestartedError, ReplayFrozenError, RTAppUnloadError,
    BadManipulationConfigurationError, IncompatibleRTAppsError,
    IncompatibleRTAppApiError, ESIError, RealtimeApplicationError,
)
from dspace.bosch_hol_sdk.replaydevicecontrol import (
    get_replay_device, ReplayDevice,
)
from dspace.bosch_hol_sdk.system_reset import kill_runtime
from dspace.bosch_hol_sdk.utils import run_file_remotely


# Port on which the Pyro API is listening
PYRO_PORT = 33033
# IP of the secondary server
REMOTE_IP = "192.168.140.102"  # remote ip of device that slave diagram in
# RTMaps Port (can be choosen freely), used for the "master/slave" mechanism in RTMaps
RTMAPS_PORT = 11118  # rtmaps should be standalone, port can not be the same port in rtmaps

ESI_CHECK_INTERVAL = 5  # seconds.
PROGRESS_PRINT_INTERVAL = 1  # seconds.


cwd = Path(__file__).resolve().parent

# *************xil api variable*******************
with (cwd / "xil_variable.json").open() as file:
    xil_variable = json.load(file)
# ************************************************

DEFAULT_REPLAY_DATA_TIMEOUT = 20  # seconds
COUNTER_SUM_PROP_NAME = "Sum_Count"

SOMEIP_ROOT_PATTERN = (
    r'.*'  # Platform
    r'BusSystems/'
    r'Ethernet/'
    r'(?P<ECU>[^/]+)/'
    r'[CP]SIs/'
    r'ROOT_PACKAGE/'
    r'Service_(?P<ServiceID>[^/]+)_1/'
)

SOMEIP_SUBSCRIPTION_STATUS_PATTERN = SOMEIP_ROOT_PATTERN + (
    r'.*/'  # We ignore some parts of the path.
    r'Event_Group_(?P<GroupId>\d+)/'
    r'ADCC/'
    r'Subscription Status'
)

SOMEIP_COUNTER_PATTERN = SOMEIP_ROOT_PATTERN + (
    r'(?P<Type>Methods|Events)/'  # Matche both but we want to know which.
    r'(?P<Name>[^/]+)/'  # Method or event name
    r'(?:Response_RX/)?'  # Only exists in the case of Methods
    r'Status/'
    r'Data\s(?P<Counter>\S+\sCounter)'  # Both transmitted and received.
)

SOMEIP_SUBSCRIPTION_REGEX = re.compile(SOMEIP_SUBSCRIPTION_STATUS_PATTERN)
SOMEIP_COUNTER_REGEX = re.compile(SOMEIP_COUNTER_PATTERN)


def get_instance(*args, **kwargs):
    return DemoReplayPlugin(*args, **kwargs)


@dataclasses.dataclass
class CounterData:
    name: str
    # NOTE: Bad type-hinting but currently no generic solution to import both
    # RTMaps objects for type hinting only.
    rtmaps: object
    component_id: str
    last_count: int = 0
    timestamp: float = 0


class RTMapsLogError(enum.Enum):
    NoError = enum.auto()
    GenericError = enum.auto()
    Image2EsiError = enum.auto()
    RTMapsZombie = enum.auto()


@dataclasses.dataclass(frozen=True)
class RTMapsErrorHandler(object):
    regex: typing.Union[str, re.Pattern]
    error_type: RTMapsLogError

    def __post_init__(self):
        if isinstance(self.regex, str):
            object.__setattr__(self, 'regex', re.compile(f'.*{self.regex}.*'))

    def __eq__(self, other):
        if not isinstance(other, str):
            raise TypeError(type(other))
        return self.regex.match(other) is not None


class RTMapsErrorHandlersList(list):
    def __getitem__(self, key):
        for handler in self:
            if key == handler:
                return handler
        else:
            raise IndexError(key)


class DemoReplayPlugin():
    PYRO_ADDRESS = (REMOTE_IP, PYRO_PORT)

    def __init__(self, rtmaps, logger):
        self._rtmaps = rtmaps
        self._logger = logger
        self._user_data = {}
        self._pending_exception = None
        self._set_print = 0
        self._counters = []
        self._replay_data_timeout = DEFAULT_REPLAY_DATA_TIMEOUT
        self._remote_rtmaps = None
        self._rt_app_download = False
        self._restart_service_pending = False
        self._last_esi_check = 0
        self._last_progress_print = 0
        self._esi_restart_count = 0
        self._someip_monitor_list = []
        self._ecu = None
        self._esi = None
        self._sclx = None
        self._sclx2 = None
        self._sclx2_reader = None
        self._rtmaps_error_handlers = RTMapsErrorHandlersList([
            # The order implies precedence!
            RTMapsErrorHandler(
                regex=r'component.*is still alive',
                error_type=RTMapsLogError.RTMapsZombie,
            ),
            RTMapsErrorHandler(
                regex=r"component Image2Esi",
                error_type=RTMapsLogError.Image2EsiError,
            ),
        ])

        # XIL
        self._xil_api_maport = None
        self._setTrue = True
        self._start_offset = float(15)   # change Start offset
        self._stop_offset = float(3600)  # change stop offset

        self._logger.info(
            f'Creating replay-plugin instance with version {full_version_str}'
        )

        self._debug = False
        # NETIO configuration
        self.netio_init()

    def _connect_to_remote_rtmaps(self):
        ip, port = self.PYRO_ADDRESS
        uri = f"PYRO:RTMapsService@{ip}:{port}"
        self._logger.info(f"Connecting to remote RTMaps at {uri}")
        self._remote_rtmaps = Pyro5.api.Proxy(uri)
        # Clear any old logs.
        self._remote_rtmaps.clear_log()

    def _execute_command(self, cmd: list, **kwargs: typing.Any) -> bool:
        err_msgs = False
        result = subprocess.run(cmd, text=True, capture_output=True, **kwargs)

        for line in result.stderr.splitlines():
            err_msgs = True
            self._logger.error(line)

        for line in result.stdout.splitlines():
            self._logger.info(line)

        self._logger.info(
            f"Command '{cmd}' finished with code {result.returncode}"
        )

        # With error -> return False,  no error -> return True
        return result.returncode == 0 and not err_msgs

    def download_SCLX_APP(self, sdf_path: str, CMDLOADER: str, platform: str, on_error: str = "unload"):
        # Register platform and download application in one step.
        self._logger.info(f'Loading the real-time application: {sdf_path}')
        cmd1 = [
            CMDLOADER,
            '-ra'
        ]

        cmd2 = [
            CMDLOADER,
            '-p', platform,
            sdf_path
        ]

        if not self._execute_command(cmd1) or not self._execute_command(cmd2):
            if on_error == "unload":
                self._logger.info("Failed to load application.")
                self._logger.info(
                    "Forcifully unloading previous applications and trying again."
                )
                self._sclx.unload()
                self._sclx2.unload()
                # Try again.
                return self.download_SCLX_APP(sdf_path, CMDLOADER, platform, on_error="reboot")
            elif on_error == "reboot":
                self._logger.info(
                    "Failed to load application. Rebooting HIL and trying again."
                )
                self._sclx.reboot()
                self._sclx2.reboot()
                # Sleep 10 seconds first. There is no point of starting pinging
                # right away and it might even repsond if it's pinged too soon.
                time.sleep(10)
                self._sclx.wait_till_online(60)
                self._sclx2.wait_till_online(60)
                # Now try one last time.
                return self.download_SCLX_APP(sdf_path, CMDLOADER, platform, on_error="fail")
            else:
                self._logger.error('Downloading application failed!')
                raise RTAppDownloadError()

        self._rt_app_download = True
        self._logger.info('Downloading application finished successfully')

    def unload_SCLX_APP(self, CMDLOADER: str, platform: str):
        # control desk application unload
        self._logger.info('Unloading the real-time application.')
        ret = self._execute_command(
            [CMDLOADER, '-unload', '-p', platform, '-ol', '2']
        )
        if not ret:
            self._logger.error('Unloading application failed!')
            raise RTAppUnloadError()

        self._logger.info('Unloading application finished successfully')

    def _ensure_version_compatibility(self, maport):
        # Read versions
        api_version = get_version_tuple()
        rtapp_versions = self._get_rt_app_versions(maport)

        # Report versions
        self._logger.info(f'Detected Replay API version: {api_version}')
        for rtpc, version in enumerate(rtapp_versions, 1):
            self._logger.info(
                f'Detected RT App version on RTPC{rtpc}: {version}'
            )

        # Check compatibilities
        if rtapp_versions[0][:2] != rtapp_versions[1][:2]:
            raise IncompatibleRTAppsError(
                rtapp_versions[0],
                rtapp_versions[1],
            )

        for rtpc, version in enumerate(rtapp_versions, 1):
            if api_version[:2] != version[:2]:
                raise IncompatibleRTAppApiError(
                    api_version=api_version,
                    rtapp_version=version,
                    rtpc=rtpc,
                )

    def _get_rt_app_version_paths(self):
        pc_roots = [
            'SCALEXIO Real-Time PC()://Replay_MP_Chery_BM',
            'SCALEXIO Real-Time PC_2()://Replay_MP_SOMEIP',
        ]
        return (f'{root}/Model Root/ModelVersion/Out1' for root in pc_roots)

    def _get_rt_app_versions(self, maport):
        versions = []
        for i, path in enumerate(self._get_rt_app_version_paths(), 1):
            try:
                version = maport.read_variable(path)
                version = tuple(version[:3])
            except Exception:
                self._logger.exception(
                    f'Failed to read the RT App version of PC{i}'
                )
                version = (0, 0, 0)
            finally:
                versions.append(version)

        return versions

    def netio_init(self) -> None:
        self._ecu = get_replay_device(ReplayDevice.ECU)
        self._esi = get_replay_device(ReplayDevice.ESI)
        self._sclx = get_replay_device(ReplayDevice.SCALEXIO1)
        self._sclx2 = get_replay_device(ReplayDevice.SCALEXIO2)
        self._sclx2_reader = DsMessagesReader(self._sclx2, logger=self._logger)

    def ecu_on(self) -> None:
        self._logger.info("Turning ECU on.")
        self._ecu.netio_socket.turn_on()
        self._logger.info("Waiting 10s for ECU to be ON.")
        time.sleep(10)

    def ecu_off(self) -> None:
        self._logger.info("Turning ECU off.")
        self._ecu.netio_socket.turn_off()

    def ESI_check(self, reboot_on_error=True) -> None:
        self._logger.info('Checking connection to the ESI units.')
        if not self._esi.is_ready():
            self._logger.warning('ESI units not ready!')
            if reboot_on_error:
                self._reset_esis()

    def SCLX_model_config_StartStop(self) -> None:
        ReplayModeEnabled = self._xil_api_maport.read_variable(xil_variable[0]['EnableReplayMode'])
        self._logger.info(f'ReplayModeEnabled: {ReplayModeEnabled}')
        if ReplayModeEnabled == 0.0:
            self._xil_api_maport.write_variable(xil_variable[0]['EnableReplayMode'], float(1))

        Enable_StartStopSetFromSCLX = self._xil_api_maport.read_variable(xil_variable[0]['Enable_StartStopSetFromSCLX'])
        self._logger.info(f'Enable_StartStopSetFromSCLX: {Enable_StartStopSetFromSCLX}')
        if Enable_StartStopSetFromSCLX is False:
            self._xil_api_maport.write_variable(xil_variable[0]['Enable_StartStopSetFromSCLX'], self._setTrue)

        startTime_RTPC = self._xil_api_maport.read_variable(xil_variable[0]['startTime_RTPC'])
        self._logger.info(f'startTime_RTPC: {startTime_RTPC}')

    def SCLX_model_config_offset(self) -> None:
        # change value of start offset and stop offset
        self._xil_api_maport.write_variable(xil_variable[0]['StartOffset'], self._start_offset)
        # self._xil_api_maport.write_variable(xil_variable[0]['StopOffset'], self._stop_offset)
        startoffset = self._xil_api_maport.read_variable(xil_variable[0]["StartOffset"])
        self._logger.info(f'StartOffset: {startoffset}')

    def SCLX_model_config_startReplay(self) -> None:
        # start Relplay !!!!!!
        EnableReplay = self._xil_api_maport.read_variable(xil_variable[0]['StartReplay'])
        self._logger.info(f'EnableReplay: {EnableReplay}')
        if EnableReplay == 0:
            self._xil_api_maport.write_variable(xil_variable[0]['StartReplay'], int(1))
            startReplay = self._xil_api_maport.read_variable(xil_variable[0]['StartReplay'])
            self._logger.info(f'set EnableReplay: {startReplay}')

    def SCLX_model_read_ptpMaster(self) -> None:
        # gPTP_Master
        BaseTimeVector_DuT = self._xil_api_maport.read_variable(xil_variable[1]['BaseTimeVector_DuT'])
        self._logger.info(f'BaseTimeVector_DuT: {BaseTimeVector_DuT}')
        UpdateBaseTime_DuT = self._xil_api_maport.read_variable(xil_variable[1]['UpdateBaseTime_DuT'])
        self._logger.info(f'UpdateBaseTime_DuT: {UpdateBaseTime_DuT}')

    def DuTSyncStartCalc(self, logger) -> None:
        # DuTSyncStartCalc
        if self._set_print == 2:
            firsttTs_Lidar = self._xil_api_maport.read_variable(xil_variable[2]['firsttTs_Lidar'])
            logger.info(f'firsttTs_Lidar: {firsttTs_Lidar}')

            firstTimestamp_LidarMSOP = self._xil_api_maport.read_variable(xil_variable[2]['firstTimestamp_LidarMSOP'])
            logger.info(f'firstTimestamp_LidarMSOP: {firstTimestamp_LidarMSOP}')

            firstTs = self._xil_api_maport.read_variable(xil_variable[2]['firstTs'])
            logger.info(f'firstTs: {firstTs}')

            AdditionalOffset = self._xil_api_maport.read_variable(xil_variable[2]['AdditionalOffset'])
            logger.info(f'AdditionalOffset: {AdditionalOffset}')

            syncStartTime_DuT = self._xil_api_maport.read_variable(xil_variable[2]['syncStartTime_DuT'])
            logger.info(f'syncStartTime_DuT: {syncStartTime_DuT}')
        self._set_print += 1

    def get_replay_state(self, replay_state, logger) -> None:
        # get Progress
        replayDuration = self._xil_api_maport.read_variable(xil_variable[0]["replayDuration"])
        replay_state.elapsed_time = replayDuration
        if logger:
            logger.info(f'replay Duration: {replayDuration}')

        replayProgress = self._xil_api_maport.read_variable(xil_variable[0]["replayProgress"])
        replay_state.progress = round(replayProgress * 100)
        if logger:
            logger.info(f'progress: {replay_state.progress}%')

        if logger:
            replayState = self._xil_api_maport.read_variable(xil_variable[0]["replayState"])
            logger.info(f'replayState for model: {replayState}')

    def monitor_ESI1(self, logger) -> None:
        # ESI1
        for key, value in xil_variable[3].items():
            if "comment" not in key:
                Buffered_ESI1 = self._xil_api_maport.read_variable(value)
                logger.debug(f'{key}: {Buffered_ESI1}')

    def monitor_ESI2(self, logger) -> None:
        # ESI2
        for key, value in xil_variable[4].items():
            if "comment" not in key:
                Buffered_ESI2 = self._xil_api_maport.read_variable(value)
                logger.debug(f'{key}: {Buffered_ESI2}')

    def monitor_RTPC(self, logger) -> None:
        # RTPC1
        for key, value in xil_variable[5].items():
            if "comment" not in key:
                value_rtpc1 = self._xil_api_maport.read_variable(value)
                logger.debug(f'{key}: {value_rtpc1}')

    def monitor_someip(self, logger) -> None:
        for name, path in self._someip_monitor_list:
            value = self._xil_api_maport.read_variable(path)
            logger.debug(f'{name}: {value}')

    # def monitor_taskOverrun(self, logger) -> None:
    #     # task overrun monitor
    #     for key, value in xil_variable[6].items():
    #         if "comment" not in key:
    #             value_overrun = self._xil_api_maport.read_variable(value)
    #             logger.debug(f'{key}: {value_overrun}')

    def _configure_data_manipulation(self, configuration_string):
        try:
            config_units = deserialize_data_manipulation(configuration_string)
            check_data_manipulation_objects(config_units)
        except Exception as exc:
            # Ideally should never happen because we check the data also
            # before serialization.
            raise BadManipulationConfigurationError() from exc

        access_objects = {
            'maport': self._xil_api_maport,
            'rtmaps': {
                PlayerLocation.PC1: self._rtmaps,
                PlayerLocation.PC2: self._remote_rtmaps,
            },
        }
        for unit in config_units:
            manipulation_object = DataManipulation(
                configuration=unit,
                access_objects=access_objects,
                logger=self._logger.getChild('data_manipulation'),
            )
            manipulation_object.apply()

    def _check_replay_data_files(self, connection_manager, pc1_path, pc2_path):
        available = {}
        pc1_sensors = {
            'camera': [],
            'can': [],
            'lidar': [],
        }
        pc2_args = [pc2_path, '-vvv']

        for connection in connection_manager.get_port_connections():
            if connection.player.location == PlayerLocation.PC1:
                pc1_sensors[connection.type].append(connection.name)
            else:
                pc2_args.extend((f'--{connection.type}', connection.name))

        # Running check on PC1.
        logger = self._logger.getChild('IdxRecCompare')
        compare_obj = idx_rec_compare.IdxRecCompare(
            pc1_path,
            pc1_sensors,
            logger,
        )
        available[PlayerLocation.PC1], timestamp1 = compare_obj.data_check()

        # Running check on PC2.
        serialized_output, stderr = run_file_remotely(
            idx_rec_compare.__file__,
            *pc2_args,
            remote_ip=REMOTE_IP,
            username='dspace',
            password='dspace',
        )

        if stderr.strip():
            for line in stderr.splitlines():
                try:
                    level, msg = line.split(':', 1)
                    log_func = getattr(self._logger, level.lower())
                except (ValueError, AttributeError):
                    self._logger.error(line)
                else:
                    log_func(f'Remote IdxRecCompare: {msg}')

        output = json.loads(serialized_output)
        available[PlayerLocation.PC2] = output['sensors']
        timestamp2 = int(output['ts_end'])

        for location, sensors in available.items():
            self._logger.debug(f'Data analysis results on {location}:')
            for name, data in sensors.items():
                self._logger.debug(f'\t{name}: {data}')

        self._logger.debug(f'End time PC1: {timestamp1}')
        self._logger.debug(f'End time PC2: {timestamp2}')

        found = []
        missing = []

        for connection in connection_manager.get_port_connections():
            if available[connection.player.location][connection.name][1] > 0:
                found.append(connection)
            else:
                missing.append(connection)

        self._logger.info(
            f'Connections considered: {[conn.name for conn in found]}'
        )
        if missing:
            self._logger.warning(
                f'Missing connections: {[conn.name for conn in missing]}'
            )
            # Tolerate errors regarding those possibly missing.
            for conn in missing:
                self._rtmaps_error_handlers.append(
                    RTMapsErrorHandler(
                        regex=f"{conn.name}: couldn't find stream file",
                        error_type=RTMapsLogError.NoError,
                    )
                )

        return found, min(timestamp1, timestamp2)

    def _prepare_someip_monitor_list(self):
        var_list = []
        for entry in XilManipulationBase.get_known_paths():
            m = SOMEIP_SUBSCRIPTION_REGEX.match(entry)
            if m:
                name = (
                    f'ECU {m.group("ECU"):<16} / '
                    f'Service {m.group("ServiceID")} '
                    f'({hex(int(m.group("ServiceID")))})'
                    f'  Subscription status'
                )
                var_list.append((name, entry))
                continue

            m = SOMEIP_COUNTER_REGEX.match(entry)
            if m:
                name = (
                    f'ECU {m.group("ECU"):<16} / '
                    f'Service {m.group("ServiceID")} '
                    f'({hex(int(m.group("ServiceID")))}) /   '
                    f'{m.group("Type")[:-1]} {m.group("Name")} / '
                    f'{m.group("Counter")}'
                )
                var_list.append((name, entry))
                continue

        var_list.sort()
        self._someip_monitor_list = var_list

    def configure(self, replay_data, log_files, additional_properties=None):
        # Read the debug flag.
        self._debug = replay_data.get('debug', 'false').lower() == 'true'
        if self._debug:
            self._logger.warning('Debug mode activated!')

        try:
            # First try!
            self._real_configure(
                replay_data=replay_data,
                log_files=log_files,
                additional_properties=additional_properties,
            )
        except Exception:
            self._logger.exception(
                'First configuration attempt failed. Trying again...'
            )
            self.cleanup(datareplay_pb2.ERROR, replay_data)

            master_diagram = self._rtmaps.diagram

            self._remote_rtmaps = None
            self._rtmaps.shutdown()
            self._rtmaps.reset()
            time.sleep(3)
            self._pending_exception = None

            # Second try
            self._rtmaps.load_diagram(master_diagram)
            self._real_configure(
                replay_data=replay_data,
                log_files=log_files,
                additional_properties=additional_properties,
            )

        # Start the replay.
        try:
            # Start stop replay.
            self.SCLX_model_config_StartStop()

            # Change value of start offset and stop offset.
            # self.SCLX_model_config_offset()

            # Start relplay.
            self.SCLX_model_config_startReplay()

            self._logger.info("Replay started!")
            self._logger.info('Waiting for 3s')
            time.sleep(3)

            # gPTP_Master
            # self.SCLX_model_read_ptpMaster()
        except TestbenchPortException as ex:
            self._logger.exception(
                f'VendorCodeDescription: {ex.VendorCodeDescription}'
            )
            raise XilApiError(ex.CodeDescription)

    def _real_configure(self, replay_data, log_files, additional_properties=None):
        self._user_data['start_time'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )
        self._replay_data_timeout = float(replay_data.get(
            'replay_data_timeout',
            DEFAULT_REPLAY_DATA_TIMEOUT,
        ))

        # Check if there were already errors during the initialization.
        if self._pending_exception is not None:
            tmp_exception = self._pending_exception
            self._pending_exception = None
            raise tmp_exception

        # Connect to Pyro server.
        self._connect_to_remote_rtmaps()

        # Download control desk app (Raises the apropriate exception if failed).
        self.download_SCLX_APP(
            replay_data['sdf_path'],
            replay_data['CMDLOADER'],
            replay_data['platform_name'],
        )

        # initialize xil api
        self._logger.info('Initializing XIL API access.')
        self._xil_api_maport = XILAPIMAPort(
            replay_data['platform_name'],
            replay_data['sdf_path'],
            directory='.',
            logger=None,
        )

        # reload all the known XIL path in the cache.
        self._logger.info('Reloading XIL variables.')
        reload_xil_paths(self._xil_api_maport)
        self._prepare_someip_monitor_list()

        self._ensure_version_compatibility(self._xil_api_maport)

        # check ESI connection (it raises an exception if not reachable).
        self.ESI_check()
        self._last_esi_check = time.perf_counter()

        data_path_pc1 = replay_data['path1']
        data_path_pc2 = replay_data['path2']
        connection_manager = PortConnectionManager()

        connections, end_time = self._check_replay_data_files(
            connection_manager,
            data_path_pc1,
            data_path_pc2,
        )

        replay_time = None
        try:
            if replay_data['replay_time_flag'].lower() == 'true':
                replay_time = ReplayTimeConfiguration(
                    start=int(replay_data['replay_start_time']),
                    end=int(replay_data['replay_end_time']),
                )
        except Exception as exc:
            self._logger.error(
                f'Failed to parse replay-time configuration: {exc}'
            )

        # prepare connection
        connection_logger = self._logger.getChild("RTMapsConnection")
        rtmaps_connection = RtmapsConnection(
            self._rtmaps,
            self._remote_rtmaps,
            end_time,
            connection_logger,
        )

        # Enable the "slave" mechanism on the secondary server
        rtmaps_connection.configure_diagrams(
            replay_data['slave_diagram'],
            REMOTE_IP,
            RTMAPS_PORT,
        )

        data_lengths = {}
        for player in connection_manager.get_players():
            if player.location == PlayerLocation.PC1:
                data_path = data_path_pc1
            else:
                data_path = data_path_pc2

            data_lengths[player.location] = rtmaps_connection.configure_player(
                player=player,
                data_path=data_path,
                replay_time=replay_time,
            )

        for location, length in data_lengths.items():
            self._user_data[f'{location}_data_length'] = str(length)

        for connection in connections:
            rtmaps_connection.connect_port(connection)

        self._fetch_diagram_counters()

        # Check for any errors from the remote rtmaps.
        time.sleep(3)
        self._process_remote_rtmaps_logs()
        # Check if there were already errors during the initialization.
        if self._pending_exception is not None:
            tmp_exception = self._pending_exception
            self._pending_exception = None
            raise tmp_exception

        # Apply data manipualtion
        try:
            self._configure_data_manipulation(replay_data['data_manipulation'])
        except TestbenchPortException as ex:
            self._logger.exception(
                f'VendorCodeDescription: {ex.VendorCodeDescription}'
            )
            raise XilApiError(ex.CodeDescription)

    def _process_scalexio_messages(self, logger):
        logger = logger.getChild('ScalexioMessages')

        for message in self._sclx2_reader.get_messages():
            if message.type in (DsMessageType.WARNING, DsMessageType.ERROR):
                # Report warnings and errors.
                if "late" not in message.message:
                    logger.log(message.type.logging_level, message.message)
                if 'The sending buffers might be blocked' in message.message:
                    raise RealtimeApplicationError(message.message)

    def _fetch_diagram_counters(self):
        self._logger.info("Getting the replay counters in each diagram")
        counters_to_check = [
            ("remote diagram", self._remote_rtmaps, "data_counter_dsu0_1"),
            ("local diagram", self._rtmaps, "data_counter_dsu1_1"),
        ]
        for name, rtmaps, component_id in counters_to_check:
            try:
                rtmaps.check_component_availability(component_id)
                rtmaps.check_property_availability(
                    component_id,
                    COUNTER_SUM_PROP_NAME,
                )
            except Exception:
                self._logger.warning(
                    f"Component {component_id} is missing in the {name}!"
                )
                self._logger.warning(f"Replay timeout disabled for {name}!")
            else:
                self._logger.info(
                    f"Registering counter {component_id} for {name}."
                )
                self._counters.append(
                    CounterData(
                        name=name,
                        rtmaps=rtmaps,
                        component_id=component_id,
                    )
                )

    def _check_real_replay_progress(self, logger=None):
        logger = logger or self._logger
        for data in self._counters:
            try:
                count = data.rtmaps.get_integer_property(
                    data.component_id,
                    COUNTER_SUM_PROP_NAME,
                )
            except Exception:
                logger.exception(
                    f"Failed to read replay count on {data.name}!"
                )
                count = data.last_count
            logger.debug(
                f"Count of {data.component_id} in {data.name}: {count}"
            )

            # This converts None to the last count.
            # Usually Nones are only returned when the component has not
            # finished starting (but what if it was dead?). We use last_count,
            # just to be safe to invoke a timeout.
            count = count or data.last_count

            if count > data.last_count or data.timestamp == 0:
                data.last_count = count
                data.timestamp = time.perf_counter()
            else:
                elapsed_time = time.perf_counter() - data.timestamp
                if elapsed_time >= self._replay_data_timeout:
                    raise ReplayFrozenError(
                        f"{data.name} is stale for {elapsed_time} seconds"
                    )

    def get_progress(self, replay_state):
        # Fill user data:
        for key, value in self._user_data.items():
            replay_state.user_data[key] = value

        replay_state.user_data['update_time'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        # check ESI connection (it raises an exception if not reachable).
        if time.perf_counter() - self._last_esi_check >= ESI_CHECK_INTERVAL:
            self.ESI_check()
            self._last_esi_check = time.perf_counter()

        # Check if there are any pending exceptions (mostly form the rtmaps logs).
        if self._pending_exception is not None:
            tmp_exception = self._pending_exception
            self._pending_exception = None
            raise tmp_exception

        self._process_remote_rtmaps_logs()

        if self._xil_api_maport is None:
            # There is nothing to read anyway.
            replay_state.state = datareplay_pb2.ERROR
            return

        progress_logger = self._logger.getChild("getProgress")

        try:
            # DuTSyncStartCalc
            self.DuTSyncStartCalc(progress_logger)

            if time.perf_counter() - self._last_progress_print < PROGRESS_PRINT_INTERVAL:
                progress_logger = None
            else:
                self._last_progress_print = time.perf_counter()

            # get Progress
            self.get_replay_state(replay_state, progress_logger)

            # Replay System Monitoring
            if progress_logger:
                self.monitor_ESI1(progress_logger)
                self.monitor_ESI2(progress_logger)
                self.monitor_RTPC(progress_logger)
                if self._debug:
                    self.monitor_someip(progress_logger)
                # self.monitor_taskOverrun(progress_logger)
        except TestbenchPortException as ex:
            self._logger.exception(f'CodeDescription: {ex.CodeDescription}')
            self._logger.error(
                f'VendorCodeDescription: {ex.VendorCodeDescription}'
            )

        if progress_logger:
            self._check_real_replay_progress(progress_logger)
            self._process_scalexio_messages(progress_logger)

        if replay_state.progress == 100:
            self._logger.info("Waiting for data transmission in 2s")
            time.sleep(2)

            # get finish time
            replay_state.user_data['end_time'] = datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'
            )

            self._logger.info(f"start_time: {replay_state.user_data['start_time']}")
            self._logger.info(f"end_time: {replay_state.user_data['end_time']}")

            replay_state.state = datareplay_pb2.FINISHED

    def cleanup(self, reason, replay_data):
        self._logger.info(f"Starting cleanup process with reason {reason}.")

        # Check ESI connection (just for logging purposes).
        try:
            self.ESI_check(reboot_on_error=False)
        except Exception:
            self._logger.exception('Failed to check ESI status.')

        # xil api clean up
        if self._xil_api_maport is not None:
            self._logger.info("Cleaning-up the XIL MAPort object.")
            try:
                self._xil_api_maport.cleanup()
            except Exception:
                self._logger.exception('Failed to cleanup XIL MAPort.')

        # SCALEXIO unload
        if self._rt_app_download:
            try:
                self.unload_SCLX_APP(
                    replay_data['CMDLOADER'],
                    replay_data['platform_name'],
                )
            except Exception:
                self._logger.exception('Failed to unload application.')

        # Read any available logs in the remote rtmaps without exceptions.
        try:
            self._process_remote_rtmaps_logs(handle_errors=False)
        except Exception:
            self._logger.exception('Failed to process remote RTMaps logs.')

        if self._restart_service_pending:
            try:
                self._restart_services()
            except Exception:
                self._logger.exception('Failed to restart services.')

        self._logger.info("Cleanup process finished.")

    def _restart_services(self):
        self._logger.warning(
            'An RTMaps error requiring a restart has been detected! '
            'Both services will be restarted now!'
        )
        kill_runtime.pc_ssh_restart(REMOTE_IP, "dspace", "dspace")
        kill_runtime.restart_replay_api_service()

    def _process_rtmaps_log(self, level, message, logger):
        # To understand the following magic formula see the following
        # mapping table between the numeric values of the log-levels of RTMaps
        # and of the Python logging module:
        # ________________________________
        # | Level     | RTMaps | logging |
        # |-----------|--------|---------|
        # | debug     | 3      | 10      |
        # | info      | 0      | 20      |
        # | warning   | 1      | 30      |
        # | error     | 2      | 40      |
        # | exception | N/A    | 50      |
        # --------------------------------
        # logging.DEBUG has a level of 10. Anything less, is most likely RTMaps
        # style logging level, so we need to map it.
        # RTMaps has the debug-level as the highest value, we solve this by
        # adding 1 and then taking modulo 4. This puts the debug-level at 0,
        # info at 1, warning at 2 and error at 3.
        # Now all that is missing is an offset of 1 and a factor of 10 to get
        # the same levels as the logging module of Python.
        if level < 10:
            level = (((level + 1) % 4) + 1) * 10

        # Print the log message if a logger was provided.
        if logger is not None:
            logger.log(level, message)

        # Handle errors.
        if level < logging.ERROR:
            return RTMapsLogError.NoError

        try:
            handler = self._rtmaps_error_handlers[message]
        except IndexError:
            self._logger.warning(f'Unknown RTMaps error: {message}')
            return RTMapsLogError.GenericError
        else:
            self._logger.info(
                f'RTMaps error "{message}" resolved to {handler.error_type}'
            )
            return handler.error_type

    def _create_esi_support(self, esi):
        self._logger.info(f"Reading ESI logs from {esi.name}...")
        # basedir = Path("/var/log/dspace")
        basedir = Path("/tmp")
        username = r"root"
        password = r"100%esiu"
        try:
            now = datetime.now().isoformat().replace(":", "_")
            tmp_dir = basedir / f"{now}_{esi.name}_{esi.ip}"
            tmp_dir.mkdir(parents=True, exist_ok=True)

            # Job log file.
            file_handlers = list(filter(
                lambda x: isinstance(x, logging.FileHandler),
                self._logger.parent.handlers,
            ))
            if file_handlers:
                logfile = Path(file_handlers[-1].baseFilename)
                if logfile.exists():
                    self._logger.info(
                        f"Collecting the job's log file: {logfile}"
                    )
                    shutil.copy2(logfile, tmp_dir)

            self._logger.info(self._logger.parent.handlers)
            # FTP files.
            ftp_cred = {
                "id": esi.name,
                "addr": esi.ip,
                "usr": username,
                "passwd": password,
            }
            self._logger.info("Getting logs per FTP.")
            ftp_handle = esi_ftp_client(ftp_cred, self._logger)
            if ftp_handle.ftp_ready:
                ftp_handle.download_logs(save_dir=tmp_dir)
                coredump = Path('/tmp') / 'generic_app.elf.core'
                self._logger.info(
                    f'Attempting to download a coredump from {coredump}'
                )
                try:
                    ftp_handle.download_binary(
                        coredump,
                        tmp_dir / coredump.name,
                    )
                except Exception as exc:
                    self._logger.info(f'No coredump downloaded: {exc}!')
                else:
                    self._logger.warning('ESI coredump downloaded!')
                ftp_handle.quit()
            else:
                self._logger.error(f'Failed to connent to {esi} per FTP!')

            # Telnet stats.
            self._logger.info("Getting system stats per Telnet.")
            with esi_telnet.Telnet(esi.ip, 23, timeout=10) as tn:
                tn.login(username, password)
                esi_telnet.get_esi_stats(tn, tmp_dir)
        finally:
            # Zip the content.
            tmp_tarball_name = Path.home() / tmp_dir.with_suffix(".tgz").name
            ds_log_dir = Path('/var/log/dspace/')
            self._logger.info(f"Creating tarball file at {tmp_tarball_name}")
            with tarfile.TarFile.gzopen(tmp_tarball_name, mode="x") as tarball:
                tarball.add(tmp_dir)
            self._logger.info(
                f"ESI logs of {esi.name} written to {tmp_tarball_name}."
            )
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                shutil.move(tmp_tarball_name, ds_log_dir)
            except Exception as exc:
                self._logger.warning(
                    f'Failed to move tarball to {ds_log_dir}: {exc}'
                )
                self._logger.info(f'The file is left at {tmp_tarball_name}')
            else:
                self._logger.info(
                    f'Tarball moved to {ds_log_dir / tmp_tarball_name.name}'
                )

    def _reset_esis(self, force=False):
        self._logger.warning("Attempting to restart ESI units!")
        if not force and self._esi_restart_count > 0:
            self._logger.warning("Suppressing a second ESI reset!")
            return

        self._esi_restart_count += 1

        if self._esi is None:
            self._logger.error('No ESI units device instances created!')
            return

        self._logger.info("Reading ESI logs...")
        for esi in self._esi.devices:
            try:
                self._create_esi_support(esi)
            except Exception:
                self._logger.exception(
                    f'Error while creating support tarball for {esi.name}'
                )

        self._logger.warning("Resetting ESI units is disabled!")
        raise ESIError()

        self._logger.warning("Resetting ESI units!")

        ecu_is_on = self._ecu.power_state == NetioState.on
        if ecu_is_on:
            # Turning ECU off.
            self.ecu_off()

        retries = 3
        reset_failed = False
        for i in range(retries, 0, -1):
            # Since both ESIs are connected to the same socket, it's enough to
            # reboot only one of them.
            self._esi.reboot()

            try:
                # Wait for both ESIs to finish booting.
                self._logger.info('Waiting for the ESI units.')
                self._esi.wait_till_online()
                self._logger.info('ESI units ready.')
            except TimeoutError as exc:
                # The raised TimeoutError has the device as its argument.
                dev = exc.args[0]
                self._logger.error(f"{dev.name} failed to boot properly.")
                continue
            else:
                break
        else:
            self._logger.error(
                f"Failed to reset ESI units! (tried {retries} times)"
            )
            reset_failed = True

        if ecu_is_on:
            # Turning ECU back on.
            self.ecu_on()
            # ECU was restarted. Abort the test and notify the job.
            raise ECURestartedError()

        if reset_failed:
            raise ESIRestartTimeoutError()

    def _process_remote_rtmaps_logs(self, handle_errors=True):
        # Get the rtmaps console log output from the secondary server.
        # Currently simply log them again on the main machine
        if self._remote_rtmaps is None:
            return

        rtmaps_logger = self._logger.getChild("RemoteRTMaps")
        # Read the logs and clear right away to make sure we are consistent.
        try:
            rtmaps_logs = self._remote_rtmaps.get_log()
            self._remote_rtmaps.clear_log()
        except Exception:
            self._logger.warning('Failed to read remote RTMaps logs!')
            return

        exception_to_raise = None
        while rtmaps_logs:
            level, message = rtmaps_logs.pop(0)
            error = self._process_rtmaps_log(level, message, rtmaps_logger)

            # Error handling.
            if error != RTMapsLogError.NoError:
                # We catch any possible exception and don't raise it right
                # away because we want to print all messages first!
                try:
                    self._handle_rtmaps_error(error, message)
                except Exception as exc:
                    # We only store the first one for raising at the end.
                    exception_to_raise = exception_to_raise or exc
        if handle_errors and exception_to_raise is not None:
            raise exception_to_raise

    def _handle_rtmaps_error(self, error, message):
        self._logger.error(message)
        # Handling ESI Problems.
        if error == RTMapsLogError.Image2EsiError:
            # This method will raise its own exception if needed.
            self._reset_esis()
        else:
            if error == RTMapsLogError.RTMapsZombie:
                self._restart_service_pending = True
            raise RTMapsError(message)

    def get_callback(self):
        """
        Optional: If defined, it must return a function that accepts two
        arguments (level and message) or None. The function is called each time
        an RTMaps log event occurs. Must be non blocking.
        """
        def rtmaps_callback(level, message):
            error = self._process_rtmaps_log(level, message, None)
            # Generic error handling.
            if error != RTMapsLogError.NoError:
                self._logger.error(message)

                # Strictly speaking, we can let the exceptions get raised here,
                # because the replay_executor catches those exceptions and
                # stores them anyway. But we might want to handle our own
                # exceptions internally to be on the safe side.
                new_exception = None
                try:
                    self._handle_rtmaps_error(error, message)
                except Exception as exc:
                    new_exception = exc

                # Store the new exception only if there isn't one pending already.
                self._pending_exception = self._pending_exception or new_exception

        return rtmaps_callback
