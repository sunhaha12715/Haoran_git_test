from dspace.bosch_hol_sdk.configuration import ReplayManipulationType
from .manipulationbase import XilManipulationBase


_CAN_IF_MAPPING = {
    'CAN01': 'DS6341_S13_Ch1',
    'CAN02': 'DS6341_S13_Ch2',
    'CAN03': 'DS6341_S13_Ch3',
    'CAN04': 'DS6341_S13_Ch4',
    'CAN05': 'DS6342_S14_Ch1',
    'CAN06': 'DS6342_S14_Ch2',
    'CAN07': 'DS6342_S14_Ch3',
    'CAN08': 'DS6342_S14_Ch4',
    'CAN09': 'DS6342_S14_Ch5',
    'CAN10': 'DS6342_S14_Ch6',
}


class CanManipulator(XilManipulationBase):

    def _get_var_path(self) -> dict:
        """ construct variable path from the configuration """
        # Construct the variable path
        _id = f'0x{self.configuration.id:03X}'
        frame = self.configuration.frame
        signal = self.configuration.signal
        has_value = False

        # Get board chanel from network and CAN_IF_MAPPING
        try:
            physical_board_channel = _CAN_IF_MAPPING[self.configuration.name]
        except KeyError as exc:
            raise KeyError(
                f"Unknown CAN channel '{self.configuration.name}'. "
                'Check configuration.'
            ) from exc

        path_base = (
            'SCALEXIO Real-Time PC()://Replay_MP_Chery_BM/Model Root/'
            f'CANManipulation/{physical_board_channel}'
        )

        manipulation_type = self.configuration.type
        if manipulation_type in [
            ReplayManipulationType.OVERWRITE,
            ReplayManipulationType.CRC,
        ]:
            frame_path = f'{path_base}/SM_[{_id}]_[{frame}]_[{signal}]'
            # NOTE: The value-field exists for CRC but it is not evaluated.
            has_value = True
        elif manipulation_type is ReplayManipulationType.SUSPEND:
            frame_path = f'{path_base}/FS_[{_id}]_[{frame}]'
            has_value = False
        elif manipulation_type is ReplayManipulationType.ROLLING_COUNTER:
            frame_path = (
                f'{path_base}/SigFreezer_[{_id}]_[{frame}]_[{signal}]'
            )
            has_value = False
        else:
            raise ValueError(
                f'Unsupported manipulation type for CAN ({manipulation_type})'
            )

        paths = {
            'start': f'{frame_path}/StartTime/Value',
            'duration': f'{frame_path}/Duration/Value',
        }
        if has_value:
            paths['value'] = f'{frame_path}/Value/Value'

        self._check_variable_path(paths.values())
        return paths

    def apply(self) -> None:
        """ Apply the CAN manipulation according to the configuration. """
        var_path = self._get_var_path()
        config = self.configuration

        # #####################################################################
        # The following table explains the supported cases at the moment.
        # _______________________________________________________________
        # | Type                     | list         |  settable value   |
        # ---------------------------------------------------------------
        # | Overwrite (manipulation) | yes          |  yes              |
        # | CRC                      | yes          |  no (ignored)     |
        # | Suspend                  | no           |  no               |
        # | Rolling counter          | no           |  no               |
        # ---------------------------------------------------------------
        #
        # #####################################################################

        if config.type in [
            ReplayManipulationType.OVERWRITE,
            ReplayManipulationType.CRC,
        ]:
            self._write_variable(
                var_path['start'],
                [step.start for step in config.steps],
                f'{config.frame}_{config.signal}_start',
            )
            self._write_variable(
                var_path['duration'],
                [step.duration for step in config.steps],
                f'{config.frame}_{config.signal}_duration',
            )
            if config.type is ReplayManipulationType.OVERWRITE:
                self._write_variable(
                    var_path['value'],
                    [step.value for step in config.steps],
                    f'{config.frame}_{config.signal}_value',
                )
        elif config.type in [
            ReplayManipulationType.SUSPEND,
            ReplayManipulationType.ROLLING_COUNTER,
        ]:
            self._write_variable(
                var_path['start'],
                config.steps[0].start,
                f'{config.frame}_start',
            )
            self._write_variable(
                var_path['duration'],
                config.steps[0].duration,
                f'{config.frame}_duration',
            )
        else:
            # Actually should never happen because we check in different places
            self._logger.error(
                f'Received unexpected manipulation type {config.type}'
            )


if __name__ == '__main__':
    import logging
    import subprocess
    import time

    from DataReplayAPI.drapi.helper.xilapi_maport import XILAPIMAPort
    from configuration import ReplayManipulationStep, CanManipulationUnit
    # import json

    # with open("/home/dspace/workspace/replay_test/user_config.json") as file:
    #     data = json.load(file)
    # sdf_path = data[0]["sdf_path"]
    # platform = data[0]["platform"]
    # CMDLOADER = data[0]["CMDLOADER"]

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    sdf_path = r'/home/dspace/workspace/pp16699_engineering/RTApp/BuildResult_Rev_1_1_0/Replay_MP_Chery_BM.sdf'
    platform = 'SCALEXIO_2_2'
    CMDLOADER = '/opt/dspace/xilapi.net4.0/MAPort/Main/bin/CmdLoader'

    # SCALEXIO load
    subprocess.run(
        [CMDLOADER, '-ra'],
        shell=True,
        text=True,
        capture_output=True,
        timeout=10,
    )
    result = subprocess.run(
        [CMDLOADER, '-p', platform, sdf_path],
        text=True,
        capture_output=True,
    )

    if result.stderr:
        logging.error(result.stderr)

    if result.stdout:
        logging.info(result.stdout)

    logging.info(
        f'Downloading application finished with code {result.returncode}'
    )

    logging.info('waiting for 5 after application download')
    time.sleep(5)

    xil_api_maport = XILAPIMAPort(
        platform,
        sdf_path,
        directory='.',
        logger=None,
    )
    # xil_api_maport = None

    try:
        # Create bus object for manipulation
        bus_object_list = [
            CanManipulationUnit(
                name='CAN01',
                id=0x141,
                frame='CSA_1',
                type=ReplayManipulationType.SUSPEND,
                steps=[
                    ReplayManipulationStep(
                        start=3.265,
                        duration=2.123,
                    ),
                ],
            ),
            CanManipulationUnit(
                name='CAN09',
                id=0x210,
                frame='PDC_Priv_1_OBJ',
                signal='RollgCntr',
                type=ReplayManipulationType.ROLLING_COUNTER,
                steps=[
                    ReplayManipulationStep(
                        start=3.265,
                        duration=2.123,
                    ),
                ],
            ),
            CanManipulationUnit(
                name='CAN10',
                id=0x02C,
                frame='LCM_1',
                signal='RollgCntr1',
                type=ReplayManipulationType.ROLLING_COUNTER,
                steps=[
                    ReplayManipulationStep(
                        start=10,
                        duration=18.5,
                    ),
                ],
            ),
            CanManipulationUnit(
                name='CAN01',
                id=0x141,
                frame='CSA_1',
                signal='GearShiftPosInverse',
                type=ReplayManipulationType.OVERWRITE,
                steps=[
                    ReplayManipulationStep(
                        start=3.265,
                        duration=1.234,
                        value=0.0,
                    ),
                    ReplayManipulationStep(
                        start=5.698,
                        duration=1.234,
                        value=1.0,
                    ),
                    ReplayManipulationStep(
                        start=7.698,
                        duration=1.234,
                        value=15.0,
                    ),
                ],
            ),
        ]

        print(bus_object_list)

        # Apply manipulation
        for obj in bus_object_list:
            bmc = CanManipulator(obj, xil_api_maport)
            bmc.apply()

    finally:
        if xil_api_maport is not None:
            xil_api_maport.cleanup()

        result = subprocess.run(
            [CMDLOADER, '-unload', '-p', platform, '-ol', '2'],
            text=True,
            capture_output=True,
        )

        if result.stderr:
            logging.error(result.stderr)

        if result.stdout:
            logging.info(result.stdout)

        logging.info(
            f'Unloading application finished with code {result.returncode}'
        )
