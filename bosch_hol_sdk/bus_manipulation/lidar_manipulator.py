from .manipulationbase import XilManipulationBase


class LidarManipulator(XilManipulationBase):
    # get signal path as dict from text document
    def _get_var_path(self) -> dict:
        """ construct variable path from the configuration """
        frame_path = (
            'SCALEXIO Real-Time PC()://Replay_MP_Chery_BM/Model Root/'
            'ReplayStreams/UDP_Stream_1/SuspendLidarUDP'
        )

        paths = {
            'start': f'{frame_path}/StartTime/Value',
            'duration': f'{frame_path}/Duration/Value',
        }

        self._check_variable_path(paths.values())
        return paths

    def apply(self) -> None:
        """ Apply the CAN manipulation according to the configuration. """
        var_path = self._get_var_path()

        self._write_variable(
            var_path['start'],
            self.configuration.steps[0].start,
            'UDP_Stream_1_start',
        )
        self._write_variable(
            var_path['duration'],
            self.configuration.steps[0].duration,
            'UDP_Stream_1_duration',
        )


if __name__ == '__main__':
    import logging
    import subprocess
    import time

    from DataReplayAPI.drapi.helper.xilapi_maport import XILAPIMAPort
    from configuration import (
        LidarManipulationUnit, ReplayManipulationStep, ReplayManipulationType,
    )

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
        configuration = LidarManipulationUnit(
            type=ReplayManipulationType.SUSPEND,
            steps=[
                ReplayManipulationStep(
                    start=10.0,
                    duration=10.0,
                ),
            ],
        )

        usp = LidarManipulator(configuration, xil_api_maport)
        usp.apply()
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
