from .manipulationbase import XilManipulationBase
from dspace.bosch_hol_sdk.configuration import ReplayManipulationType


class SomeIpManipulator(XilManipulationBase):

    # get signal path as dict from text document
    def _get_var_path(self) -> dict:
        svc_id = self.configuration.service_id
        signal = self.configuration.signal
        event_group = self.configuration.event_group
        event = self.configuration.event

        path_base = (
            'SCALEXIO Real-Time PC_2()://Replay_MP_SOMEIP/Model Root/'
            'SOMEIP/Manipulation'
        )
        tx_cont_path = (
            f'{path_base}/TxController_[{svc_id}]_[{event_group}]_[{event}]'
        )
        manipulation_path = (
            f'{path_base}/SM_[{svc_id}]_[{event_group}]_[{event}]/'
            'DataElementOverwrite'
        )
        paths = {
            'mode': f'{tx_cont_path}/ManipulationMode/Value',
            'start': f'{tx_cont_path}/StartTime/Value',
            'duration': f'{tx_cont_path}/Duration/Value',
            'value': f'{manipulation_path}/OWVal_[{signal}]/Value',
        }
        self._check_variable_path(paths.values())
        return paths

    def apply(self):
        var_path = self._get_var_path()

        name = f'{self.configuration.service_id}_{self.configuration.event}'
        mode_map = {
            ReplayManipulationType.SUSPEND: 1.0,
            ReplayManipulationType.OVERWRITE: 2.0,
            ReplayManipulationType.SIMULATE: 3.0,
        }

        self._write_variable(
            var_path['mode'],
            mode_map[self.configuration.type],
            f'{name}_mode',
        )

        # Event though SUSPEND has one element, it is modelled as a vector, but
        # only the first element of this vector is evaluated. We already ensure
        # in the checking functions of the configuration that SUSPEND-
        # manipulations have one element, so no need to do it again here.
        self._write_variable(
            var_path['start'],
            [step.start for step in self.configuration.steps],
            f'{name}_start',
        )
        self._write_variable(
            var_path['duration'],
            [step.duration for step in self.configuration.steps],
            f'{name}_duration',
        )

        if self.configuration.type in [
            ReplayManipulationType.OVERWRITE,
            ReplayManipulationType.SIMULATE,
        ]:
            self._write_variable(
                var_path['value'],
                [step.value for step in self.configuration.steps],
                f'{name}_value',
            )


if __name__ == '__main__':
    import logging
    import subprocess
    import time

    from DataReplayAPI.drapi.helper.xilapi_maport import XILAPIMAPort
    from configuration import ReplayManipulationStep, SomeIpManipulationUnit
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
        config = SomeIpManipulationUnit(
            service_name='FOO',
            service_id=45061,
            event_group=1,
            event='notifyTrunLightFaults_Enh_TrunLight',
            signal='DataElement_notifyTrunLightFaults',
            type=ReplayManipulationType.OVERWRITE,
            steps=[
                ReplayManipulationStep(
                    start=20.0,
                    duration=10.0,
                    value=1.0,
                ),
                ReplayManipulationStep(
                    start=30.0,
                    duration=10.0,
                    value=0.0,
                ),
                ReplayManipulationStep(
                    start=40.0,
                    duration=10.0,
                    value=1.0,
                ),
            ],
        )
        smc = SomeIpManipulator(config, xil_api_maport)
        smc.apply()
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
