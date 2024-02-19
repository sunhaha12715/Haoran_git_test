'''
The script is provide to reset replay system with killing runtime
Requirements: Python3,
by JuanL, DSC, 2023/8/9

-- python3.9 system_reset.py sclx
-- python3.9 system_reset.py esi
-- python3.9 system_reset.py ecu
-- python3.9 system_reset.py kil

'''
import argparse
from pathlib import Path
import subprocess
import textwrap
import time
import logging
import logging.handlers

from dspace.bosch_hol_sdk.netio_api_json_via_http import NetioState
from dspace.bosch_hol_sdk.system_reset.kill_runtime import (
    pc_ssh_restart, restart_replay_api_service,
)
from dspace.bosch_hol_sdk.replaydevicecontrol import (
    get_replay_device, ReplayDevice,
)


pc_2 = {
    "ip": "192.168.140.102",
    "username": "dspace",
    "passwd": "dspace",
}

platform = "SCALEXIO_2"
CMDLOADER = '/opt/dspace/xilapi.net4.0/MAPort/Main/bin/CmdLoader'


def kill_runtime():
    logging.info('Start Kill rtmaps runtime...')

    try:
        if pc_ssh_restart(pc_2["ip"], pc_2["username"], pc_2["passwd"]):
            logging.info("Restart pyro server service on pc2 success.")
        else:
            logging.warning("Restart pyro server service on pc1 failed.")
    except Exception as exc:
        logging.exception(f"Failed to restart pyro server: {exc}")

    if restart_replay_api_service():
        logging.info("Restart replay API service on pc1 success")
    else:
        logging.warning("Restart replay API service on pc1 failed.")


def poweroff_ECU():
    logging.info('Starting to power off ECU...')
    ecu = get_replay_device(ReplayDevice.ECU)

    logging.info("Powering off KL15 and waiting for 10 seconds")
    ecu.KL15.turn_off()
    time.sleep(10)

    logging.info("Powering off KL30 and waiting for 20 seconds")
    ecu.KL30.turn_off()
    time.sleep(20)

    logging.info("Poweroff ECU finished successfully.")


def poweron_ecu():
    logging.info('Starting to power on ECU...')
    ecu = get_replay_device(ReplayDevice.ECU)

    logging.info("Powering on KL30 and waiting for 20 seconds")
    ecu.KL30.turn_on()
    time.sleep(20)

    logging.info("Powering on KL15 and waiting for 10 seconds")
    ecu.KL15.turn_on()
    time.sleep(10)

    logging.info("ECU poweron finished successfully.")


def poweroff_ECU_ESI():
    poweroff_ECU()
    poweroff_esi()

    logging.info('waiting for 5s...')
    time.sleep(5)
    logging.info("Poweroff ECU and ESI finished successfully.")


def poweroff_esi():
    esi = get_replay_device(ReplayDevice.ESI)
    logging.info('Starting to power off ESI...')
    esi.turn_off()

    logging.info("ESI poweroff finished successfully.")


def poweron_esi():
    esi = get_replay_device(ReplayDevice.ESI)
    logging.info('Power on ESI...')
    esi.turn_on()
    time.sleep(10)
    esi.wait_till_online()


def download_SCLX_APP():
    logging.info('ControlDesk application download start...')

    sdf_dir = Path(__file__).parent / 'reset_test_app'
    if not sdf_dir.exists() and not sdf_dir.is_dir():
        logging.error('Wrong package structure. Test app is missing')
        return
    all_sdfs = list(sdf_dir.glob('*.sdf'))
    if not all_sdfs:
        logging.error(f'No SDF file found under {sdf_dir}')
        return
    elif len(all_sdfs) > 1:
        logging.warning(f'More than on SDF file found {all_sdfs}.')
    sdf_path = all_sdfs[0]
    logging.info(f'Using SDF file: {sdf_path}')

    cmd2 = [
        CMDLOADER,
        '-ra'
    ]

    cmd3 = [
        CMDLOADER,
        '-p', platform,
        sdf_path
    ]
    subprocess.run(cmd2, text=True, capture_output=True)
    result = subprocess.run(cmd3, text=True, capture_output=True)

    if result.stderr:
        logging.error(result.stderr)

    if result.stdout:
        logging.info(result.stdout)

    logging.info(f'Downloading application finished with code {result.returncode}')
    logging.info('Waiting for 5 after application download')

    time.sleep(5)


def unload_SCLX_APP():
    logging.info('ControlDesk application unload start...')
    result = subprocess.run(
        [CMDLOADER, '-unload', '-p', platform, '-ol', '2'],
        text=True,
        capture_output=True,
    )

    if result.stderr:
        logging.error(result.stderr)

    if result.stdout:
        logging.info(result.stdout)

    logging.info(f'Unloading application finished with code {result.returncode}')


def reboot_SCLX():
    rtpc1 = get_replay_device(ReplayDevice.SCALEXIO1)
    rtpc2 = get_replay_device(ReplayDevice.SCALEXIO2)

    rtpc1.reboot()
    rtpc2.reboot()
    time.sleep(10)
    rtpc1.wait_till_online(60)
    rtpc2.wait_till_online(60)


def main(State=None):
    if State is None:
        State = get_arg_from_cli()

    State = State.lower()
    logging.info(f'Received command: {State}')

    if State == "sclx":
        logging.info("Start poweroff and poweron sclx, esi and ecu.")
        logging.info("System Reset Start...")
        # Restart SCALEXIO.
        reboot_SCLX()
        # Download SCALEXIO application.
        download_SCLX_APP()
        # Unload SCALEXIO application.
        unload_SCLX_APP()
        logging.info("System Reset Successfully.")

    elif State == "esi":
        logging.info("Start poweroff and poweron esi and ecu.")
        ecu = get_replay_device(ReplayDevice.ECU)
        if ecu.KL15.state != NetioState.off:
            logging.error('Cannot restart the ESI units while the ECU is on')
            return
        logging.info("System Reset Start...")
        # Kill (PC1 and PC2) rtmaps_runtime.
        kill_runtime()
        # poweroff ECU, ESI
        poweroff_esi()
        # Download SCALEXIO application.
        download_SCLX_APP()
        # Poweron ESI, ping and check esi.
        poweron_esi()
        # Unload SCALEXIO application.
        unload_SCLX_APP()
        logging.info("System Reset Successfully.")

    elif State == "ecu":
        logging.info("Start poweroff and poweron ecu.")
        logging.info("System Reset Start...")
        # poweroff ECU
        poweroff_ECU()
        # Poweron ECU
        poweron_ecu()
        logging.info("System Reset Successfully.")

    elif State in ["kill", "killruntime"]:
        logging.info("Start kill rtmaps_runtime.")
        # Kill (PC1 and PC2) rtmaps_runtime
        kill_runtime()
        logging.info("Kill rtmaps_runtime Successfully.")

    elif State == "ecu_on":
        poweron_ecu()

    else:
        logging.error("Please input correct parameter: 'sclx' or 'esi' or 'ecu' or 'kill'.")


def get_arg_from_cli():
    root_logger = logging.getLogger()
    root_logger.setLevel(level=logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

    file_handler = logging.handlers.RotatingFileHandler(
        filename='/var/log/dspace/system_reset.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    file_handler.setLevel(level=logging.INFO)
    file_handler.setFormatter(log_format)
    root_logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(log_format)
    root_logger.addHandler(stream_handler)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent("""\
        Supported actions:
            * sclx:     reboot the SCALEXIO systems
                - Also loads and unloads an application for testing purposes
            * esi:      restart the ESI units
                - Does not work if the ECU is on (KL15)
                - Includes restart the pyro and grpc services as well
                - Loads and unloads an application to synchronize the ESI units
            * ecu:      restart the ECU
                - Restarts both KL15 and KL30 with long delays inbetween
            * kill:     kill the rtmaps-runtimes
                - Also restarts the pyro and grpc services
            * ecu_on:   turn on the ECU
                - Both KL30 and KL15
        """),
    )
    parser.add_argument(
        'action',
        help='the reset action to execute (see the list below)',
        choices=['sclx', 'esi', 'ecu', 'kill', 'ecu_on'],
    )
    args = parser.parse_args()
    return args.action


if __name__ == "__main__":
    main()
