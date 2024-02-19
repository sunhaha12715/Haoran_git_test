''' 
The script helps to check ESI status via accessing the various logs via ESI's FTP server
Requirements: Python3
by XiangL, DSC, 2023/8/16
'''

''' 
Change log

rev_0_2: 
- enhanced download_logs: i2c logs are included
- use retrbinary to download files

rev_0_1: 
- The first version

'''

''' 
Usage

see Main

'''

# Dependencies
from ftplib import FTP
import re
import logging
from pathlib import Path
from datetime import datetime

# Global varaibles


# Classes
class esi_ftp_client:
    pattern_ip_addr = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    pattern_master_select = re.compile(r"ptp4l\:.+selected best master clock ([\da-f]{6}\.[\da-f]{4}\.[\da-f]{6})")
    pattern_ptp_report = re.compile(r"ptp4l\:.+rms\s+(\d+) max\s+(\d+)")

    __log_generic_app__ = "/home/root/generic_app.log"
    __log_messages__ = "/var/log/messages"
    __log_dmesg__ = "/var/log/dmesg"
    __log_i2c_dir__ = "/var/tmp/"

    def __init__(self, esi_info: dict, logger: logging) -> None:
        '''
        esi_info = {
                    "id": "esi_identification",
                    "addr": "ip_address_of_esi",
                    "usr": "user_name",
                    "passwd": r"password",
                   }
        '''

        self.logger = logger
        self.esiu = None
        self.ftp_ready = False

        # Check esi_info
        esi_info_keys = ["id", "addr", "usr", "passwd"]
        if type(esi_info) is not dict:
            self.logger.error(f"The given <{esi_info}> is not a dictionary of required 'esi_info', failed to create the client.")
        else:
            for idx, key in enumerate(esi_info_keys):
                if key not in esi_info:
                    self.logger.error(f"<{key}> is missing in the given <{esi_info}>, failed to create the client.")
                    break
                if key == "addr":
                    ret = self.pattern_ip_addr.match(esi_info[key])
                    if ret is None:
                        self.logger.error(f"The given IP address <{esi_info[key]}> is not valid, failed to create the client.")
                        break
                if idx == len(esi_info_keys) - 1:
                    self.esiu = esi_info

        if self.esiu is not None:
            try:
                self.ftp = FTP(self.esiu["addr"], self.esiu["usr"], self.esiu["passwd"], timeout=10)
                self.ftp_id = esi_info["id"]
                self.logger.info(f"Successfully connected to ESI FTP Server <{self.ftp_id}> at: <{self.esiu['addr']}>")
            except:
                self.logger.error(f"The ESI FTP server: <{self.esiu['addr']}> cannot be accessed.")
            else:
                self.ftp_ready = True
    
    def quit(self) -> None:
        if self.ftp_ready:
            self.ftp.quit()
            self.ftp_ready = False
            self.logger.info(f"Successfully disconnected from ESI FTP Server <{self.ftp_id}>")
        else:
            self.logger.warning(f"The ESI FTP server has not been connected yet, no need to quit.")
    
    def get_file(self, file_path: str) -> list:
        file_content = list()
        if not self.ftp_ready:
            self.logger.error(f"The ESI FTP server has not been connected yet, cannot access files.")
        else:
            # Read file
            try:
                ret = self.ftp.retrlines(f"RETR {file_path}", file_content.append)
            except:
                self.logger.error(f"Failed to get file: <{file_path}>.")

        return file_content

    def download_logs(self, save_dir: str) -> list:
        logs = dict()
        logs["generic_app"] = None
        logs["messages"] = None
        logs["dmesg"] = None

        file_downloaded = list()

        if not self.ftp_ready:
            self.logger.error("The ESI FTP server has not been connected yet, cannot access the logs.")
        else:
            # Get list of all i2c logs
            try:
                i2c_log_list = self.ftp.nlst(f"{self.__log_i2c_dir__}/*iic_log.txt")
            except Exception:
                i2c_log_list = []

            if not i2c_log_list:
                self.logger.warning(f"No I2C log can be found from the ESI FTP server.")
            else:
                for each_log in i2c_log_list:
                    # Get file's name
                    log_name = Path(each_log).stem
                    logs[log_name] = each_log

            log_dir = Path(save_dir).resolve()
            if not log_dir.is_dir():
                self.logger.error(f"The saving directory is not valid: <{log_dir}>, cannot download logs.")
            else:
                tag_ts = datetime.now().strftime("%y%m%d%H%M%S")
                for log_key in logs:
                    log_file = log_dir / f"{self.ftp_id}_{log_key}_{tag_ts}.log"
                    if "_iic_log" not in log_key:
                        file_to_download = getattr(self, f"__log_{log_key}__")
                    else:
                        file_to_download = logs[log_key]
                    try:
                        logs[log_key] = self.download_binary(file_to_download, log_file)
                        self.logger.info(f"Log: <{log_key}> has been downloaded.")
                        file_downloaded.append(log_key)
                    except Exception as exc:
                        self.logger.info(f"Failed to download Log <{log_key}>: {exc}")

        return file_downloaded

    def download_binary(self, remote_file, local_file):
        local_file = Path(local_file)
        remote_file = Path(remote_file)

        if str(remote_file) not in self.ftp.nlst(str(remote_file.parent)):
            raise FileNotFoundError(remote_file)

        with local_file.open("wb") as f:
            return self.ftp.retrbinary(f"RETR {remote_file}", f.write)

    def check_gptp_status(self) -> dict:
        gptp_status = {
            "master": None,
            "error_rms": list()
        }

        if not self.ftp_ready:
            self.logger.error("The ESI FTP server has not been connected yet, cannot check its status.")
        else:
            msg = self.get_file(self.__log_messages__)

            idx = len(msg)
            if idx > 0:
                while idx >= 1:
                    msg_line = msg[idx-1]
                    error_rms_len = len(gptp_status["error_rms"])
                    if error_rms_len < 10:
                        ret = self.pattern_ptp_report.search(msg_line)
                        if ret is not None:
                            # This is the line reporting ptp error
                            gptp_status["error_rms"].append(ret.group(1))
                            
                            idx -= 1
                            continue
                    
                    if gptp_status["master"] is None:
                        ret = self.pattern_master_select.search(msg_line)
                        if ret is not None:
                            # This is the line reporting ptp master selection
                            gptp_status["master"] = ret.group(1)
                            print(f"The current gPTP master is: {gptp_status['master']}")
                            '''
                            Note:
                            It is possible that in the log file the master selection cannot be found,
                            if the ESI units have continously run for long time AND SCLX has not unloaded
                            the application. The message for master selection is then flushed by others.
                            '''
                            idx -= 1
                            continue
                    
                    if (error_rms_len >= 10) and (gptp_status["master"] is not None):
                        break
                    else:
                        idx -= 1
                        continue

        if error_rms_len > 0:
            self.logger.info(f"The recent 10 RMS error of ptp timer are: {gptp_status['error_rms']}")

        return gptp_status

    def get_fw_info(self) -> list:
        fw_info = list()
        if not self.ftp_ready:
            self.logger.error("The ESI FTP server has not been connected yet, cannot get the firmware information.")
        else:
            log = self.get_file(self.__log_generic_app__)

            # Find the start line of ESI firmware version
            start_line = None
            for idx, each_log in enumerate(log):
                if "esi firmware version" in each_log.lower():
                    start_line = idx + 1
                    break
            
            if start_line is not None:
                offset = 0
                while offset <= 3:
                    fw_info.append(log[start_line+offset])
                    self.logger.info(f"{fw_info[-1]}")
                    offset += 1

            return fw_info

## Main
if __name__ == "__main__":
    # Initialize a logger
    logger = logging.getLogger("ESI_FTP_Client") 
    logger.setLevel(logging.DEBUG)

    logFormatter = logging.Formatter("%(name)s | %(levelname)s : %(message)s")

    logStreamer = logging.StreamHandler()
    logStreamer.setLevel(logging.DEBUG)
    logStreamer.setFormatter(logFormatter)

    logger.addHandler(logStreamer)

    # Create ESI unit info
    ESIU1 = {
        "id": "ESI1",
        "addr": "192.168.141.21",
        "usr": "root",
        "passwd": r"100%esiu",
    }

    ESIU2 = {
        "id": "ESI2",
        "addr": "192.168.141.22",
        "usr": "root",
        "passwd": r"100%esiu",
    }

    # Instantiate the ESI XCP
    esi1 = esi_ftp_client(ESIU1, logger)
    esi2 = esi_ftp_client(ESIU2, logger)

    if esi1.ftp_ready:
        # Check gPTP status of ESI
        ptp_status = esi1.check_gptp_status()

        # Get ESI firmware
        fw_info = esi1.get_fw_info()

        # Download logs
        logs = esi1.download_logs(save_dir="./esi_logs")

        # Close the FTP
        esi1.quit()
    
    if esi2.ftp_ready:
        # Check gPTP status of ESI
        ptp_status = esi2.check_gptp_status()

        # Get ESI firmware
        fw_info = esi2.get_fw_info()

        # Download logs
        logs = esi2.download_logs(save_dir="./esi_logs")

        # Close the FTP
        esi2.quit()
