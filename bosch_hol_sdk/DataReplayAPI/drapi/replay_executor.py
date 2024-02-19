import faulthandler
import importlib.util
import logging, os
import traceback
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from time import sleep

import rtmaps as rtmaps_interface

import datareplay_pb2
from replay_exceptions import BaseDownloadStepError, BaseReplayStepError, BaseUploadStepError


faulthandler.enable(all_threads=False)

rtmaps_loglevel_mapping = {0: logging.INFO, 1: logging.WARNING, 2: logging.ERROR, 3: logging.DEBUG}


global save_log, rtmaps_helper, status_request_delay
class RTMapsHelper():
    global save_log
    def __init__(self) -> None:
        self._rtmaps_instance = rtmaps_interface.RTMapsAbstraction()
        self._rtmaps_logger = logging.getLogger("RTMaps")
        self._has_error = False
        self._error_list = list()
        self._callback = None

        def log_rtmaps(_, level, message):
            decoded_message = message.decode("utf-8")
            self._rtmaps_logger.log(rtmaps_loglevel_mapping[level], decoded_message)
            if level == 2:
                self._has_error = True
                self._error_list.append(decoded_message)
            # if not self._has_error and self._callback is not None:
            if self._callback is not None:
                try:
                    self._callback(level, decoded_message)
                except Exception as e:
                    self._has_error = True
                    self._error_list.append(str(e))

        self._rtmaps_instance.register_report_reader(log_rtmaps)

    def reset(self):
        self._has_error = False
        self._callback = None
        self._rtmaps_instance.shutdown()
        self._rtmaps_instance.reset()
        self._error_list.clear()
        try:
            self._rtmaps_logger.removeHandler(self.file_handler_rt)
            self.file_handler_rt.close()
        except:
            pass
        
    def set_logger_name(self, new_name):
        self._rtmaps_logger.name = new_name.split('_')[1]
        self._rtmaps_logger.setLevel(level=logging.DEBUG)
        self.file_handler_rt = logging.FileHandler(os.path.join(save_log, new_name.split(".")[0] + ".log"))
        self.file_handler_rt.setLevel(level=logging.DEBUG)
        log_format = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s: %(message)s")
        self.file_handler_rt.setFormatter(log_format)
        self._rtmaps_logger.addHandler(self.file_handler_rt)

    def set_log_level(self, level):
        self._rtmaps_logger.setLevel(level)

    def has_error(self):
        return self._has_error

    def get_rtmaps_instance(self):
        return self._rtmaps_instance

    def get_error_messages(self):
        return "\n".join(self._error_list)

    def set_callback(self, callback):
        self._callback = callback

def init_start(logpath):
    global rtmaps_helper, status_request_delay, save_log
    save_log = logpath
    # On Linux RTMaps crashes with a segfault when the RTMapsAbstraction is intantiated multiple times
    # So use one global instance here
    rtmaps_helper = RTMapsHelper()
    # The time in seconds between the calls to the get_progress of the plugins
    status_request_delay = 1.0

class ExecutorState(Enum):
    """
    The state of the executor. Indicates if the jobs where actually executed, but does not state
    if the actual job was successful. This is used to determine if the job database on disk must be 
    updated.
    """
    EXECUTED = auto()
    NOT_EXECUTED = auto()

class DataReplayException(Exception):
    __module__ = Exception.__module__
    """Raise this exception to indicate errors within the Data Replay context"""

class DataReplayPluginException(DataReplayException):
    __module__ = Exception.__module__
    """Raise this exception to indicate errors within the Data Replay Plugin context"""

def load_plugin(path, *args, **kwargs):
    """
    Loads a module and passes the args and kwargs to a get_instance function.
    This function must return a plugin object providing the necessary functions for the specific task
    (download/replay/upload)
    path -- path to the module to be loaded
    args / kwargs -- passed to the get_instance method
    """
    spec = importlib.util.spec_from_file_location("plugin", path)
    if spec is None:
        raise DataReplayPluginException(f"Module '{path}' not found")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if "get_instance" not in dir(module):
        raise DataReplayPluginException(f"Module {path} does not contain a get_instance function")
    if not callable(module.get_instance):
        raise DataReplayPluginException(f"get_instance in module {path} is not callable")
    return module.get_instance(*args, **kwargs)

class ReplayExecutor:
    global save_log
    def __init__(self, replay_synchronization, job_state=None) -> None:
        """
        Handles exection of a replay job
        replay_synchronization -- ReplaySynchronization providing Locks/Semaphore to enable parallel download/replay/upload
        """
        self._stopped = False
        if job_state is None:
            self._job_state = datareplay_pb2.JobState()
        else:
            self._job_state = job_state
        self._replay_synchronization = replay_synchronization

    def stop(self):
        self._stopped = True

    def get_state(self):
        return self._job_state

    def execute_replay(self, replay_job, addjobtime=None):
        """
        Executes a complete replay_job including download, replay, and upload.
        replay_job -- a ReplayJob object as defined in the datareplay.proto

        Returns a ExecutorState to indicate if the executor actually started replay
        """
        global save_log
        self._logger = logging.getLogger(replay_job.job_name)
        self._logger.setLevel(level=logging.DEBUG)
        self.measurement_time = datetime.now().strftime('%Y%m%d%H%M%S')
        self.file_handler_ex = logging.FileHandler(os.path.join(save_log, self.measurement_time + '_' +replay_job.job_name + ".log"))
        self.file_handler_ex.setLevel(level=logging.DEBUG)
        log_format = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s: %(message)s")
        self.file_handler_ex.setFormatter(log_format)
        self._logger.addHandler(self.file_handler_ex)

        if self._job_state.overall_state != datareplay_pb2.PENDING:
            self._logger.debug(f"Job {replay_job.job_name} is not in state PENDING")
            return ExecutorState.NOT_EXECUTED
        
        self._logger.info(f"Add job time is {addjobtime}")
        self._logger.info(f"Executing replay for job {replay_job.job_name}, skipping job")
        self._job_state.overall_state = datareplay_pb2.RUNNING

        # Check if the RTMaps diagram is available
        if not Path(replay_job.rtmaps_diagram).exists():
            self._job_state.overall_state = datareplay_pb2.ERROR
            self._job_state.replay_state.state = datareplay_pb2.ERROR
            self._job_state.replay_state.message = "Diagram not found"
            self._logger.error(f"RTMaps diagram {replay_job.rtmaps_diagram} of job {replay_job.job_name} not found")
            return ExecutorState.EXECUTED
        
        # As the ThreadPoolExecutor will launch the next job if some threads are waiting for a mutex, 
        # use a semaphore to ensure only a certain number of threads is executing this function
        # simultanously
        with self._replay_synchronization.overall:
            if self._job_state.overall_state == datareplay_pb2.ABORTED:
                return ExecutorState.NOT_EXECUTED

            # 1. Download
            with self._replay_synchronization.download:
                if self._stopped:
                    # The overall state remains "PENDING" here, because it did not start executing
                    self._logger.info(f"Skipping complete job {replay_job.job_name}")
                    return ExecutorState.NOT_EXECUTED

                if replay_job.execute_download:
                    self._logger.info(f"Performing download for job {replay_job.job_name}")
                    try:
                        download_data = self._download(replay_job, self._job_state.download_state)
                    except Exception as e:
                        self._logger.error(f"Exception occurred during step download: {type(e).__name__}: {e}", exc_info=True)
                        self._job_state.download_state.state = datareplay_pb2.ERROR
                        self._job_state.download_state.message = f"Exception: {type(e).__name__}: {e}: {traceback.format_exc()}"

                else:
                    self._logger.info(f"Skipping download for job {replay_job.job_name}")
                    self._job_state.download_state.state = datareplay_pb2.NOT_EXECUTED
                    download_data = None

                if self._job_state.download_state.state == datareplay_pb2.ERROR:
                    self._job_state.overall_state = datareplay_pb2.ERROR
                    self._logger.error(f"Error occurred in download of job {replay_job.job_name}")
                    return ExecutorState.EXECUTED
            
            # 2. Replay
            with self._replay_synchronization.replay:
                if self._stopped:
                    self._job_state.overall_state = datareplay_pb2.ABORTED
                    self._logger.info(f"Skipping replay and upload for job {replay_job.job_name}")
                    return ExecutorState.EXECUTED

                self._logger.info(f"Performing replay for job {replay_job.job_name}")
                try:
                    self._replay(replay_job, self._job_state.replay_state, download_data)
                except Exception as e:
                    self._logger.error(f"Exception occurred during step replay: {type(e).__name__}: {e}", exc_info=True)
                    self._job_state.replay_state.state = datareplay_pb2.ERROR
                    self._job_state.replay_state.message = f"Exception: {type(e).__name__}: {e}: {traceback.format_exc()}"
                    rtmaps_helper.reset()
                
                self._logger.info(f"Replay finished with state {datareplay_pb2._STATEENUM.values_by_number[self._job_state.replay_state.state].name}")
                if self._job_state.replay_state.state == datareplay_pb2.ERROR:
                    self._job_state.overall_state = datareplay_pb2.ERROR
                    self._logger.error(f"Error occurred in replay of job {replay_job.job_name}")
                    return ExecutorState.EXECUTED

            # 3. Upload
            with self._replay_synchronization.upload:
                if self._stopped:
                    self._job_state.overall_state = datareplay_pb2.ABORTED
                    self._logger.info(f"Skipping upload for job {replay_job.job_name}")
                    return ExecutorState.EXECUTED

                if replay_job.execute_upload:
                    self._logger.info(f"Performing upload for job {replay_job.job_name}")
                    try:
                        self._upload(replay_job, self._job_state.upload_state)
                    except Exception as e:
                        self._logger.error(f"Exception occurred during step upload: {type(e).__name__}: {e}", exc_info=True)
                        self._job_state.upload_state.state = datareplay_pb2.ERROR
                        self._job_state.upload_state.message = f"Exception: {type(e).__name__}: {e}: {traceback.format_exc()}"
                else:
                    self._logger.info(f"Skipping upload for job {replay_job.job_name}")
                    self._job_state.upload_state.state = datareplay_pb2.NOT_EXECUTED

                if self._job_state.upload_state.state == datareplay_pb2.ERROR:
                    self._job_state.overall_state = datareplay_pb2.ERROR
                    self._logger.error(f"Error occurred in upload of job {replay_job.job_name}")
                    return ExecutorState.EXECUTED

            if self._job_state.upload_state.state == datareplay_pb2.TIMEOUT \
                    or self._job_state.download_state.state == datareplay_pb2.TIMEOUT \
                    or self._job_state.replay_state.state == datareplay_pb2.TIMEOUT:
                self._job_state.overall_state = datareplay_pb2.TIMEOUT
            else:
                self._job_state.overall_state = datareplay_pb2.FINISHED
            self._logger.debug(f"Replay finished with state {self._job_state.overall_state}")

            self._logger.removeHandler(self.file_handler_ex)
            self.file_handler_ex.close()


    def _download(self, job, download_state):
        """
        Executes a single download job.
        job -- a ReplayJob object as defined in the datareplay.proto
        download_state -- a StepState object as defined in the datareplay.proto
        """
        download_plugin = load_plugin(job.download_plugin, self._logger.getChild("DownloadPlugin"))
        try:
            data = download_plugin.start(job.replay_data)
            download_state.state = datareplay_pb2.RUNNING
            download_start_time = datetime.now()
            timeout = int(job.replay_data['download_timeout'])
            while download_state.state == datareplay_pb2.RUNNING:
                sleep(status_request_delay)
                download_plugin.get_progress(download_state)
                download_state.elapsed_time = (datetime.now() - download_start_time).total_seconds()
                if timeout > 0 and download_state.elapsed_time > timeout:
                    download_state.state = datareplay_pb2.TIMEOUT
                    break
                if self._stopped:
                    download_state.state = datareplay_pb2.ABORTED
                    break
            return data
        except BaseDownloadStepError as e:
            self._logger.exception(f"Download job ended with error {e.step_state}: {e.step_message}")
            download_state.state = e.step_state
            download_state.message = e.step_message
        except Exception as e:
            self._logger.exception("Download job ended with an unknown exception")
            if download_state.state in [datareplay_pb2.PENDING, datareplay_pb2.RUNNING]:
                download_state.state = datareplay_pb2.ERROR
            if not download_state.message:
                download_state.message = str(e)
        finally:
            download_plugin.cleanup(download_state.state)

    def _get_default_plugin(self):
        try:
            from dspace.bosch_hol_sdk import replay_plugin
            self._logger.info(f'Default plugin found at: {replay_plugin.__file__}')
            return replay_plugin.__file__
        except ImportError:
            raise FileNotFoundError('No default installed replay-plugin found')

    def _replay(self, job, replay_state, download_data):
        """
        Executes a single replay job.
        job -- a ReplayJob object as defined in the datareplay.proto
        replay_state -- a StepState object as defined in the datareplay.proto
        download_data -- the value returned by the start function of the download plugin.
        """
        rtmaps_helper.reset()
        rtmaps_helper.set_logger_name(self.measurement_time + '_' + job.job_name + ".RTMaps")
        plugin_path = job.replay_plugin or self._get_default_plugin()
        self._logger.info(f'Loading replay plugin from: {plugin_path}')
        replay_plugin = load_plugin(plugin_path, rtmaps_helper.get_rtmaps_instance(), self._logger.getChild("ReplayPlugin"))
        try:
            callback = replay_plugin.get_callback()
            rtmaps_helper.set_callback(callback)
        except AttributeError:
            self._logger.debug("Found no RTMaps callback")
        self._logger.debug("Loaded replay plugin")
        rtmaps_helper.get_rtmaps_instance().load_diagram(job.rtmaps_diagram)
        self._logger.debug("Loaded RTMaps diagram")
        try:
            replay_plugin.configure(job.replay_data, job.log_files, download_data)
            self._logger.debug("RTMaps diagram configured")
            diagram_start_time = datetime.now()
            rtmaps_helper.get_rtmaps_instance().run()
            self._logger.debug("RTMaps diagram started")
            replay_state.state = datareplay_pb2.RUNNING
            while replay_state.state == datareplay_pb2.RUNNING:
                sleep(status_request_delay)
                replay_plugin.get_progress(replay_state)
                #replay_state.elapsed_time = (datetime.now() - diagram_start_time).total_seconds()
                if job.timeout > 0 and replay_state.elapsed_time > job.timeout:
                    replay_state.state = datareplay_pb2.TIMEOUT
                    break
                if self._stopped:
                    replay_state.state = datareplay_pb2.ABORTED
                    break
                # Commented out because we implement a retry mechanism in
                # configure() and we already handle RTMaps errors.
                """
                if rtmaps_helper.has_error():
                    replay_state.state = datareplay_pb2.ERROR
                    replay_state.message = "Error in RTMaps diagram: " + rtmaps_helper.get_error_messages()
                    break
                """
        except BaseReplayStepError as e:
            self._logger.exception(f"Replay job ended with error {e.step_state}: {e.step_message}")
            replay_state.state = e.step_state
            replay_state.message = e.step_message
        except Exception as e:
            self._logger.exception("Replay job ended with an unknown exception")
            if replay_state.state in [datareplay_pb2.PENDING, datareplay_pb2.RUNNING]:
                replay_state.state = datareplay_pb2.ERROR
            if not replay_state.message:
                replay_state.message = str(e)
        finally:
            rtmaps_helper.reset()
            replay_plugin.cleanup(replay_state.state, job.replay_data)

    def _upload(self, job, upload_state):
        """
        Executes a single upload job.
        job -- a ReplayJob object as defined in the datareplay.proto
        upload_state -- a StepState object as defined in the datareplay.proto
        """
        upload_plugin = load_plugin(job.upload_plugin, self._logger.getChild("UploadPlugin"))
        try:
            upload_plugin.start(job.log_files)
            upload_state.state = datareplay_pb2.RUNNING
            upload_start_time = datetime.now()
            timeout = int(job.replay_data['upload_timeout'])
            while upload_state.state == datareplay_pb2.RUNNING:
                sleep(status_request_delay)
                upload_plugin.get_progress(upload_state)
                upload_state.elapsed_time = (datetime.now() - upload_start_time).total_seconds()
                if timeout > 0 and upload_state.elapsed_time > timeout:
                    upload_state.state = datareplay_pb2.TIMEOUT
                    break
                if self._stopped:
                    upload_state.state = datareplay_pb2.ABORTED
                    break
        except BaseUploadStepError as e:
            self._logger.exception(f"Upload job ended with error {e.step_state}: {e.step_message}")
            upload_state.state = e.step_state
            upload_state.message = e.step_message
        except Exception as e:
            self._logger.exception("Upload job ended with an unknown exception")
            if upload_state.state in [datareplay_pb2.PENDING, datareplay_pb2.RUNNING]:
                upload_state.state = datareplay_pb2.ERROR
            if not upload_state.message:
                upload_state.message = str(e)
        finally:
            upload_plugin.cleanup(upload_state.state)
