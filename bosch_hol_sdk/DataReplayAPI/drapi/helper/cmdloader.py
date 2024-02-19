import subprocess
import logging

CMDLOADER = '/opt/dspace/xilapi.net4.0/MAPort/Main/bin/CmdLoader'

class CmdLoader:
    """
    Helper class to integrate the dSPACE CmdLoader in a python environment. Is used to load and unload applications
    to a dSPACE real time system.
    """
    def __init__(self, platform : str, logger=None) -> None:
        """
        Creates a new CmdLoader instance. 

        platform - The platform to which the application is loaded onto. Note: Needs to be known to the CmdLoader, it must already be registered.
        logger (optional) - a logger instance. If none provided, one will be created automatically.
        """
        self._platform = platform
        if logger is None:
            self._logger = logging.getLogger("CmdLoader")
        else:
            self._logger = logger

    def load(self, sdf):
        """
        Loads the given application to the platform.

        sdf - path to the sdf which is loaded to the platform
        """
        result = subprocess.run([CMDLOADER, sdf, '-p', self._platform, '-ol', '2'], text=True, capture_output=True)
        if result.stderr is not None and result.stderr.strip():
            self._logger.error(result.stderr)
        if result.stdout is not None and result.stdout.strip():
            self._logger.info(result.stdout)
        self._logger.debug(f'Downloading application finished with code {result.returncode}')
        return result.returncode

    def unload(self):
        """
        Unloads the currently running from the platform
        """
        result = subprocess.run([CMDLOADER, '-unload', '-p', self._platform, '-ol', '2'], text=True, capture_output=True)
        if result.stderr is not None and result.stderr.strip():
            self._logger.error(result.stderr)
        if result.stdout is not None and result.stdout.strip():
            self._logger.info(result.stdout)
        self._logger.debug(f'Unloading application finished with code {result.returncode}')
        return result.returncode
