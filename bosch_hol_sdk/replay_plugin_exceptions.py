from pathlib import Path
import site

cwd = Path(__file__).resolve().parent
site.addsitedir(cwd / "DataReplayAPI" / "drapi")

from replay_exceptions import BaseReplayStepError
import datareplay_pb2


class RTAppDownloadError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'Failed to download the real-time application on the HIL.'
            ),
            **kwargs,
        )


class RTAppUnloadError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'Failed to unload the real-time application from the HIL.'
            ),
            **kwargs,
        )


class XilApiError(BaseReplayStepError):
    def __init__(self, error_msg, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=f'XIL API access failed: {error_msg}',
            **kwargs,
        )


class ESIError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'An ESI error occured. The ESI units must be restarted.'
            ),
            **kwargs,
        )


class ESIRestartTimeoutError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.TIMEOUT,
            step_message=(
                'Failed to connect to the ESI units after restarting.'
            ),
            **kwargs,
        )


class ECURestartedError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'The ECU was restarted. '
                'The test results might not be reliable.'
            ),
            **kwargs,
        )


class RTMapsError(BaseReplayStepError):
    def __init__(self, error_msg, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=f'RTMaps reported an error: {error_msg}',
            **kwargs,
        )


class ReplayFrozenError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.TIMEOUT,
            step_message=(
                'The replay data counters are frozen. '
                'Please check the connections.'
            ),
            **kwargs,
        )


class BadManipulationConfigurationError(BaseReplayStepError):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'Failed to parse and apply the data manipulation. '
                'Please check the configuration.'
            ),
            **kwargs,
        )


class IncompatibleRTAppsError(BaseReplayStepError):
    def __init__(self, version1, version2, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                'The Real-time applications have incompatible versions with'
                f'each other: {version1} <> {version2}'
            ),
            **kwargs,
        )


class IncompatibleRTAppApiError(BaseReplayStepError):
    def __init__(self, api_version, rtapp_version, rtpc, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=(
                f'The Real-time applications on RTPC{rtpc} has an incompatible'
                f' version with the Replay API: each other: RTPC{rtpc}='
                f'{rtapp_version} <> API={api_version}'
            ),
            **kwargs,
        )


class RealtimeApplicationError(BaseReplayStepError):
    def __init__(self, error_msg, *args, **kwargs):
        super().__init__(
            *args,
            step_state=datareplay_pb2.ERROR,
            step_message=f'Error in the realtime application: {error_msg}',
            **kwargs,
        )
