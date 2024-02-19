import dataclasses
import pathlib
import types
import typing

from .manipulationconfig import _ReplayManipulationUnit
from . import manipulationutils


PathLikeType = typing.Union[pathlib.PurePosixPath, str]
DEFAULT_CMDLOADER_PATH = pathlib.PurePosixPath(
    r'/opt/dspace/xilapi.net4.0/MAPort/Main/bin/CmdLoader'
)


@dataclasses.dataclass
class ReplayTimeConfiguration:
    # The start time in microseconds.
    start: int
    # The end time in microseconds.
    end: int

    def __post_init__(self):
        if self.start < 0:
            raise ValueError(
                f'Negative start times are not allowed: {self.start}'
            )
        if self.end <= 0:
            raise ValueError(
                f'Zero or negaitve end times are not allowed: {self.end}'
            )
        if self.end <= self.start:
            raise ValueError(
                'The end time must be bigger than the start time: '
                f'{self.end} <= {self.start}'
            )


@dataclasses.dataclass
class ReplayJobConfiguration:
    # The replay job name.
    name: str
    # The master RTMaps diagram on PC1.
    pc1_diagram: PathLikeType
    # The slave RTMaps diagram on PC2.
    pc2_diagram: PathLikeType
    # The replay data on PC1.
    pc1_replay_data: PathLikeType
    # The replay data on PC2.
    pc2_replay_data: PathLikeType
    # Path to the sdf file of ControlDesk.
    sdf_file: PathLikeType
    # Path to the download-plugin. if None, the download step is skipped.
    download_plugin: typing.Optional[PathLikeType] = None
    # Path to the replay-plugin.
    replay_plugin: typing.Optional[PathLikeType] = None
    # Path to the upload-plugin. if None, the upload step is skipped.
    upload_plugin: typing.Optional[PathLikeType] = None
    # The platform name as defined in RecentHardware.xml
    platform: str = 'SCALEXIO_2'
    # Path of cmdloader executable on PC1.
    cmdloader: PathLikeType = DEFAULT_CMDLOADER_PATH
    # If not None, the time-configuration applied to the replay data.
    time_config: typing.Optional[ReplayTimeConfiguration] = None
    # If not empty, the replay-data manipulation to be performed.
    data_manipulation: list[_ReplayManipulationUnit] = dataclasses.field(
        default_factory=list,
    )
    # The timeout for the download step of this job.
    download_timeout: int = 3600
    # The timeout for the replay step of this job.
    replay_timeout: int = 3600
    # The timeout for the upload step of this job.
    upload_timeout: int = 3600
    # The timeout for the replay data (frame counters).
    replay_data_timeout: int = 20
    # Switch for debugging mode to collect more data during replay.
    debug: bool = False

    def create_replay_job(
        self,
        datareplay_pb2: types.ModuleType,
    ) -> 'datareplay_pb2.ReplayJob':  # noqa: F821 we can't import here.
        """
        Create a gRPC ReplayJob from this configuration.

        params:
            * datareplay_pb2: the datareplay_pb2 module used for the gRPC
                              communication.
        """
        job = datareplay_pb2.ReplayJob()
        job.job_name = self.name

        job.rtmaps_diagram = str(self.pc1_diagram)
        job.replay_data['slave_diagram'] = str(self.pc2_diagram)

        job.replay_data['path1'] = str(self.pc1_replay_data)
        job.replay_data['path2'] = str(self.pc2_replay_data)

        replay_plugin = self.replay_plugin or ''
        job.replay_plugin = str(replay_plugin)
        job.execute_download = bool(self.download_plugin)
        job.download_plugin = str(self.download_plugin)
        job.execute_upload = bool(self.upload_plugin)
        job.upload_plugin = str(self.upload_plugin)
        job.timeout = self.replay_timeout

        job.replay_data['sdf_path'] = str(self.sdf_file)
        job.replay_data['platform_name'] = self.platform
        job.replay_data['CMDLOADER'] = str(self.cmdloader)
        job.replay_data['download_timeout'] = str(self.download_timeout)
        job.replay_data['upload_timeout'] = str(self.upload_timeout)
        job.replay_data['replay_data_timeout'] = str(self.replay_data_timeout)

        job.replay_data['replay_time_flag'] = str(bool(self.time_config))
        if self.time_config:
            job.replay_data['replay_start_time'] = str(self.time_config.start)
            job.replay_data['replay_end_time'] = str(self.time_config.end)

        job.replay_data['debug'] = str(self.debug)
        # Data manipulation
        manipulationutils.check_all(self.data_manipulation)
        job.replay_data['data_manipulation'] = manipulationutils.serialize(
            self.data_manipulation,
        )
        return job
