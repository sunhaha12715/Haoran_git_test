__author__ = 'dSPACE'
__version__ = '1.7.10.post1'
__all__ = [
    'datareplay_pb2', 'datareplay_pb2_grpc',
    'ReplayManipulationType', 'ReplayManipulationStep', 'CanManipulationUnit',
    'LidarManipulationUnit', 'SomeIpManipulationUnit',
    'CameraManipulationUnit',
    'ReplayTimeConfiguration', 'ReplayJobConfiguration',
    'ReplayManipulationWithoutStepsError', 'UnsupportedManipulationTypeError',
    'get_replay_device', 'ReplayDevice',
]

from .configuration import (
    ReplayManipulationType, ReplayManipulationStep, CanManipulationUnit,
    LidarManipulationUnit, SomeIpManipulationUnit,
    CameraManipulationUnit,
    ReplayTimeConfiguration, ReplayJobConfiguration,
    ReplayManipulationWithoutStepsError, UnsupportedManipulationTypeError,
)
from .replaydevicecontrol import get_replay_device, ReplayDevice
from .version_management import dissect_version

# Temporary hack to make importing the grpc-files possible :(
import site
import pathlib
site.addsitedir(pathlib.Path(__file__).parent / 'DataReplayAPI' / 'drapi')
from .DataReplayAPI.drapi import datareplay_pb2, datareplay_pb2_grpc


site.addsitedir(pathlib.Path(__file__).parent / 'DrapiServices')
from .DrapiServices import DrapiServices_pb2
from .DrapiServices import DrapiServices_pb2_grpc


def get_version_tuple():
    try:
        version = dissect_version(__version__)
        return tuple(map(int, version.group('release').split('.')))
    except Exception:
        return (0, 0, 0)
