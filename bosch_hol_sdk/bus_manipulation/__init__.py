from .bus_manipulator import CanManipulator
from .lidar_manipulator import LidarManipulator
from .someip_manipulator import SomeIpManipulator
from .camera_manipulator import CameraManipulator

from ..configuration import (
    LidarManipulationUnit, SomeIpManipulationUnit, CanManipulationUnit,
    CameraManipulationUnit,
)

from .manipulationbase import BadXilPathError
from .manipulationbase import XilManipulationBase


def DataManipulation(*, configuration, **kwargs):
    if isinstance(configuration, CanManipulationUnit):
        return CanManipulator(configuration=configuration, **kwargs)
    elif isinstance(configuration, LidarManipulationUnit):
        return LidarManipulator(configuration=configuration, **kwargs)
    elif isinstance(configuration, SomeIpManipulationUnit):
        return SomeIpManipulator(configuration=configuration, **kwargs)
    elif isinstance(configuration, CameraManipulationUnit):
        return CameraManipulator(configuration=configuration, **kwargs)
    else:
        raise TypeError(configuration)


def reload_xil_paths(maport):
    XilManipulationBase.reload_known_paths(maport)
