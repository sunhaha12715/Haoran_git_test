import dataclasses
import enum
import re


INFINITE_STEP_DURATION_VALUE = 9999.0
_STR_CHECK_RE = re.compile(r'[a-zA-Z0-9_]*')


class ReplayManipulationType(str, enum.Enum):
    """ The data-replay manipulation type. """
    # Overwrite signals (CAN and SOME/IP)
    OVERWRITE = 'overwrite'
    # Suspend the data transmission (only CAN and LIDAR).
    SUSPEND = 'suspend'
    # Manipulate the CRC field (only for CAN).
    CRC = 'crc'
    # Manipulate the rolling counter of CAN.
    ROLLING_COUNTER = 'rolling_counter'
    # Simulate the signal (only for SOME/IP).
    SIMULATE = 'simulate'


@dataclasses.dataclass(frozen=True)
class ReplayManipulationStep:
    """ The configuration for the manipulation step. """
    # The start time of this manipulation step in seconds.
    start: float
    # The duration of this manipulation step in seconds.
    duration: float = -1
    # The manipulation value used in this step.
    # Only relevant for the following manipulation types:
    #   * ReplayManipulationType.OVERWRITE
    #   * ReplayManipulationType.SIMULATE
    # It is merely ignored for the other types.
    value: float = 0

    def __post_init__(self):
        if float(self.duration) < 0:
            object.__setattr__(self, 'duration', INFINITE_STEP_DURATION_VALUE)


class _HexInt(int):
    """ Basically a normal int that has a hex representation. """
    def __repr__(self):
        return hex(self)


@dataclasses.dataclass(frozen=True)
class _ReplayManipulationUnit:
    """
    The base class for all manipulation unit. Should not be used on its own.
    """
    # The manipulation type to be performed.
    type: ReplayManipulationType = dataclasses.field(compare=False)
    # The manipulation steps for executing this manipulation configuration.
    steps: list[ReplayManipulationStep] = dataclasses.field(
        compare=False,
    )

    def __post_init__(self):
        for field in dataclasses.fields(self):
            if issubclass(field.type, str):
                value = getattr(self, field.name)
                if not _STR_CHECK_RE.fullmatch(value):
                    raise ValueError(
                        f"Bad value for field '{self.__class__.__name__}."
                        f"{field.name}': '{value}'! Only alpha-numeric and "
                        'underscore is supported!'
                    )


@dataclasses.dataclass(frozen=True)
class CanManipulationUnit(_ReplayManipulationUnit):
    """
    A data-replay manipulation configuration unit for CAN.

    Currently supported manipulation types:
        * ReplayManipulationType.OVERWRITE
        * ReplayManipulationType.SUSPEND
        * ReplayManipulationType.CRC
        * ReplayManipulationType.ROLLING_COUNTER
    """
    # The CAN channel name.
    name: str
    # The CAN frame ID.
    id: int
    # The CAN frame name.
    frame: str
    # The CAN signal name (only for ReplayManipulationType.OVERWRITE and
    # ReplayManipulationType.CRC).
    signal: str = ''

    def __post_init__(self):
        super().__post_init__()
        object.__setattr__(self, 'id', _HexInt(self.id))


@dataclasses.dataclass(frozen=True)
class LidarManipulationUnit(_ReplayManipulationUnit):
    """
    A data-replay manipulation configuration unit for the LIDAR data.

    Currently supported manipulation types:
        * ReplayManipulationType.SUSPEND
    """


@dataclasses.dataclass(frozen=True)
class SomeIpManipulationUnit(_ReplayManipulationUnit):
    """
    A data-replay manipulation configuration unit for SOME/IP.

    Currently supported manipulation types:
        * ReplayManipulationType.OVERWRITE
        * ReplayManipulationType.SUSPEND
        * ReplayManipulationType.SIMULATE
    """
    # The SOME/IP service name.
    service_name: str
    # The SOME/IP service id.
    service_id: int
    # The SOME/IP event group number.
    event_group: int
    # The SOME/IP event name.
    event: str
    # The signal name in the event.
    signal: str


@dataclasses.dataclass(frozen=True)
class CameraManipulationUnit(_ReplayManipulationUnit):
    """
    A data-replay manipulation configuration unit for cameras.

    Currently supported manipulation types:
        * ReplayManipulationType.SUSPEND
    """
    # The camera name.
    name: str
