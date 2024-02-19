import dataclasses
import enum
import operator


class PlayerLocation(str, enum.Enum):
    PC1 = 'pc1'
    PC2 = 'pc2'


class SensorType(str, enum.Enum):
    CAN = 'can'
    CAMERA = 'camera'
    LIDAR = 'lidar'


@dataclasses.dataclass(frozen=True)
class RTMapsPlayer:
    name: str
    location: PlayerLocation

    def __str__(self):
        return f'Player <{self.name}> on {self.location}'


MasterPlayer1 = RTMapsPlayer('Player_1', PlayerLocation.PC1)
SlavePlayer1 = RTMapsPlayer('Player_1', PlayerLocation.PC2)
SlavePlayer2 = RTMapsPlayer('Player_2', PlayerLocation.PC2)


@dataclasses.dataclass(frozen=True)
class PortConnection:
    player: RTMapsPlayer
    name: str
    type: SensorType
    destination: str


class PortConnectionManager:
    _port_connections = (
        # Master connections
        PortConnection(MasterPlayer1, 'SideFrontCam01', SensorType.CAMERA, 'ReplaySystemInterface.input_1'),
        PortConnection(MasterPlayer1, 'SideFrontCam02', SensorType.CAMERA, 'ReplaySystemInterface.input_2'),
        PortConnection(MasterPlayer1, 'SideRearCam01', SensorType.CAMERA, 'ReplaySystemInterface.input_3'),
        PortConnection(MasterPlayer1, 'SideRearCam02', SensorType.CAMERA, 'ReplaySystemInterface.input_4'),
        PortConnection(MasterPlayer1, 'SurCam02', SensorType.CAMERA, 'ReplaySystemInterface.input_5'),
        PortConnection(MasterPlayer1, 'SurCam04', SensorType.CAMERA, 'ReplaySystemInterface.input_6'),
        PortConnection(MasterPlayer1, 'SurCam01', SensorType.CAMERA, 'ReplaySystemInterface.input_7'),
        PortConnection(MasterPlayer1, 'SurCam03', SensorType.CAMERA, 'ReplaySystemInterface.input_8'),

        # Slave-Player1 connections
        PortConnection(SlavePlayer1, 'FrontCam02', SensorType.CAMERA, 'ReplaySystemInterface.input_1'),
        PortConnection(SlavePlayer1, 'FrontCam01', SensorType.CAMERA, 'ReplaySystemInterface.input_2'),
        PortConnection(SlavePlayer1, 'RearCam01', SensorType.CAMERA, 'ReplaySystemInterface.input_3'),
        PortConnection(SlavePlayer1, 'CornerRadar02', SensorType.CAN, 'ReplaySystemInterface.input_5'),
        PortConnection(SlavePlayer1, 'CornerRadar04', SensorType.CAN, 'ReplaySystemInterface.input_6'),
        PortConnection(SlavePlayer1, 'CornerRadar03', SensorType.CAN, 'ReplaySystemInterface.input_7'),
        PortConnection(SlavePlayer1, 'FrontRadar01', SensorType.CAN, 'ReplaySystemInterface.input_8'),
        PortConnection(SlavePlayer1, 'USS', SensorType.CAN, 'ReplaySystemInterface.input_9'),
        PortConnection(SlavePlayer1, 'rtk01', SensorType.CAN, 'ReplaySystemInterface.input_10'),
        PortConnection(SlavePlayer1, 'VCar01', SensorType.CAN, 'ReplaySystemInterface.input_11'),
        PortConnection(SlavePlayer1, 'VCar02', SensorType.CAN, 'ReplaySystemInterface.input_12'),
        PortConnection(SlavePlayer1, 'VCar03', SensorType.CAN, 'ReplaySystemInterface.input_13'),
        PortConnection(SlavePlayer1, 'CornerRadar01', SensorType.CAN, 'ReplaySystemInterface.input_14'),

        # Slave-Player2 connections
        PortConnection(SlavePlayer2, 'FrontLidar01', SensorType.LIDAR, 'Splitter_118.input'),
    )

    def get_port_connections(self):
        return self._port_connections

    def get_port_connection(self, name):
        for connection in self._port_connections:
            if connection.name == name:
                return connection

    def get_players(self):
        return tuple(set(map(
            operator.attrgetter('player'),
            self._port_connections,
        )))
