"""
The script configure the rtmaps diagram and components ports
Requirements: Python3, Replay_pc1
by QianweiY, DSC, 2023/8/3

v1.0:
-The first demo version
-test rtmaps diagram:
      copy7-pc1: "/home/dspace/workspace/replay_test/Complete_DSU1_Chery_API.rtd"
      copy7-pc2: "/home/dspace/workspace/replay_test/Complete_DSU0_Chery_API.rtd"
"""
import logging
import os
import typing

from dspace.bosch_hol_sdk.configuration import ReplayTimeConfiguration
from dspace.bosch_hol_sdk.port_connection_config import (
    PortConnection, RTMapsPlayer, PlayerLocation,
)


class RtmapsConnection():
    def __init__(
        self,
        rtmaps_instance,
        remote_rtmaps_instance,
        valid_data_end,
        logger=None,
    ) -> None:
        if logger is None:
            self._logger = logging.getLogger("rtmaps_connection")
        else:
            self._logger = logger

        # Patch the local rtmaps with a parse method for transparency.
        rtmaps_instance.parse = rtmaps_instance._parse

        self._valid_data_end = valid_data_end
        self._fifo_size = 16
        self._rtmaps = {
            PlayerLocation.PC1: rtmaps_instance,
            PlayerLocation.PC2: remote_rtmaps_instance,
        }

    def connect_port(self, port_connection: PortConnection) -> None:
        src = f"{port_connection.player.name}.{port_connection.name}"
        dst = port_connection.destination
        pc = port_connection.player.location
        rtmaps = self._rtmaps[pc]
        self._logger.info(f'Connecting {src} to {dst} on {pc}')
        try:
            rtmaps.parse(f"{src} -> {dst}")
        except Exception:
            self._logger.exception(f"Failed to connect {src} and {dst}!")
            return

        try:
            rtmaps.parse(f"{src}.fifosize = {self._fifo_size}")
        except Exception as exc:
            self._logger.error(
                f"Failed to set the 'fifosize' property of '{src}': {exc}"
            )

        try:
            rtmaps.parse(f"{src}.replayMode = <<Immediate>>")
        except Exception as exc:
            self._logger.error(
                f"Failed to set the 'replayMode' property of '{src}': {exc}"
            )

        try:
            rtmaps.parse(f"{src}.threaded = true")
        except Exception as exc:
            self._logger.error(
                f"Failed to set the 'threaded' property of '{src}': {exc}"
            )

    def configure_diagrams(self, slave_diagram, slave_ip, slave_port):
        master_rtmaps = self._rtmaps[PlayerLocation.PC1]
        slave_rtmaps = self._rtmaps[PlayerLocation.PC2]

        # Enable the "slave" mechanism on the secondary server
        slave_rtmaps.expose_as_slave(slave_port)

        # Enable the "master" mechanism on the local server and connect the
        # runtime of the secondary server to this runtime
        master_rtmaps.parse("SocketDistribution.type = 1")
        master_rtmaps.parse(
            f"SocketDistribution.tempAddress = <<{slave_ip} {slave_port}>>"
        )
        master_rtmaps.parse("SocketDistribution.AddHost")
        master_rtmaps.parse("SocketDistribution.Connect")

        # Load slave diagram
        slave_rtmaps.load_diagram(slave_diagram)

        # Increase the RTMaps engine timeout.
        for rtmaps in self._rtmaps.values():
            rtmaps.parse("Engine.shutdownTimeout = 20000000")

    def configure_player(
        self,
        player: RTMapsPlayer,
        data_path: os.PathLike,
        replay_time: typing.Optional[ReplayTimeConfiguration],
    ) -> None:
        name = player.name
        pc = player.location
        rtmaps = self._rtmaps[pc]

        rtmaps.add_component('Player', name)

        self._logger.debug(f"Configure with dataset {data_path}")
        rtmaps.set_property(name, 'file', data_path)
        rtmaps.parse(f"{name}.timelag = -1")

        first_time = rtmaps.get_integer_property(name, 'first')  # Unit us
        self._logger.debug(f"{player}: first time: {first_time}")

        last_time = rtmaps.get_integer_property(name, 'last')  # Unit us
        self._logger.debug(f"{player}: last time: {last_time}")

        original_first_time = first_time
        original_last_time = last_time

        if replay_time:
            if replay_time.start > last_time or replay_time.end < first_time:
                # Nothing to be replayed so end the job.
                raise ValueError(
                    f'{player}: the provided replay-time configuration is '
                    f'completel out of the data range [{first_time}, '
                    f'{last_time}]: {replay_time}'
                )

            if first_time > replay_time.start or replay_time.end > last_time:
                # The given replay time does not exactly fit within the data.
                self._logger.warning(
                    f'{player}: the provided replay-time configuration exceeds'
                    f' the available data range [{first_time}, {last_time}]: '
                    f'{replay_time}'
                )

            # XXX: Feature disabled for this release
            """
            if self._valid_data_end < replay_time.end:
                self._logger.warning(
                    f'{player}: the provided replay-time configuration exceeds'
                    f' the calculated valid data range [0, '
                    f'{self._valid_data_end}]: {replay_time}'
                )
            """

            first_time = int(max(replay_time.start, first_time))
            last_time = int(min(replay_time.end, last_time))
        # XXX: Feature disabled for this release
        # last_time = int(min(self._valid_data_end, last_time))

        self._logger.info(f"{player}: Effective data begin: {first_time}")
        rtmaps.set_property(name, 'beginning', first_time)

        self._logger.info(f"{player}: Effective data end: {last_time}")
        rtmaps.set_property(name, 'end', last_time)

        return original_last_time - original_first_time
