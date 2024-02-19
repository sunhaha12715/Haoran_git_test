#!/usr/bin/env python3.9
'''
Author: xiaofei zhang
Date: 2023.12.14
'''
import itertools
import json
from pathlib import Path
import re
import logging
import struct

import dpkt


class IdxRecCompare():
    """Compare the file size in s8(pcap) and idx for the same sensor, output result."""

    def __init__(self, data, sensors, logger) -> None:
        """_summary_

        Args:
            data (PathLike): Path of the Record data.
            sensors (_dict_): The sensor names of the current replay job passed in as a dictionary.
            logger (Logger): instance of logger object for reporting log messages.
        """
        self.logger = logger
        self.sensors = sensors
        self.data_path = Path(data)
        self.ts_sensor = {
            sensor: (0.00, -1)
            for sensor in itertools.chain.from_iterable(sensors.values())
        }
        self.sensor_data = {}

    def data_check(self):
        """
        Returns:
            compare_result(tuple): includes each sensor name and the
                                   corresponding end time and the shortest
                                   end time among them
        """
        self.read_rec()
        regex = re.compile(r'@(.+)')
        self._directory = self.data_path.parent
        ts_match = regex.search(self.data_path.stem)
        if not ts_match:
            raise ValueError(f'Failed to extract the timestamp from filename {self.data_path}.')
        self._timestamp = ts_match.group(1)

        for key, value in self.sensors.items():
            if key in ['camera', 'lidar']:
                self.read_data_file(value)  # Lidar/Camera
            elif key == 'can':
                self.check_can_sensors(value)  # CAN
            else:
                self.logger.error(f"Unexpected key '{key}'. Ignoring.")

        if not self.ts_sensor:
            # No data available.
            return {}, 0.00

        ts_all = tuple(ts for ts, num in self.ts_sensor.values() if num > 0)
        end_time = min(ts_all)
        return (self.ts_sensor, end_time)

    def read_data_file(self, sensor_names: list):
        """Read all record data for comparison"""

        for sensor_name in sensor_names:
            self.logger.info(f"Analysis record data in the {self._directory}; sensor: {sensor_name}")
            index_file = self._directory.joinpath(f"{sensor_name}@{self._timestamp}.idx")

            try:
                sensor_name_data, _, rec_sensor_num = self.sensor_data[sensor_name]
            except KeyError:
                self.logger.warning(f"Sensor '{sensor_name}' doesn't exist in the rec-file.")
                continue

            if not index_file.exists():
                self.logger.warning(f"The idx file '{index_file}' does not exist!")
                continue

            if rec_sensor_num < 2:
                self.logger.error(f"The valid frames of rec file '{self.data_path}' is less than 2, it can't be repaly, please check the file!")
                continue

            if sensor_name == 'FrontLidar01':
                filetype = 'pcap'
            else:
                filetype = 's8'

            data_file = self._directory.joinpath(f"{sensor_name}@{self._timestamp}.{filetype}")
            if not data_file.exists():
                self.logger.warning(f"The file '{data_file}' does not exist!")
                continue

            data_offset, ts_end = self.idx_calculate(data_file, index_file, rec_sensor_num)

            if data_offset > 0:
                if ts_end is None:
                    ts_end = self._convert_timestamp(sensor_name_data[data_offset - 2])
                self.ts_sensor[sensor_name] = ts_end, data_offset - 1

    def _convert_timestamp(self, timestamp):
        ts = timestamp.split(':')
        us = 0
        for part in ts:
            us = us*60 + float(part)
        us *= 1000 * 1000
        return round(us)

    def read_rec(self):
        """ Parse the rec-file """
        record_regex = re.compile(r"(?P<ts>[:\.\d]+)\s+@ Record\s+(?P<sensor>[^\(\s]+)\((?P<output>\S+)\[(?P<output_info>[^\]]+)\]\).*")
        data_regex = re.compile(r"(?P<ts>[:\.\d]+)\s+\/\s+(?P<sensor>[^#\s]+)#\d+.*")

        with open(self.data_path, 'r', encoding='utf-8') as temp_f:
            # Create an iterator for lines in the rec file.
            data_file_iter = iter(temp_f)

            # Go to the data section.
            for line in data_file_iter:
                if line.strip() == "[Data]":
                    break

            # Initialize the data structures.
            sensor_data_dict = {}
            frame_dict = {}

            # Extract the frame sizes from the record lines.
            for line in data_file_iter:
                # Check if it's a record line.
                m = record_regex.match(line)
                if m:
                    output_info = m.group('output_info')
                    frame_size = int(output_info.split(',')[-1])
                    sensor_name = m.group('sensor')
                    frame_dict[sensor_name] = frame_size
                    continue

                # Check if it's a sample line.
                m = data_regex.match(line)
                if m:
                    ts = m.group('ts')
                    sensor_name = m.group('sensor')
                    try:
                        sensor_data_dict[sensor_name].append(ts)
                    except KeyError:
                        sensor_data_dict[sensor_name] = [ts]

            # post process the extracted data and save them in a dictionary.
            for type_sensor, data in sensor_data_dict.items():
                # sensor frame data in rec
                rec_sensor_num = len(data)
                self.sensor_data[type_sensor] = (data, frame_dict[type_sensor], rec_sensor_num)

    def check_can_sensors(self, sensor_name: list):
        """ Analyze and compare the asc and rec files of CAN """
        for sensor_can_name in sensor_name:
            self.logger.info(f"Analyzing record data in the '{self._directory}' for sensor '{sensor_can_name}'")
            asc_file = self._directory.joinpath(f"{sensor_can_name}@{self._timestamp}.asc")

            if not asc_file.exists():
                self.logger.warning(f"The asc file '{asc_file}' does not exist!")
                continue

            asc_frame_count = self.get_asc_frame_count(asc_file)

            try:
                sensor_name_data, _, rec_sensor_num = self.sensor_data[sensor_can_name]
            except KeyError:
                self.logger.warning(f"Sensor '{sensor_can_name}' doesn't exist in the rec-file.")
                continue

            if asc_frame_count != rec_sensor_num:
                self.logger.warning(
                    f"The total number of frames in the rec file '{self.data_path}'"
                    f" doesn't match that in the asc file '{asc_file}'!"
                )

            # XXX:  ASC frame count is ignored as the replay relies only on the
            #       rec-file.
            # num_frames = min(rec_sensor_num, asc_frame_count)
            num_frames = rec_sensor_num

            ts_end = self._convert_timestamp(sensor_name_data[num_frames - 2])
            self.ts_sensor[sensor_can_name] = ts_end, num_frames - 1

    def get_asc_frame_count(self, asc_file: Path):
        """ Get the number of CAN frames in an ASC file """
        with open(asc_file, 'r', encoding='utf-8') as temp_f:
            file_iterator = iter(temp_f)

            # Find the the beginning of the data.
            for line in file_iterator:
                if line == "// version 8.1.0\n":
                    break

            # Count the lines afterwards.
            asc_frame_count = 0
            for line in file_iterator:
                asc_frame_count += 1

        return asc_frame_count

    def idx_calculate(self, sensor_file: Path, index_file: Path, rec_sensor_num: int):
        """ Calculate the number of valid frames in the idx file for comparison """
        sensor_file_size = sensor_file.stat().st_size
        if sensor_file_size == 0:
            self.logger.error(f"The size of the file '{sensor_file}' is zero!")
            return -1, None

        # Calculate the frame index.
        idx, start_address, end_address = self._get_last_frame_info(
            index_file,
            sensor_file_size,
            rec_sensor_num,
        )

        if idx < 0:
            return -1, None

        if idx != rec_sensor_num:
            self.logger.warning(f'The file "{sensor_file}" is truncated!')

        timestamp = None
        if sensor_file.suffix == '.pcap':
            timestamp = self._get_pcap_timestamp(sensor_file, start_address, end_address)
        return idx, timestamp

    def _get_pcap_timestamp(self, pcap_file: Path, start_address: int, end_address: int):
        """
        Find the highest timestamp in that packet-group in the pcap file.
        """
        with open(pcap_file, 'rb') as f:
            pcap = dpkt.pcap.Reader(f)
            timestamp = 0
            pcap_i = iter(pcap)
            f.seek(start_address)
            while f.tell() < end_address:
                this_timestamp, _ = next(pcap_i)
                timestamp = max(this_timestamp, timestamp)

        # Convert form seconds to microseconds.
        timestamp *= 1000 * 1000
        return round(timestamp)

    def _get_last_frame_info(self, index_file, file_size, maximum_index):
        """
        Return the frame information from the index file that corresponds to
        the last valid frame in the sensor data file (based on its size).

        Return:
            tuple(int, int, int): the frame index, its start address, its end address.
        """
        with open(index_file, 'rb') as file:
            rounded_size = index_file.stat().st_size // 8
            start_index = min(rounded_size - 1, maximum_index)
            for idx in range(start_index, 0, -1):
                file.seek(idx * 8)
                bytes_data = file.read(8)
                end_address = struct.unpack('q', bytes_data)[0]
                if end_address <= file_size:
                    break
            else:
                self.logger.error('No valid frame found')
                return -1, 0, 0

            file.seek((idx - 1)*8)
            bytes_data = file.read(8)
            start_address = struct.unpack('q', bytes_data)[0]
        return idx, start_address, end_address


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'path',
        action='store',
        type=Path,
        help='camera sensor name',
    )
    parser.add_argument(
        '--camera',
        action='append',
        help='camera sensor name',
        default=[],
    )
    parser.add_argument(
        '--can',
        action='append',
        help='camera sensor name',
        default=[],
    )
    parser.add_argument(
        '--lidar',
        action='append',
        help='camera sensor name',
        default=[],
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='count',
        help='increase verbosity',
        default=0,
    )

    args = parser.parse_args()

    log_level = max(logging.DEBUG, logging.ERROR - args.verbose*10)
    logging.basicConfig(
        format='%(levelname)s:%(message)s',
        level=log_level,
    )

    sensors = {
        'camera': args.camera,
        'can': args.can,
        'lidar': args.lidar,
    }
    logger = logging.getLogger('IdxRecCompare')

    my_instance = IdxRecCompare(args.path, sensors, logger)
    sensor_times, ts_end = my_instance.data_check()

    output = {
        'ts_end': ts_end,
        'sensors': sensor_times,
    }

    serialized_output = json.dumps(output)
    print(serialized_output)
