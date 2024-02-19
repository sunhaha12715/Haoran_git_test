#!/usr/bin/env python3.9
from rtmaps import RTMapsAbstraction

import argparse
import logging

import Pyro5.server

rtmaps_loglevel_mapping = {0: logging.INFO, 1: logging.WARNING, 2: logging.ERROR, 3: logging.DEBUG}

@Pyro5.server.behavior('single')
class RTMapsService(object):
    REPORT_INFO = 0
    REPORT_WARNING = 1
    REPORT_ERROR = 2

    def __init__(self) -> None:
        self._rtmaps = RTMapsAbstraction()
        self._rtmaps_logger = logging.getLogger("RTMaps")
        self._log_messages = list()
        def log_rtmaps(_, level, message):
            decoded_message = message.decode("utf-8")
            self._rtmaps_logger.log(rtmaps_loglevel_mapping[level], decoded_message)
            self._log_messages.append((rtmaps_loglevel_mapping[level], decoded_message))

        self._rtmaps.register_report_reader(log_rtmaps)

    def run(self):
        self._rtmaps_logger.info("RUN")
        return self._rtmaps.run()
    
    def shutdown(self):
        return self._rtmaps.shutdown()
    
    def reset(self):
        return self._rtmaps.reset()
    
    def restart_runtime(self):
        return self._rtmaps.restart_runtime()

    def parse(self, command):
        return self._rtmaps._parse(command)

    def register_standard_package(self, package_name, package_sub_folder=""):
        return self._rtmaps.register_standard_package(package_name, package_sub_folder)
    
    def report(self, message, level=REPORT_INFO):
        return self._rtmaps.report(message, level)
    
    def register_report_reader(self, cb):
        return self._rtmaps.register_report_reader(cb)
    
    def play(self):
        return self._rtmaps.play()
    
    def stop(self):
        return self._rtmaps.stop()
    
    def pause(self):
        return self._rtmaps.pause()
    
    def get_current_time(self):
        return self._rtmaps.get_current_time()
    
    def get_integer_property(self, name):
        return self._rtmaps.get_integer_property(name)
    
    def get_string_property(self, name):
        return self._rtmaps.get_string_property(name)
    
    def get_enum_property(self, name):
        return self._rtmaps.get_enum_property(name)
    
    def read_int32(self, name, wait_for_data):
        return self._rtmaps.read_int32(name, wait_for_data)
    
    def get_output_names_for_component(self, component):
        return self._rtmaps.get_output_names_for_component(component)
    
    def get_input_names_for_component(self, component):
        return self._rtmaps.get_input_names_for_component(component)
    
    def get_property_names_for_component(self, component):
        return self._rtmaps.get_property_names_for_component(component)
    
    def is_running(self, ):
        return self._rtmaps.is_running()
    
    def is_paused(self, ):
        return self._rtmaps.is_paused()
    
    def add_component(self, component_type: str, component_id: str, xpos = None, ypos = None, zpos = 0):
        return self._rtmaps.add_component(component_type, component_id, xpos, ypos, zpos)
    
    def remove_component(self, component_id: str):
        return self._rtmaps.remove_component(component_id)
    
    def connect_components(self, producer_id, producer_outport, consumer_id, consumer_inport):
        return self._rtmaps.connect_components(producer_id, producer_outport, consumer_id, consumer_inport)
    
    def disconnect_components(self, producer_id, producer_outport, consumer_id, consumer_inport):
        return self._rtmaps.disconnect_components(producer_id, producer_outport, consumer_id, consumer_inport)
    
    def record_signal(self, producer_id, producer_outport, recorder_id, recording_method):
        return self._rtmaps.record_signal(producer_id, producer_outport, recorder_id, recording_method)
    
    def set_property(self, component_id, property_name, value):
        return self._rtmaps.set_property(component_id, property_name, value)
    
    def set_input_property(self, component_id, input_name, property_name, value):
        return self._rtmaps.set_input_property(component_id, input_name, property_name, value)
    
    def set_output_property(self, component_id, output_name, property_name, value):
        return self._rtmaps.set_output_property(component_id, output_name, property_name, value)
    
    def print_rtm_script(self, ):
        return self._rtmaps.print_rtm_script()
    
    def write_rtm_script(self, path):
        return self._rtmaps.write_rtm_script(path)
    
    def check_outport_availability(self, component_id, outport):
        return self._rtmaps.check_outport_availability(component_id, outport)
    
    def check_inport_availability(self, component_id, inport):
        return self._rtmaps.check_inport_availability(component_id, inport)
    
    def check_property_availability(self, component_id, property_name):
        return self._rtmaps.check_property_availability(component_id, property_name)
    
    def check_input_property_availability(self, property_name):
        return self._rtmaps.check_input_property_availability(property_name)
    
    def check_output_property_availability(self, property_name):
        return self._rtmaps.check_output_property_availability(property_name)
    
    def check_component_availability(self, component_id):
        return self._rtmaps.check_component_availability(component_id)
    
    def is_enum_property(self, component_id, property_name):
        return self._rtmaps.is_enum_property(component_id, property_name)
    
    def get_valid_enum_properties(self, component_id, property_name):
        return self._rtmaps.get_valid_enum_properties(component_id, property_name)
    
    def check_enum_property_validity(self, component_id, property_name, value):
        return self._rtmaps.check_enum_property_validity(component_id, property_name, value)
    
    def get_integer_property(self, component_id, property_name):
        return self._rtmaps.get_integer_property(component_id, property_name)
    
    def get_string_property(self, component_id, property_name):
        return self._rtmaps.get_string_property(component_id, property_name)
    
    def read_int32(self, component_id, output_name, wait_for_data):
        return self._rtmaps.read_int32(component_id, output_name, wait_for_data)
    
    def load_diagram(self, diagram_path, reset=True):
        self._rtmaps_logger.info("Load diagram")
        return self._rtmaps.load_diagram(diagram_path, reset)
    
    def reset(self):
        return self._rtmaps.reset()

    def register_std_report_reader(self):
        return self._rtmaps.register_std_report_reader()

    def get_log(self):
        return self._log_messages
    
    def clear_log(self):
        self._log_messages.clear()

    def expose_as_slave(self, port):
        self._rtmaps_logger.info("Expose")
        self._rtmaps._parse(f"SocketDistribution.portUDP = {port}")
        self._rtmaps._parse(f"SocketDistribution.type = 2")


def start_server(host, port):
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s: %(name)s: %(levelname)s: %(message)s")
    logging.getLogger('Pyro5').setLevel(logging.DEBUG)
    ExposedRTMaps = Pyro5.server.expose(RTMapsService)

    logging.info(f"Starting daemon on {host}:{port}")
    daemon = Pyro5.server.Daemon(host=host, port=port)
    uri = daemon.register(ExposedRTMaps, objectId="RTMapsService")
    logging.info(str(uri))
    daemon.requestLoop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Pyro-Server for accessing RTMaps on a remote machine.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--host',
        type=str,
        help='the hostname/ip to listen on',
        default='192.168.140.102',
    )
    parser.add_argument(
        '--port',
        type=int,
        help='the port to listen on',
        default=33033,
    )
    args = parser.parse_args()
    start_server(host=args.host, port=args.port)
