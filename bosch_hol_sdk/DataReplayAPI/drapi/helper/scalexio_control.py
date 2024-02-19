#!/usr/bin/env python3

import os
import socket
import requests


class ScalexioControl:
    
    def __init__(self, scalexio_ip) -> None:
        self._scalexio_ip = scalexio_ip
        socket.inet_aton(self._scalexio_ip)
        self._api_url = f"http://{self._scalexio_ip}/api/v1.0"

    def load_application(self, rta_file):
        requests.post(self._api_url + "/applications", headers = {}, files = {
            "application_file": (os.path.basename(rta_file), open(rta_file, "rb"), "application/octet-stream"),
        })

    def _execute_post_operation(self, url, operation):
        data = {
            "operation": operation
        }
        requests.post(url, headers = {}, data=data)

    def stop(self):
        self._execute_post_operation(self._api_url + "/applications", "stop")
    
    def start(self):
        self._execute_post_operation(self._api_url + "/applications", "start")
    
    def unload(self):
        self._execute_post_operation(self._api_url + "/applications", "unload")

    def reboot(self):
        self._execute_post_operation(self._api_url + "/system_control", "reboot")



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--ip", help="IP of the dSPACE SCALEXIO system")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-l", "--load", help="Path to the RTA to be loaded to the SCALEXIO")
    group.add_argument("-a", "--action", choices=["start", "stop", "unload", "reboot"], help="Acion to be performed")

    args = parser.parse_args()

    scalexio_control = ScalexioControl(args.ip)
    if args.load is not None:
        scalexio_control.load_application(args.load)
    else:
        if args.action == "start":
            scalexio_control.start()
        elif args.action == "stop":
            scalexio_control.stop()
        elif args.action == "unload":
            scalexio_control.unload()
        elif args.action == "reboot":
            scalexio_control.reboot()




