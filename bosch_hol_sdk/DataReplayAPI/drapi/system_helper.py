import json
import platform
import subprocess

import datareplay_pb2

def get_volume_info_linux():
    command_str = "--output=target,used,avail,size"
    out = subprocess.check_output(["df", command_str], shell=False).decode()
    FACTOR = 1024
    response = datareplay_pb2.Volumes()

    for line in out.split("\n")[1:]:
        parsed_volume = line.split()
        if not parsed_volume: continue
        if parsed_volume[0].startswith(("/boot", "/init", "/dev", "/run", "/sys", "/usr")): continue
        volume = datareplay_pb2.Volume()
        volume.mount = parsed_volume[0]
        volume.size = int(parsed_volume[3])*FACTOR
        volume.free_space = int(parsed_volume[2])*FACTOR
        response.volumes.append(volume)
    return response

def get_volume_info_windows():
    command_str = "Get-CimInstance -ClassName Win32_LogicalDisk | Select-Object DeviceID, FreeSpace, Size | ConvertTo-Json"
    volumes = subprocess.check_output(["powershell.exe", command_str], shell=False)
    response = datareplay_pb2.Volumes()
    
    for parsed_volume in json.loads(volumes):
        if parsed_volume["Size"] is None: continue
        volume = datareplay_pb2.Volume()
        volume.mount = parsed_volume["DeviceID"]
        volume.size = parsed_volume["Size"]
        volume.free_space = parsed_volume["FreeSpace"]
        response.volumes.append(volume)
    return response


if platform.system() == "Windows":
    get_volume_info = get_volume_info_windows
else:
    get_volume_info = get_volume_info_linux

