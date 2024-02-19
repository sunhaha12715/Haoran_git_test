## The script implements a HTTP client to control the NetIO device via JSON over HTTP
# by XiangL., DSC, 20230220

## Dependencies
import dataclasses
import enum
import logging
import requests
import typing

## Global Variables


class NetioState(enum.IntEnum):
    unknown = -1
    off = 0
    on = 1
    off_delay = 2
    on_delay = 3
    toggle = 4
    no_change = 5
    ignored = 6


@dataclasses.dataclass
class NetioSocket:
    """ High-level Abstraction of one or more netio-device power sockets. """
    netio_device: "netio_powerdin_4pz_ctrl"
    ids: typing.Union[int, list[int]]

    def __post_init__(self):
        if isinstance(self.ids, int):
            self.ids = [self.ids]

        if any(map(lambda x: not 1 <= x <= 4, self.ids)):
            raise ValueError(self.id)

        self._netio_outputs = [netio_output(id=x) for x in self.ids]

    def turn_on(self):
        return self.netio_device.control_output(
            action_set=[out.set_action(NetioState.on) for out in self._netio_outputs]
        )

    def turn_off(self):
        return self.netio_device.control_output(
            action_set=[out.set_action(NetioState.off) for out in self._netio_outputs]
        )

    @property
    def state(self) -> NetioState:
        states = set(self.separate_states.values())
        if len(states) == 1:
            return NetioState(states.pop())
        return NetioState.unknown

    @property
    def separate_states(self) -> dict[int, NetioState]:
        states = {x: NetioState.unknown for x in self.ids}

        complete_status = self.netio_device.get_status()
        if complete_status is None:
            return states
        try:
            outputs = complete_status["Outputs"]
            for socket in outputs:
                if socket["ID"] in self.ids:
                    state = socket["State"]
                    try:
                        states[socket["ID"]] = NetioState(state)
                    except ValueError:
                        logging.error(f"Unknown netio state recieved: {state}")
        except KeyError:
            logging.error("unexpected status format from the Netio device")
            logging.debug(f"{complete_status}")

        return states


## Classes
class netio_output:
    OFF         = NetioState.off
    ON          = NetioState.on
    OFF_DELAY   = NetioState.off_delay
    ON_DELAY    = NetioState.on_delay
    TOGGLE      = NetioState.toggle
    NO_CHANGE   = NetioState.no_change
    IGNORED     = NetioState.ignored

    def __init__(self, id) -> None:
        self._id = id

    def set_action(self, action: NetioState, delay: int = -1) -> dict:
        if action not in NetioState or action == NetioState.unknown:
            logging.warning(f"## NetIO control: <{action}> is not defined for action of NetIO API, the command is ignored.")
            return None

        out_dict = {
            "ID": self._id,
            "Action": action,
        }
        if delay >= 0:
            out_dict["Delay"] = delay

        return out_dict


class netio_powerdin_4pz_ctrl:
    def __init__(self, ip, port, uname="netio", pwd="netio") -> None:
        self.url = f"http://{ip}:{port}/netio.json"    # Enable when working with real device
        #self.url = f"http://{ip}:{port}"              # For self-testing only
        self.header = {"Content-Type": "application/json"}
        self.auth = (uname, pwd)
    
    def status_code_to_text(self, status_code) -> str:
        if status_code == 200:
            return "Request successful."
        elif status_code == 400:
            return "Invalid syntax in the control command."
        elif status_code == 401:
            return "Invalid Username or Password."
        elif status_code == 403:
            return "The device is in read-only mode."
        elif status_code == 500:
            return "The internal server of the device is not ready, please try later."
        else:
            return "The status code <{status_code}> is unknown."

    def control_output(self, action_set: list) -> None:
        # Check content of action set
        action_set_pass = True
        for action in action_set:
            try:
                if not 1 <= action["ID"] <= 4:
                    logging.error(f"## The ID field within the action set is not correct, cannot send the command.")
                    action_set_pass = False
            except:
                logging.exception(f"## The action set is not correct, cannot send the command.")
                action_set_pass = False
        
        if action_set_pass:
            # Create JSON data to be post
            json_payload = {
                "Outputs": action_set,
            }
            
            # Send the JSON data
            response = requests.post(self.url, json=json_payload, headers=self.header, auth=self.auth)
            if response.status_code == 200:
                logging.debug(f"## Successfully apply the output control on the NetIO device at {self.url}.")
            else:
                action_set_pass = False
                logging.error(f"## Failed to apply the output control with the data:")
                logging.error(self.status_code_to_text(response.status_code))
                logging.debug(f"{json_payload}")

        return action_set_pass

    def get_status(self) -> dict:
        # Read the JSON data
        response = requests.get(self.url)

        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"## Failed to inquiry status of the device.")
            logging.error(self.status_code_to_text(response.status_code))
            return None


## Main
if __name__ == "__main__":

    ## User Configuration
    server_config = {"ip": "192.168.140.50",
                     "port": "80"}

    ## Initialization
    # Create instances for output channels of NetIO
    netio_out_ch_1 = netio_output(id=3)
    netio_out_ch_2 = netio_output(id=4)

    # Create instances for NetIO device
    netio_dev = netio_powerdin_4pz_ctrl(ip=server_config["ip"], port=server_config["port"])
    for x in range(4):
        socket = NetioSocket(netio_dev, x+1)
        print(socket.id, socket.state)
    
    ## Get status of the NetIO device
    ret = netio_dev.get_status()
    output_status = ret["Outputs"]
    print(f"## The current outputs' status: ")
    print(f"# {output_status}")

    ## Set desired action to the outputs
    action_set = list()
    action_set.append(netio_out_ch_1.set_action(netio_output.TOGGLE, delay=1000)) # 只需要output.on 或者output.off
    action_set.append(netio_out_ch_2.set_action(netio_output.TOGGLE))

    ## Apply action to the NetIO device
    netio_dev.control_output(action_set=action_set)

    
