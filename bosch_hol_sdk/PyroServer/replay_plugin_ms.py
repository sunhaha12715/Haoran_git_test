import Pyro5.api

# Port on which the Pyro API is listening
PYRO_PORT = 33033
# IP of the secondary server
REMOTE_IP = "172.22.2.72"
# RTMaps Port (can be choosen freely), used for the "master/slave" mechanism in RTMaps
RTMAPS_PORT = 11113

def get_instance(*args, **kwargs):
    return BasicReplayPlugin(*args, **kwargs)

class BasicReplayPlugin():
    def __init__(self, rtmaps, logger):
        """Constructor. Receives a fully initialized RTMaps instance as an argument. The diagram is already loaded."""
        self._rtmaps = rtmaps
        self._logger = logger
        uri = f"PYRO:RTMapsService@{REMOTE_IP}:{PYRO_PORT}"
        self._remote_rtmaps = Pyro5.api.Proxy(uri) 


    def configure(self, replay_data, log_files, additional_properties=None):
        """
        Is used to configure the RTMaps diagramm and to prepare the system for the replay
        job. For example, this function can also be used to download an application to a 
        SCALEXIO system and to create an instance of the XIL API MAPort. The arguments are 
        the replay_data and the log_files properties from the job defintion. 
        The last argument is the value returned by the start function of the download plugin. 
        May be blocking, the RTMaps diagram is started right after this function returns.
        replay_data -- a dict as provided by the replay_job
        log_files -- a dict as provided by the replay_job
        additional_properties - the value returned by the start function of the start function of the DownloadPlugin
        """
        # Enable the "slave" mechanism on the secondary server
        self._remote_rtmaps.expose_as_slave(RTMAPS_PORT)
        # Enable the "master" mechanism on the local server and connect the runtime of the secondary server to this runtime
        self._rtmaps._parse("SocketDistribution.type = 1")
        self._rtmaps._parse(f"SocketDistribution.tempAddress = <<{REMOTE_IP} {RTMAPS_PORT}>>")
        self._rtmaps._parse("SocketDistribution.AddHost")
        self._rtmaps._parse("SocketDistribution.Connect")
        # maybe load the diagram from "replay_data"
        self._remote_rtmaps.load_diagram("/mnt/c/Users/CarstenMe/Desktop/MS/slave.rtd")


    def get_progress(self, replay_state):
        """
        Is used to retrieve the current progress of the replay. Is used to retrieve the current progress of the upload. 
        The result is passed to the client when calling GetCurrentJobState or GetAllJobStates. 
        The StepState.state must be set to ERROR if an error occurred or to FINISHED, if the replay 
        finished properly. 
        replay_state -- StepState as defined in the datareplay.proto. Fields progress, message and state may be set.
        """
        # Get the rtmaps console log output from the secondary server. Currently simply log them again on the main machine
        for level, message in self._remote_rtmaps.get_log():
            self._logger.log(level, message)
        self._remote_rtmaps.clear_log()

    def cleanup(self, reason):
        """	
        Is used to perform some clean up after the replay is finished or if the 
        timeout occurred. Can be used to unload applications from a SCALEXIO system 
        or to free some other resources. Can be blocking.
        reason -- StateEnum as defined in the datareplay.proto
        """
        pass

    def get_callback(self):
        """
        Optional: If defined, it must return a function that accepts two arguments or None. The function is called
        each time an RTMaps log event occurs. Must be non blocking.
        """
        def rtmaps_callback(level, message):
            pass

        return rtmaps_callback