def get_instance(*args, **kwargs):
    return BasicReplayPlugin(*args, **kwargs)

class BasicReplayPlugin():
    def __init__(self, rtmaps, logger):
        """Constructor. Receives a fully initialized RTMaps instance as an argument. The diagram is already loaded."""
        self._rtmaps = rtmaps
        self._logger = logger


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
        pass

    def get_progress(self, replay_state):
        """
        Is used to retrieve the current progress of the replay. Is used to retrieve the current progress of the upload. 
        The result is passed to the client when calling GetCurrentJobState or GetAllJobStates. 
        The StepState.state must be set to ERROR if an error occurred or to FINISHED, if the replay 
        finished properly. 
        replay_state -- StepState as defined in the datareplay.proto. Fields progress, message and state may be set.
        """
        pass

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