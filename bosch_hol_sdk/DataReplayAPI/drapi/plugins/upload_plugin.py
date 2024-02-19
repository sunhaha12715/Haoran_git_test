def get_instance(*args, **kwargs):
    return BasicUploadPlugin(*args, **kwargs)

class BasicUploadPlugin():
    def __init__(self, logger):
        self._logger = logger
        pass

    def start(self, log_files):
        """
        Starts the upload function. The log_files from the ReplayJob is passed as the 
        argument. Must be non blocking, the upload should be executed in a different 
        thread or asynchronously.
        log_files -- a dict as provided by the replay_job
        """
        pass
   
    def get_progress(self, upload_state):
        """
        Is used to retrieve the current progress of the upload. The result is passed 
        to the client when calling GetCurrentJobState or GetAllJobStates. The StepState.state must 
        be set to ERROR if an error occurred or to FINISHED, if the upload finished properly. 
        Once the StepState.state is set to ERROR, the following upload step is omitted. Must be 
        non blocking during normal operation, as it is called cyclically (currently every second).
        upload_state -- StepState as defined in the datareplay.proto. Fields progress, message and state may be set.
        """
        pass

    def cleanup(self, reason):
        """
        Is used to perform basic cleanup, especially when the upload was aborted or not successful.
        Can be blocking.
        reason -- StateEnum as defined in the datareplay.proto
        """
        pass
