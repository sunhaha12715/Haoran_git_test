def get_instance(*args, **kwargs):
    return BasicDownloadPlugin(*args, **kwargs)

class BasicDownloadPlugin():
    def __init__(self, logger):
        self._logger = logger

    def start(self, replay_data):
        """
        Starts the download function. The replay_data from the ReplayJob is passed as the 
        argument. Returns a dict which is passed later to the replay_plugin. Must be non blocking, 
        the download should be executed in a different thread or asynchronously.
        replay_data -- a dict as provided by the replay_job
        """
        return dict()

    def get_progress(self, download_state):
        """
        Is used to retrieve the current progress of the download. The result is passed 
        to the client when calling GetCurrentJobState or GetAllJobStates. The StepState.state 
        must be set to ERROR if an error occurred or to FINISHED, if the download finished properly. 
        Once the StepState.state is set to ERROR, the following steps (replay and upload) are 
        omitted. Must be non blocking during normal operation, as it is called cyclically
        download_state -- StepState as defined in the datareplay.proto. Fields progress, message and state may be set.
        """
        pass


    def cleanup(self, reason):
        """
        Is used to perform basic cleanup, especially when the download was aborted or not successful. 
        As the following steps are not executed, already downloaded files should maybe be deleted. Can be blocking.
        reason -- StateEnum as defined in the datareplay.proto
        """
        pass