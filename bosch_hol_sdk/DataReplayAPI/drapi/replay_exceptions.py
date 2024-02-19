


class BaseDataReplayError(Exception):
    def __init__(self, *args, step_state, step_message, **kwargs):
        super().__init__(*args, **kwargs)
        self.step_state = step_state
        self.step_message = step_message


class BaseDownloadStepError(BaseDataReplayError):
    pass


class BaseReplayStepError(BaseDataReplayError):
    pass


class BaseUploadStepError(BaseDataReplayError):
    pass
