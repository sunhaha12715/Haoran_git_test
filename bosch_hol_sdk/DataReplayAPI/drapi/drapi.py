#!/usr/bin/env python3.9
import argparse
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from datetime import timedelta,timezone
import pickle
import re
import signal
from concurrent import futures
from threading import Lock, Semaphore
from typing import NamedTuple
from pathlib import Path
import grpc

import datareplay_pb2
import datareplay_pb2_grpc
import replay_executor
import system_helper


logfolder = Path('/var/log/dspace/drapi/')
logfolder.mkdir(parents=True, exist_ok=True)


class ReplaySynchronization(NamedTuple):
    overall : Semaphore
    download : Semaphore
    replay: Lock
    upload : Semaphore

class QueuedReplayJob(NamedTuple):
    job : datareplay_pb2.ReplayJob
    future : futures.Future
    executor : replay_executor.ReplayExecutor

class gRPCReplayExecutor(datareplay_pb2_grpc.ReplayExecutorCommunicationServicer):
    _logger = logging.getLogger("DRAPI")
    
    def __init__(self, database_path=None, parallelize=False, max_downloads=1, max_uploads=1) -> None:
        super().__init__()
        log_format = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s: %(message)s")
        self._logger.setLevel(level=logging.DEBUG)
        file_handler_d = TimedRotatingFileHandler(
            logfolder / "DRAPI.log",
            when="MIDNIGHT",
            interval=1,
            backupCount=7,
        )
        file_handler_d.suffix = "%Y-%m-%d.log"
        file_handler_d.exMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        file_handler_d.setLevel(level=logging.DEBUG)
        file_handler_d.setFormatter(log_format)
        self._logger.addHandler(file_handler_d)

        if parallelize:
            # + 1 for the replay step
            self.max_workers = max_downloads + max_uploads + 1
            self._logger.info(f"Using {self.max_workers} workers with {max_downloads} simultaneous downloads and {max_uploads} simultaneous uploads")
        else:
            self.max_workers = 1
            max_downloads = 1
            max_uploads = 1
            self._logger.info(f"Using one worker")

        self._executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self._queued_jobs = list()
        self._replay_synchronization = ReplaySynchronization(Semaphore(self.max_workers), Semaphore(max_downloads), Lock(), Semaphore(max_uploads))
        self._lock = Lock()
        self._database_path = database_path
        self.deserialize_jobs()

    def GetRunningReplayJobState(self, request, context):
        response = datareplay_pb2.JobState()
        # self._logger.debug("GetRunningReplayJobState")
        response.overall_state = datareplay_pb2.FINISHED
        with self._lock:
            for qrp in self._queued_jobs:
                # If only one worker, search for the overall_state
                job_state = qrp.executor.get_state()
                if self.max_workers == 1:
                    if job_state.overall_state == datareplay_pb2.RUNNING:
                        response.CopyFrom(job_state)
                        break
                # If multiple worker, search for the job which is currently in the step replay
                else:
                    if job_state.replay_state.state == datareplay_pb2.RUNNING:
                        response.CopyFrom(job_state)
                        break
        return response

    def GetRunningJobStates(self, request, context):
        response = datareplay_pb2.JobStates()
        # self._logger.debug("GetRunningJobStates")
        
        with self._lock:
            for qrp in self._queued_jobs:
                job_state = qrp.executor.get_state()
                if job_state.overall_state == datareplay_pb2.RUNNING:
                    response.job_states.append(datareplay_pb2.JobState())
                    response.job_states[-1].CopyFrom(job_state)
        return response

    def GetAllJobStates(self, request, context):
        response = datareplay_pb2.JobStates()
        # self._logger.debug("GetAllJobStates")
        with self._lock:
            for qrp in self._queued_jobs:
                job_state = qrp.executor.get_state()
                response.job_states.append(datareplay_pb2.JobState())
                response.job_states[-1].CopyFrom(job_state)
        return response

    def GetAllReplayJobs(self, request, context):
        response = datareplay_pb2.ReplayJobs()
        with self._lock:
            for qrp in self._queued_jobs:
                response.jobs.append(datareplay_pb2.ReplayJob())
                response.jobs[-1].CopyFrom(qrp.job)
        return response

    def DeleteAllReplayJobs(self, request, context):
        response = datareplay_pb2.Response()
        response.return_value = datareplay_pb2.FAIL
        try:
            with self._lock:
                previous_job_count = len(self._queued_jobs)
                self.stop_execution()
                self._queued_jobs.clear()
                self._logger.info(f"Deleted {previous_job_count} replay jobs")
                self.serialize_jobs()
            response.return_value = datareplay_pb2.SUCCESS
        finally:
            return response

    def DeleteFinishedJobs(self, request, context):
        response = datareplay_pb2.Response()
        response.return_value = datareplay_pb2.FAIL
        try:
            with self._lock:
                previous_job_count = len(self._queued_jobs)
                finished_job_states = (datareplay_pb2.FINISHED, datareplay_pb2.ABORTED, datareplay_pb2.ERROR)
                self._queued_jobs[:] = [qrp for qrp in self._queued_jobs if qrp.executor.get_state().overall_state not in finished_job_states]
                current_job_count = len(self._queued_jobs)
                self._logger.info(f"Deleted {previous_job_count-current_job_count} finished replay jobs, {current_job_count} jobs remaining")
                self.serialize_jobs()
            response.return_value = datareplay_pb2.SUCCESS
        finally:
            return response

    def DeletePendingJobs(self, request, context):
        response = datareplay_pb2.Response()
        response.return_value = datareplay_pb2.FAIL
        try:
            with self._lock:
                previous_job_count = len(self._queued_jobs)
                self._queued_jobs[:] = [qrp for qrp in self._queued_jobs if qrp.executor.get_state().overall_state != datareplay_pb2.PENDING]
                current_job_count = len(self._queued_jobs)
                self._logger.info(f"Deleted {previous_job_count-current_job_count} finished replay jobs, {current_job_count} jobs remaining")
                self.serialize_jobs()
            response.return_value = datareplay_pb2.SUCCESS
        finally:
            return response

    def DeleteReplayJobs(self, request, context):
        response = datareplay_pb2.Response()
        response.return_value = datareplay_pb2.FAIL
        try:
            with self._lock:
                previous_job_count = len(self._queued_jobs)
                delete_indices = {idx for idx, qrp in enumerate(self._queued_jobs) if re.fullmatch(request.regexp, qrp.job.job_name)}
                for idx in delete_indices:
                    self._queued_jobs[idx].executor.stop()
                    self._queued_jobs[idx].future.cancel()
                self._queued_jobs[:] = [qrp for idx, qrp in enumerate(self._queued_jobs) if idx not in delete_indices]
                current_job_count = len(self._queued_jobs)
                self._logger.info(f"Deleted {previous_job_count-current_job_count} replay jobs with regex {request.regexp}, {current_job_count} jobs remaining")
                self.serialize_jobs()
            response.return_value = datareplay_pb2.SUCCESS
        finally:
            return response

    def GetVolumeInfo(self, request, context):
        return system_helper.get_volume_info()
    
    def AddReplayJobs(self, request, context):
        response = datareplay_pb2.Response()
        response.return_value = datareplay_pb2.FAIL
        try:
            self._logger.info(f"Adding {len(request.jobs)} new jobs")
            with self._lock:
                self.add_jobs(request.jobs)
            self._logger.info(f"Adding jobs finished")
            self.serialize_jobs()
            response.return_value = datareplay_pb2.SUCCESS
        finally:
            return response

    def stop_execution(self):
        self._logger.info("Stopping execution")
        for qrp in self._queued_jobs:
            qrp.executor.stop()
            qrp.future.cancel()
        for qrp in self._queued_jobs:
            try:
                qrp.future.result()
            except futures.CancelledError:
                pass

    def add_jobs(self, jobs, job_states=None):
        if job_states is None:
            job_states = [datareplay_pb2.JobState() for _ in jobs]
        for job, job_state in zip(jobs, job_states):
            job_state.current_job.CopyFrom(job)
            executor = replay_executor.ReplayExecutor(self._replay_synchronization, job_state)
            
            SHA_TZ = timezone(timedelta(hours=16), name='Asia/Shanghai',)
            jobtime = datetime.utcnow().astimezone(SHA_TZ).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            
            future = self._executor.submit(executor.execute_replay, job, jobtime)

            future.add_done_callback(self._make_future_callback(job, executor))
            self._queued_jobs.append(QueuedReplayJob(job, future, executor))

    def serialize_jobs(self):
        if self._database_path is None: return
        jobs = [qrp.job for qrp in self._queued_jobs]
        job_states = [qrp.executor.get_state() for qrp in self._queued_jobs]
        try:
            with open(self._database_path, "wb") as file:
                pickle.dump((jobs, job_states), file)
                self._logger.debug(f"Stored {len(jobs)} jobs at {self._database_path}")
        except OSError as e:
            self._logger.error(f"Serialzing jobs failed: {type(e)}: {e}")

    def deserialize_jobs(self):
        if self._database_path is None: return
        try:
            with open(self._database_path, "rb") as file:
                jobs, job_states = pickle.load(file)
                self._logger.info(f"Loaded {len(jobs)} jobs from {self._database_path}")
                for state in job_states:
                    if state.overall_state == datareplay_pb2.RUNNING:
                        state.overall_state = datareplay_pb2.ERROR
                        self._logger.info(f"Job {state.current_job.job_name} was in state RUNNING, discarding with state ERROR.")
                self.add_jobs(jobs, job_states)
        except FileNotFoundError as e:
            self._logger.info(f"Found no existing database, starting with empty queue")
        except OSError as e:
            self._logger.error(f"Deserializing jobs failed: {type(e)}: {e}, starting with empty queue")

    # Due to Python's late binding use a function to create functions on the fly 
    def _make_future_callback(self, job, executor):
        def future_callback(future):
            self._logger.debug(f"Job {job.job_name} finished with state {executor.get_state().overall_state}")
            try:
                if future.exception():
                    self._logger.error(f"Unhandled exception in job {job.job_name}: {future.exception()}")
                    executor.get_state().overall_state = datareplay_pb2.ERROR
                # Only serialize the jobs here if they were not cancelled
                # as cancelling often cancels multiple jobs - so it is sufficient
                # to only write the state then. Additionally, also check if the executor
                # was actually executed.
                result = future.result()
                if result is replay_executor.ExecutorState.EXECUTED:
                    self.serialize_jobs()
            except futures.CancelledError:
                pass
        return future_callback


class NumberGreaterZeroAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values < 1:
            raise argparse.ArgumentError(self, "The number must be greater or equal than 1")
        setattr(namespace, self.dest, values)

class DelayAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values < 0.1:
            raise argparse.ArgumentError(self, "The number must be greater or equal than 0.1")
        setattr(namespace, self.dest, values)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", default="[::]:50051", help="Specifies the address and port the Data Replay Executor starts listening (default: %(default)s)")
    parser.add_argument("-db", "--database", help=("The database where jobs descriptions are stored. If the file exists, PENDING  "
                                                    "will be requeued. If the argument is not passed, no database will be created. Note: If the database "
                                                    "contains jobs in state RUNNING, these jobs will not be resumed or rerun. Instead, the overall_state of these jobs is set "
                                                    "to ERROR to avoid an inconsistent state."))
    parser.add_argument("-l", "--log-level", default="DEBUG", choices=logging._levelToName.values(), help="Specify the log level of the Data Replay Executor (default: %(default)s)")
    parser.add_argument("-rl", "--rtmaps-log-level", default="WARNING", choices=logging._levelToName.values(), help=("Specify the log level of the rtmaps logger. Note: can only be "
                                                                                                                        "higher than the overall level (can only be used to reduce the "
                                                                                                                        "number of warnings) (default: %(default)s)"))
    parser.add_argument("-f", "--log-file", default=Path('/var/log/dspace/drapi/log_all.log'), type=Path, help="The file where the text output is logged")
    parser.add_argument("-d", "--status-request-delay", default=1.0, action=DelayAction, type=float, help=("The time in seconds between the calls to get_progress" 
                                                                                                            "to the plugins (default: %(default)s)"))
    parser.add_argument("-P", "--parallelize", action="store_true", help="Specifies if the download and upload of multiple jobs are executed simultaneously")
    parser.add_argument("-D", "--max-downloads", default=1, type=int, action=NumberGreaterZeroAction, help=("EXPERIMENTAL. The number of parallel downloads. " 
                                                                                                            "Only used with --parallelize (default: %(default)s)"))
    parser.add_argument("-U", "--max-uploads", default=1, type=int, action=NumberGreaterZeroAction, help=("EXPERIMENTAL. The number of parallel uploads. Only "
                                                                                                            "used with --parallelize (default: %(default)s)"))

    args = parser.parse_args(argv)

    log_handler = [ logging.StreamHandler() ]
    
    # Also log to file if required
    if args.log_file is not None:
        # log_handler.append(logging.FileHandler(args.log_file))
        file_handler = TimedRotatingFileHandler(args.log_file, when="MIDNIGHT", interval=1, backupCount=7)
        log_handler.append(file_handler)

    replay_executor.init_start(logfolder)

    logging.basicConfig(level=args.log_level, format="%(asctime)s: %(name)s: %(levelname)s: %(message)s", handlers=log_handler)

    replay_executor.rtmaps_helper.set_log_level(args.rtmaps_log_level)
    # Set module wide settings
    replay_executor.status_request_delay = args.status_request_delay

    # Initialize executor
    parallelization_args = { "parallelize": args.parallelize, "max_downloads": args.max_downloads, "max_uploads": args.max_uploads }

    executor = gRPCReplayExecutor(args.database, **parallelization_args)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datareplay_pb2_grpc.add_ReplayExecutorCommunicationServicer_to_server(executor, server)
    server.add_insecure_port(args.address)

    # Configure signal handler for graceful shutdown
    original_handlers = {}
    def signal_handler(sig, frame):
        # Install original handler to allow for a second CTRL+C to forcefully
        # abort the execution.
        signal.signal(sig, original_handlers.get(sig, signal.SIG_DFL))

        logging.info(f"Received signal {sig}, stopping")
        executor.stop_execution()

        logging.info("Serializing jobs.")
        executor.serialize_jobs()

        logging.info("Stopping gRPC server.")
        server.stop(None)

        logging.info("Stopped gRPC server")

    original_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, signal_handler)
    original_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, signal_handler)

    # Start gRPC server
    logging.info(f"Starting gRPC server at {args.address}")
    
    server.start()
    logging.info("Started gRPC server")
    
    server.wait_for_termination()
    logging.info("Shutdown")


if __name__ == "__main__":
    main()
