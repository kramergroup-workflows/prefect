import time, os
import abc

from io import StringIO
from typing import Optional, Tuple, List

import anyio.abc
from fabric import Connection
from invoke.exceptions import Failure, ThreadException, UnexpectedExit
from invoke.runners import Result
from pydantic import Field
from typing_extensions import Literal

from prefect.blocks.core import SecretStr
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from prefect.infrastructure.process import Process

from io import TextIOBase
from enum import Enum

class SlurmJobStatus(Enum):
    COMPLETED=0
    RUNNING=1
    FAILED=2
    PREEMPTED=3
    PENDING=4
    UNDEFINED=5
    UNKNOWN=6
class SlurmBackend:

    @abc.abstractmethod
    def submit(self,slurm_kwargs:dict[str,str],run_script:TextIOBase=None, grace_seconds:int=30) -> int:
        """Submit a new SLURM Job to process a flow rum"""

    @abc.abstractmethod
    def status(self,jobid:int, grace_seconds:int=30) -> SlurmJobStatus:  
        """Obtain the status of a SLURM job"""

    @abc.abstractmethod
    def kill(self,jobid:int, grace_seconds:int=30):
        """Cancel the job with jobid"""

class CLIBasedSlurmBackend(SlurmBackend):

    host:str
    username:str
    password:str

    def __init__(self,host:str, username:str, password:str):
        self.host=host
        self.username=username
        self.password=password

    def submit(self,slurm_kwargs:dict[str,str], run_script:TextIOBase=None, grace_seconds:int=30) -> int:
        
        result = self._run_remote_command(
            cmd=self._submit_command(slurm_kwargs),
            in_stream=run_script,
            grace_seconds=grace_seconds,
        )

        return int(result.stdout.strip())

    def kill(self,jobid:int, grace_seconds:int=30):

        self._run_remote_command(
              cmd=self._kill_command(jobid),
              grace_seconds=grace_seconds,
        )

    def status(self,jobid:int, grace_seconds:int=30) -> SlurmJobStatus:

        result = self._run_remote_command(
            cmd=self._status_command(jobid),
            grace_seconds=grace_seconds,
        )

        # Status command exits with non-zero exit code if jobid is not found.
        # This includes finished jobs that have been removed from the queue!!!!
        if result.exited != 0: return SlurmJobStatus.UNDEFINED

        try:
          status, exit_code = [v.strip() for v in result.stdout.split()[0:2]]

          if status == "PENDING": return SlurmJobStatus.PENDING
          if status == "COMPLETED": return SlurmJobStatus.COMPLETED
          if status == "PREEMPTED": return SlurmJobStatus.PREEMPTED
          if status == "FAILED": return SlurmJobStatus.FAILED
          if status == "RUNNING": return SlurmJobStatus.RUNNING

          return SlurmJobStatus.UNKNOWN
        except:
          return SlurmJobStatus.UNDEFINED


    def _run_remote_command(
      self, cmd: str, in_stream=None, grace_seconds: int = 30, safe=False
    ) -> Result:

      result = None
      with self._get_connection() as c:

          try:
              result = c.run(
                  cmd, in_stream=in_stream, timeout=grace_seconds, hide="both"
              )
          except UnexpectedExit:
              self.logger.warn(
                  f"Slurm Job: [{cmd}] exited with unexpected result (non-zero exit code)."
              )
              if not safe:
                  raise
          except Failure:
              self.logger.error(f"Slurm Job: [{cmd}] did not exit cleanly.")
              if not safe:
                  raise
          except ThreadException:
              self.logger.error(
                  f"Slurm Job: [{cmd}] IO streams encountered problems."
              )
              if not safe:
                  raise
          except TimeoutError:
              self.logger.error(f"Slurm Job: [{cmd}] did not finish in time.")
              if not safe:
                  raise
          except:
              if not safe:
                  raise
          finally:
              c.close()

      return result


    def _submit_command(self,slurm_kwargs:dict[str,str]) -> str:
        """
        Generates the sbatch command to submit a job to slurm
        """

        ## Create the arguments from slurm_kwargs
        args = [
            f"--{k}" if v == None else f"--{k}={v}" for k, v in slurm_kwargs.items()
        ]
        cmd = " ".join(["sbatch", "--parsable"] + args)

        return cmd


    def _kill_command(self, jobid: int) -> str:
        """
        Generates the kill command to terminate a slurm job
        """

        return f"scancel ${jobid}"
    

    def _status_command(self, jobid) -> str:
        """
        Generate the squeue command to monitor job status
        """

        return f"squeue --job={jobid} --Format=State,exit_code --noheader"


    def _get_connection(self) -> Connection:
        """
        Return a connection to the slurm login node
        """
        return Connection(
            host=self.host,
            user=self.username,
            connect_kwargs={"password": self.password.get_secret_value()},
        )


class SlurmJobResult(InfrastructureResult):
    """Contains information about the final state of a completed Slurm Job"""


class SlurmJob(Infrastructure):
    """
    Runs a command in a SLURM job.

    Requires access to a SLURM scheduler.
    """

    type: Literal["slurm-job"] = Field(
        default="slurm-job", description="The type of infrastructure."
    )

    host: str = Field(
        default=None,
        description=("The hostname of the login node for the cluster running SLURM"),
    )

    username: str = Field(
        default=None, description=("The username of your account on the cluster")
    )

    pre_run: Optional[List[str]] = Field(
        default=[], description=("Commands to run before executing the flow with the slurm job")
    )

    post_run: Optional[List[str]] = Field(
        default=[], description=("Commands to run after executing the flow with the slurm job")
    )

    password: SecretStr = Field(
        default=None, description=("The password to authenticate username")
    )

    slurm_kwargs: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "A dictionary with slurm batch arguments as key-value pairs. E.g, the parameter --nodes=1"
        ),
    )


    _backend_instance : SlurmBackend = None

    @property
    def _backend(self) -> SlurmBackend:
      if not self._backend_instance: 
        self.logger.debug(f"Instantiating Slurm CLI-based backend on {self.host} for user {self.username}")
        self._backend_instance = CLIBasedSlurmBackend(self.host,self.username,self.password)

      return self._backend_instance

    @sync_compatible
    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmJobResult:

        if not self.command:
            raise ValueError("Slurm job cannot be run with empty command.")

        jobid = await run_sync_in_worker_thread(self._backend.submit, self.slurm_kwargs, StringIO(self._submit_script()))
        pid = await run_sync_in_worker_thread(self._get_infrastructure_pid, jobid)

        if task_status is not None:
            task_status.started(pid)

        self.logger.info(f"Slurm Job: Job {jobid} submitted and registered as {pid}.")

        # Monitor the job until completion
        status_code = await run_sync_in_worker_thread(self._watch_job, self._backend, jobid)

        return SlurmJobResult(identifier=pid, status_code=status_code)


    def preview(self):
        return "Not implemented"


    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        _, jobid = self._parse_infrastructure_pid(infrastructure_pid)
        self._backend.kill(jobid)


    def _submit_script(self) -> str:
        """
        Generate the submit script for the slurm job
        """
        script = ["#!/bin/bash"]
        script += [f"export {k}={v}" for k, v in self._get_environment_variables(False).items()]
        script += self.pre_run
        script += [" ".join(self.command)]
        script += self.post_run

        return "\n".join(script)


    def _get_infrastructure_pid(self, jobid: str) -> str:
        """
        Generates a Slurm infrastructure PID.

        The PID is in the format: "<cluster name>:<jobid>".
        """
        pid = f"{self.host}:{jobid}"
        return pid


    def _parse_infrastructure_pid(
        self,
        infrastructure_pid,
    ) -> Tuple[str, int]:
        """
        Parses the infrastructure pid formated as "<cluster name>:<jobid>" and
        returns the cluster name and jobid
        """
        hostname, pid = infrastructure_pid.split(":")
        return hostname, int(pid)


    def _watch_job(self, backend:SlurmBackend, jobid: str, polling_seconds: int=30) -> int:

        completed = False
        submitted = False

        startWatching = time.time()

        while not completed:

            status = backend.status(jobid)

            # Job never seen on the slurm queue
            if (status == status.UNDEFINED) and not submitted:
                self.logger.error(f"Slurm Job: Job {jobid!r} not known to slurm.")
                
                if (time.time() - startWatching) < polling_seconds:
                  # Just started watching, give the slurm agent some time to process the submission
                  continue
                else:
                  completed = True
                  return -1

            # This point is only reached if the jobid is known to slurm in an interation
            submitted = True

            # Job removed from slurm queue - assume it finished ok
            if (status == status.UNDEFINED) or (status == status.COMPLETED):
                self.logger.info(f"Slurm Job: Job {jobid!r} finished/cleared.")
                completed = True
                return 0

            if status == status.FAILED:
                self.logger.warn(f"Slurm Job: Job {jobid!r} failed.")
                completed = True
                return -1

            time.sleep(polling_seconds)

        # we should never reach this point!
        return -1


    def _get_environment_variables(self, include_os_environ: bool = True):
      os_environ = os.environ if include_os_environ else {}
      # The base environment must override the current environment or
      # the Prefect settings context may not be respected
      env = {**os_environ, **self._base_environment(), **self.env}

      # Drop null values allowing users to "unset" variables
      return {key: value for key, value in env.items() if value is not None}