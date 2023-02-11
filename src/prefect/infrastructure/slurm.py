from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.blocks.core import SecretStr
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible

from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, Union
from typing_extensions import Literal

from pydantic import Field
import anyio.abc
from io import StringIO

from fabric import Connection
from invoke.exceptions import UnexpectedExit, Failure, ThreadException
from invoke.runners import Result

import time

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
        description=(
          "The hostname of the login node for the cluster running SLURM"
        )
    )

    username: str = Field(
        default=None,
        description=(
          "The username of your account on the cluster"
        )
    )

    password: SecretStr = Field(
        default=None,
        description=(
          "The password to authenticate username"
        )  
    )

    slurm_kwargs: dict[str,str] = Field(
      default=None,
      description=(
        "A dictionary with slurm batch arguments as key-value pairs. E.g, the parameter --nodes=1"
      )
    )

    @sync_compatible
    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmJobResult:
        if not self.command:
            raise ValueError("Slurm job cannot be run with empty command.")

        jobid = (await run_sync_in_worker_thread(self._create_job)).strip()

        pid = await run_sync_in_worker_thread(self._get_infrastructure_pid, jobid)

        self.logger.info(task_status)
        if task_status is not None:
            task_status.started(pid)

        # Monitor the job until completion
        status_code = await run_sync_in_worker_thread(
            self._watch_job, jobid
        )

        return SlurmJobResult(identifier=pid, status_code=status_code)


    def preview(self):
        return self._get_submit_script()

    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        _, jobid = self._parse_infrastructure_pid(infrastructure_pid)

        with self._get_connection() as c:
          try: 
            c.run(self._get_kill_command(jobid), timeout=grace_seconds)
          except UnexpectedExit:
            self.logger.warn(f"Slurm Job: sbatch exited with unexpected result (non-zero exit code).")
          except Failure:
            self.logger.error(f"Slurm Job {jobid!r}: scancel did not exit cleanly.")
          except ThreadException:
            self.logger.error(f"Slurm Job {jobid!r}: scancel IO streams encountered problems.")
          except TimeoutError:
            self.logger.error(f"Slurm Job {jobid!r}: scancel did not finish in time.")
          finally:
            c.close()

        
    def _get_connection(self) -> Connection:
      """
      Return a connection to the slurm login node
      """
      return Connection(
        host=self.host,
        user=self.username,
        connect_kwargs={
          "password": self.password.get_secret_value()
          }
        )


    def _create_job(self, grace_seconds: int=30) -> str:
      """
      Submit a slurm job
      """
      
      result = self._run_remote_command(
        cmd=self._get_submit_command(),
        in_stream=self._get_submit_script(), 
        grace_seconds=grace_seconds
      )

      return result.stdout


    def _get_submit_command(self) -> str:
      """
      Generates the sbatch command to submit a job to slurm
      """

      ## Create the arguments from slurm_kwargs
      args = [f"--{k}" if v == None else f"--{k}={v}" for k,v in self.slurm_kwargs.items()]
      cmd = " ".join(["sbatch","--parsable"] + args)
      
      return cmd

    def _get_submit_script(self) -> StringIO:
      """
      Generate the submit script for the slurm job
      """
      return StringIO(f"""#!/bin/sh
      {" ".join(self.command)}
      """)

    def _get_kill_command(self, jobid: int) -> str:
      """
      Generates the kill command to terminate a slurm job
      """

      return f"scancel ${jobid}"

    def _get_infrastructure_pid(self, jobid: str) -> str:
        """
        Generates a Slurm infrastructure PID.

        The PID is in the format: "<cluster name>:<jobid>".
        """
        pid = f"{self.host}:{jobid}"
        return pid

    def _parse_infrastructure_pid(self,infrastructure_pid,) -> Tuple[str, int]:
        """
        Parses the infrastructure pid formated as "<cluster name>:<jobid>" and 
        returns the cluster name and jobid
        """
        hostname, pid = infrastructure_pid.split(":")
        return hostname, int(pid)


    def _get_job_status_command(self,jobid) -> str:
        """
        Generate the squeue command to monitor job status
        """

        return f"squeue --job={jobid} --Format=State,exit_code --noheader"

    def _watch_job(self,jobid: str) -> int:
        
        completed = False
        submitted = False

        while not completed:
          
          result = self._run_remote_command(self._get_job_status_command(jobid))
          
          # Job never seen on the slurm queue
          if result.exited != 0 and not submitted:
            self.logger.error(f"Slurm Job {jobid!r}: Job not known to slurm.")
            completed = True
            return -1

          # Job removed from slurm queue - assume it finished ok
          if (result.exited != 0 or result.stdout == "") and submitted:
            self.logger.info(f"Slurm Job {jobid!r}: Job finished/cleared.")
            completed = True
            return 0
          
          submitted = True

          status,exit_code = result.stdout.split()[0:2]

          if status in ["COMPLETED"]:
            self.logger.info(f"Slurm Job {jobid!r}: Job {status}.")
            completed = True
            return int(exit_code)

          if status in ["PREEMPTED"]:
            self.logger.error(f"Slurm Job {jobid!r}: Job {status}.")
            completed = True
            return -1
          
          # TODO Make interval parameterisable
          time.sleep(10)

    def _run_remote_command(self, cmd: str, in_stream=None, grace_seconds: int = 30, safe=False) -> Result:
      
      result = None
      with self._get_connection() as c:

        try: 
          result = c.run(cmd, in_stream=in_stream, timeout=grace_seconds,hide='both')
        except UnexpectedExit:
          self.logger.warn(f"Slurm Job: [{cmd}] exited with unexpected result (non-zero exit code).")
          if not safe: raise 
        except Failure:
          self.logger.error(f"Slurm Job: [{cmd}] did not exit cleanly.")
          if not safe: raise
        except ThreadException:
          self.logger.error(f"Slurm Job: [{cmd}] IO streams encountered problems.")
          if not safe: raise
        except TimeoutError:
          self.logger.error(f"Slurm Job: [{cmd}] did not finish in time.")
          if not safe: raise
        except:
          if not safe: raise
        finally:
          c.close()

      return result