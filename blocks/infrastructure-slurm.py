import prefect

from prefect.infrastructure.slurm import SlurmJob

def create_slurm_job() -> SlurmJob:

    infra = SlurmJob(
      host="hsuper-login01.hsu-hh.de",
      username="kramerd",
      slurm_kwargs={
        "jobname": "prefect",
        "partition": "small",
        "nodes": "1",
        "ntasks-per-node": "72"
      },
    )

    return infra

if __name__ == "__main__":
  create_slurm_job().save("hsuper", overwrite=True)