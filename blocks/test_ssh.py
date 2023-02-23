from fabric import Connection
from io import StringIO

s = StringIO(
  """#!/bin/sh 
  hostname
  """)

with Connection(host="hsuper-login01.hsu-hh.de",user="kramerd",connect_kwargs={"password": "@Annika02102011"}) as c:
  c.run('sbatch --parsable --job-name=prefect --partition=small --nodes=1 --ntasks-per-node=72 --time=02:00:00', in_stream=s)
    