from prefect import flow, get_run_logger

import socket
from time import sleep

@flow
def hello(name: str = "world"):
    get_run_logger().info(f"Hello {name}!")
    get_run_logger().info(socket.gethostname())
    sleep(30)
    get_run_logger().info(f"Finishing!")


if __name__ == "__main__":
    hello()
