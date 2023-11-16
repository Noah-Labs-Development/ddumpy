import logging
import socket
from contextlib import contextmanager

import docker
from docker.models.containers import Container
from docker.models.networks import Network
from rich import print

from .console import NonBlockingConsole

log = logging.getLogger()


@contextmanager
def docker_container(client: docker.DockerClient, image: str, **kwargs):
    container = None  # type: ignore
    try:
        log.info(f"Starting docker container with image: {image}")
        container: Container = client.containers.create(image, **kwargs)  # type: ignore
        container.start()
        log.info(f"Started docker container: {container.id}:{container.name}")
        yield container
    finally:
        if container:
            print("")
            log.warning(f"Stopping docker container: {container.id}:{container.name}")
            container.stop()
            container.wait()
            container.remove()


@contextmanager
def docker_network(client: docker.DockerClient, name: str, **kwargs):
    network = None  # type: ignore
    try:
        log.info(f"Starting docker network: {name}")
        network: Network = client.networks.create(name, **kwargs)  # type: ignore
        yield network
    finally:
        if network:
            log.warning(f"Removing docker network: {name}")
            network.remove()


def comunicate_with_container(container: Container):
    sock = container.attach_socket(
        params={"stdin": 1, "stdout": 1, "stderr": 1, "stream": 1}
    )

    sock._sock.settimeout(0.1)
    with NonBlockingConsole() as nbc:
        while True:
            try:
                b = sock._sock.recv(4)
                while len(b) > 0:
                    print(b.decode(), end="")
                    b = sock._sock.recv(4)
            except socket.timeout:
                pass
            key_in = nbc.get_data()
            if key_in is not None:
                sock._sock.send(key_in)

            container = container.client.containers.get(container.id)  # type: ignore
            if container.status != "running":
                break
