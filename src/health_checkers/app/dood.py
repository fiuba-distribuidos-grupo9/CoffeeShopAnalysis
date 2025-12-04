# Imports.
from __future__ import annotations
from typing import Optional
import docker
import logging

# DockerReviver class to manage Docker containers.
class DockerReviver:
    def __init__(self, docker_host: Optional[str] = None):
        self.client = docker.from_env()
        self._closed = False

    def revive_container(self, container_name: str) -> bool:
        logging.info(f"action: revive_controller | status: in progress")
        if self._closed:
            return False
        try:
            cont = self.client.containers.get(container_name)
        except Exception:
            logging.error(f"action: revive_controller | result: fail | error: '{container_name}' not found")
            return False

        cont.reload()
        status = getattr(cont, "status", "unknown")

        try:
            if status in ("exited", "created", "paused"):
                cont.start()
                logging.info(f"action: revive_controller | result: success | controller_name: {container_name}")
                return True
            elif status in ("running",):
                cont.restart()
                logging.info(f"action: revive_controller | result: success | controller_name: {container_name}")
                return True
            else:
                cont.start()
                logging.info(f"action: revive_controller | result: success | controller_name: {container_name}")
                return True
        except Exception as e:
            logging.error(f"action: revive_controller | result: fail | controller_name: {container_name} | error: {e}")
            return False

    def close(self):
        if not self._closed:
            try:
                self.client.close()
                self._closed = True
                logging.info(f"action: docker_client_close_down | result: success")
            except Exception as e:
                logging.error(f"action: docker_client_close_down | result: fail | error: {e}")
