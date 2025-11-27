from __future__ import annotations
from typing import Optional
import docker
import logging

class DockerReviver:
    def __init__(self, docker_host: Optional[str] = None):
        self.client = docker.from_env()
        self._closed = False

    def revive_container(self, container_name: str) -> bool:
        if self._closed:
            return False
        try:
            cont = self.client.containers.get(container_name)
        except Exception:
            logging.error(f"No encuentro contenedor '{container_name}'")
            return False

        cont.reload()
        status = getattr(cont, "status", "unknown")
        logging.info(f"{container_name}: status actual = {status}")

        try:
            if status in ("exited", "created", "paused"):
                cont.start()
                logging.info(f"start OK → {container_name}")
                return True
            elif status in ("running",):
                cont.restart()
                logging.info(f"restart OK → {container_name}")
                return True
            else:
                cont.start()
                logging.info(f"start intento → {container_name}")
                return True
        except Exception as e:
            logging.error(f"Error reviviendo {container_name}: {e}")
            return False

    def close(self):
        if not self._closed:
            try:
                self.client.close()
                self._closed = True
                logging.info(f"Cliente Docker cerrado")
            except Exception as e:
                logging.error(f"Error cerrando el cliente Docker: {e}")