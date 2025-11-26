from __future__ import annotations
from typing import Optional
import docker

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
            print(f"[revive] No encuentro contenedor '{container_name}'")
            return False

        cont.reload()
        status = getattr(cont, "status", "unknown")
        print(f"[revive] {container_name}: status actual = {status}")

        try:
            if status in ("exited", "created", "paused"):
                cont.start()
                print(f"[revive] start OK → {container_name}")
                return True
            elif status in ("running",):
                cont.restart()
                print(f"[revive] restart OK → {container_name}")
                return True
            else:
                cont.start()
                print(f"[revive] start intento → {container_name}")
                return True
        except Exception as e:
            print(f"[revive] Error reviviendo {container_name}: {e}")
            return False

    def close(self):
        if not self._closed:
            try:
                self.client.close()
                self._closed = True
                print(f"[reviver] Cliente Docker cerrado")
            except Exception as e:
                print(f"[reviver] Error cerrando el cliente Docker: {e}")