import os
import time
import random
import docker
from pathlib import Path

ENV_PATH = Path(__file__).parent / ".env"

def load_env(path: Path):
    """Carga un .env simple KEY=VALUE (sin dependencias)."""
    env = {}
    if not path.exists():
        print(f"[auto] WARNING: no se encontró {path}. Usando defaults.")
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env

def parse_targets(s: str):
    if not s:
        return []
    parts = [p.strip() for p in s.replace(" ", ",").split(",") if p.strip()]
    seen = set()
    uniq = []
    for p in parts:
        if p not in seen:
            uniq.append(p)
            seen.add(p)
    return uniq

def is_running(container):
    try:
        container.reload()
        return container.status == "running"
    except Exception:
        return False

def main():
    env = load_env(ENV_PATH)
    targets = parse_targets(env.get("CHAOS_TARGETS", ""))
    interval = int(env.get("CHAOS_INTERVAL", "20") or "20")

    if not targets:
        print("[auto] ERROR: no hay CHAOS_TARGETS en .env. Edite controllers/.env")
        return

    client = docker.from_env()
    print("[auto] Chaos Monkey automático listo.")
    print(f"[auto] Elegibles: {', '.join(targets)} | Intervalo: {interval}s")
    print("[auto] Ctrl+C para detener.\n")

    try:
        while True:
            candidates = targets[:]  # copia
            random.shuffle(candidates)

            chosen = None
            while candidates:
                name = candidates.pop()
                try:
                    cont = client.containers.get(name)
                except Exception:
                    print(f"[auto] {name}: no existe/visible. Se ignora.")
                    continue

                if is_running(cont):
                    chosen = cont
                    break
                else:
                    print(f"[auto] {name}: ya está caído. Buscando otro...")

            if chosen is None:
                print("[auto] No hay contenedores elegibles en estado 'running'. Dormimos y reintentamos.")
                time.sleep(interval)
                continue

            try:
                print(f"[auto] Enviando SIGKILL a {chosen.name} ...")
                chosen.kill(signal="SIGKILL")
                print(f"[auto] SIGKILL enviado a {chosen.name}.")
            except Exception as e:
                print(f"[auto] Error al SIGKILL {chosen.name}: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[auto] Detenido por usuario.")

if __name__ == "__main__":
    main()
