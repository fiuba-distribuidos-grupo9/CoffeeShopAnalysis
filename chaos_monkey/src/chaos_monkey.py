import os
import docker
from pathlib import Path

ENV_PATH = Path(__file__).parent / ".env"

def load_env(path: Path):
    """Carga un .env simple tipo KEY=VALUE."""
    env = {}
    if not path.exists():
        print(f"[manual] WARNING: no se encontró {path}. Usando defaults.")
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

def main():
    client = docker.from_env()
    env = load_env(ENV_PATH)
    targets = parse_targets(env.get("CHAOS_TARGETS", ""))
    if not targets:
        print("[manual] ERROR: no se definieron CHAOS_TARGETS en .env")
        return
    NAMES = {str(i + 1): name for i, name in enumerate(targets)}
    print("==========================================")
    print("🧨 Chaos Monkey listo (modo manual)")
    print("==========================================\n")
    print("Contenedores disponibles:")
    for key, name in NAMES.items():
        print(f"  [{key}] {name}")
    print("\nIngrese el número para enviar SIGKILL al contenedor correspondiente.")
    print(f"Comandos: {', '.join(NAMES.keys())}  |  'ls' para listar  |  'q' para salir.\n")

    while True:
        cmd = input("> ").strip().lower()

        if cmd == "q":
            print("Saliendo.")
            break

        if cmd == "ls":
            print("\nEstado actual de los contenedores:")
            for key, name in NAMES.items():
                try:
                    c = client.containers.get(name)
                    print(f"  [{key}] {name} | status={c.status}")
                except Exception:
                    print(f"  [{key}] {name} | (no encontrado)")
            print()
            continue

        if cmd in NAMES:
            name = NAMES[cmd]
            try:
                cont = client.containers.get(name)
                cont.kill(signal="SIGKILL")
                print(f"SIGKILL enviado a {name}.")
            except Exception as e:
                print(f"Error al enviar SIGKILL a {name}: {e}")
        else:
            print(f"Entrada inválida. Use {', '.join(NAMES.keys())}, 'ls' o 'q'.")

if __name__ == "__main__":
    main()
