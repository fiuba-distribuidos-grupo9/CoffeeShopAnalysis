# Import.
import os
import docker
from pathlib import Path

# Constantes.
ENV_PATH = Path(__file__).parent / ".env"

# Environment loader.
def load_env(path: Path):
    env = {}
    if not path.exists():
        print(f"[manual] WARNING: Was not found {path}. Using defaults.")
        return env

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

    return env

# Target parser.
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

# Main function.
def main():
    client = docker.from_env()
    env = load_env(ENV_PATH)
    targets = parse_targets(env.get("CHAOS_TARGETS", ""))
    if not targets:
        print("[manual] ERROR: CHAOS_TARGETS were not defined in '.env'")
        return

    NAMES = {str(i + 1): name for i, name in enumerate(targets)}
    print("====================================")
    print("🧨 Chaos Monkey ready (manual mode)")
    print("====================================\n")
    print("Available containers:")
    for key, name in NAMES.items():
        print(f"  [{key}] {name}")

    print("\nEnter the number to send SIGKILL to the corresponding container.")
    print(f"Commands: {', '.join(NAMES.keys())}  |  'ls' to list  |  'q' to quit.\n")

    while True:
        cmd = input("> ").strip().lower()
        if cmd == "q":
            print("Exiting.")
            break

        if cmd == "ls":
            print("\nCurrent state of the containers:")
            for key, name in NAMES.items():
                try:
                    c = client.containers.get(name)
                    print(f"  [{key}] {name} | status={c.status}")
                except Exception:
                    print(f"  [{key}] {name} | (not found)")

            print()
            continue

        if cmd in NAMES:
            name = NAMES[cmd]
            try:
                cont = client.containers.get(name)
                cont.kill(signal="SIGKILL")
                print(f"SIGKILL sent to {name}.")
            except Exception as e:
                print(f"Error sending SIGKILL to {name}: {e}")
        else:
            print(f"Invalid input. Use {', '.join(NAMES.keys())}, 'ls' or 'q'.")

# Entry point.
if __name__ == "__main__":
    main()
