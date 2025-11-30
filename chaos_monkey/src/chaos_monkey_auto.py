# Imports.
import os
import time
import random
import docker
from pathlib import Path

# Constants.
ENV_PATH = P1ath(__file__).parent / ".env"

# Environment loader.
def load_env(path: Path):
    env = {}
    if not path.exists():
        print(f"[auto] WARNING: Was not found {path}. Using defaults.")
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

# Check if container is running.
def is_running(container):
    try:
        container.reload()
        return container.status == "running"
    except Exception:
        return False

# Main function.
def main():
    env = load_env(ENV_PATH)
    targets = parse_targets(env.get("CHAOS_TARGETS", ""))
    interval = int(env.get("CHAOS_INTERVAL", "20") or "20")
    if not targets:
        print("[auto] ERROR: There are no CHAOS_TARGETS in '.env'. Edit 'src/.env'")
        return

    client = docker.from_env()
    print("[auto] Chaos Monkey Automatic Ready.")
    print(f"[auto] Eligible: {', '.join(targets)} | Interval: {interval}s")
    print("[auto] Ctrl+C to stop.\n")
    try:
        while True:
            candidates = targets[:]
            random.shuffle(candidates)
            chosen = None
            while candidates:
                name = candidates.pop()
                try:
                    cont = client.containers.get(name)
                except Exception:
                    print(f"[auto] {name}: It does not exist/is visible. It is ignored..")
                    continue

                if is_running(cont):
                    chosen = cont
                    break
                else:
                    print(f"[auto] {name}: It's already down. Looking for another one...")

            if chosen is None:
                print("[auto] There are no eligible containers in 'running' status. We'll sleep and try again.")
                time.sleep(interval)
                continue

            try:
                print(f"[auto] Sending SIGKILL to {chosen.name} ...")
                chosen.kill(signal="SIGKILL")
                print(f"[auto] SIGKILL sent to {chosen.name}.")
            except Exception as e:
                print(f"[auto] SIGKILL Error {chosen.name}: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[auto] Stopped by user.")

# Entry point.
if __name__ == "__main__":
    main()
