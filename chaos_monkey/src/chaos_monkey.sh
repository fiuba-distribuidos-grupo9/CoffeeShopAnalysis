# Script to set up and run the Chaos Monkey application.
docker compose up -d --build
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python src/chaos_monkey.py
