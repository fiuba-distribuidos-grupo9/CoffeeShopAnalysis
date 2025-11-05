COMPOSE := docker compose

.PHONY: cm

cm:
	@echo "Abriendo Chaos Monkey (Python). Use 1..5, 'ls' o 'q'."
	python3 src/chaos_monkey.py

eligible:
	@echo "Elegibles definidos en 'src/.env':"
	@grep -E '^CHAOS_TARGETS=' 'src/.env' || echo "('src/.env' no encontrado)"
	@grep -E '^CHAOS_INTERVAL=' 'src/.env' || echo "('src/.env' no encontrado)"

cm-auto:
	@echo "Chaos Monkey AUTOMÁTICO (lee 'src/.env'). Ctrl+C para salir."
	python3 src/chaos_monkey_auto.py
