COMPOSE_FILE := srcs/docker-compose.yml

.PHONY: help up down clean fclean re

all: up

up:
	docker compose -f $(COMPOSE_FILE) up

stop:
	@echo "Stopping all containers..."
	@docker compose -f $(COMPOSE_FILE) down

clean:
	@echo "Performing project cleanup..."
	@docker compose -f $(COMPOSE_FILE) down -v --rmi all --remove-orphans

flcean: clean
	@docker volume prune -af
	@docker system prune --all --force --volumes

re: stop up

help:
	@echo "To test: (easier in DBeaver)"
	@echo "1. Launch the makefile."
	@echo "2. Create a .venv (python -m venv .venv)"
	@echo "3. Activate the venv (source .venv/bin/activate)"
	@echo "4. pip install pandas python-dotenv sqlalchemy psycopg2-binary tqdm"
	@echo "5. Launch the python script"
	@echo "6. Check the result in DBeaver (or connect to Dockerfile)"