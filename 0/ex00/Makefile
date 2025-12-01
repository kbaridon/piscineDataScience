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
	@echo "To log in the database: "
	@echo "1. docker exec -it postgreSQL bash"
	@echo "2. psql -U login -d piscineds -W"
	@echo "3. enter your password"