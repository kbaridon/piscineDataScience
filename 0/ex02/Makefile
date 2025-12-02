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
	@echo "1. docker exec -it postgreSQL bash"
	@echo "2. psql -U login -d piscineds -W"
	@echo "3. enter your password"
	@echo "4. CREATE TABLE data_2022_oct (event_time timestamptz, event_type text, product_id integer, price decimal(10,2), user_id bigint, user_session uuid);"
	@echo "5. Not in the docker: docker cp /home/kbaridon/goinfre/subject/customer/data_2022_oct.csv postgreSQL:/tmp/data_oct.csv"
	@echo "6. COPY data_2022_oct(event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/data_oct.csv' WITH (FORMAT CSV, HEADER, NULL '');"