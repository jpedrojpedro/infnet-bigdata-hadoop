.PHONY: start stop destroy

start:
	echo "Running airflow on background"
	docker-compose build
	docker network create cluster-network || true
	docker-compose up -d

stop:
	echo "Stopping airflow"
	docker-compose down
	docker network rm cluster-network || true

destroy:
	echo "Stopping airflow and removing volumes"
	docker-compose down -v
	docker network rm cluster-network || true
