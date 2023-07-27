.PHONY: start stop destroy

start:
	echo "Running airflow on background"
	docker-compose build
	docker network create cluster-network || true
	docker-compose up -d

stop:
	echo "Stopping airflow"
	docker network rm cluster-network || true
	docker-compose down

destroy:
	echo "Stopping airflow and removing volumes"
	docker network rm cluster-network || true
	docker-compose down -v
