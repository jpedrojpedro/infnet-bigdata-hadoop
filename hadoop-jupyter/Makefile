.PHONY: start stop destroy

start:
	echo "Creating base container image"
	docker build . -t cluster-base
	echo "Running micro-cluster on background"
	docker-compose up -d

stop:
	echo "Stopping micro-cluster"
	docker-compose down

destroy:
	echo "Stopping micro-cluster and removing volumes"
	docker-compose down -v
