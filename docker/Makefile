# EYWA Core - Docker Makefile
# Convenience commands for managing Docker deployment

.PHONY: help build up down restart logs clean test

help:
	@echo "EYWA Core Docker Commands"
	@echo "========================="
	@echo "make build      - Build EYWA Docker image"
	@echo "make up         - Start all services"
	@echo "make down       - Stop all services"
	@echo "make restart    - Restart all services"
	@echo "make logs       - View logs (real-time)"
	@echo "make clean      - Stop and remove all data (⚠️  destructive)"
	@echo "make test       - Test the deployment"
	@echo "make backup     - Backup database and data"
	@echo "make shell-eywa - Open shell in EYWA container"
	@echo "make shell-db   - Open psql in PostgreSQL"

build:
	@echo "Building EYWA Docker image..."
	docker-compose build

up:
	@echo "Starting EYWA services..."
	docker-compose up -d
	@echo ""
	@echo "EYWA is starting..."
	@echo "Access at: http://localhost:8080/eywa/"
	@echo "GraphiQL: http://localhost:8080/graphiql/"
	@echo ""
	@echo "Default credentials:"
	@echo "  Username: admin"
	@echo "  Password: changeme123"

down:
	@echo "Stopping EYWA services..."
	docker-compose down

restart:
	@echo "Restarting EYWA services..."
	docker-compose restart

logs:
	docker-compose logs -f

clean:
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Are you sure? (y/N) " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		echo "All data removed."; \
	fi

test:
	@echo "Testing EYWA deployment..."
	@echo -n "PostgreSQL: "
	@docker exec eywa-postgres pg_isready -U eywa && echo "✓ OK" || echo "✗ FAIL"
	@echo -n "EYWA Health: "
	@curl -sf http://localhost:8080/health > /dev/null && echo "✓ OK" || echo "✗ FAIL"
	@echo -n "EYWA UI: "
	@curl -sf http://localhost:8080/eywa/ > /dev/null && echo "✓ OK" || echo "✗ FAIL"

backup:
	@echo "Creating backup..."
	@mkdir -p backups
	@echo "Backing up PostgreSQL..."
	@docker exec eywa-postgres pg_dump -U eywa eywa > backups/eywa-db-$$(date +%Y%m%d-%H%M%S).sql
	@echo "Backing up EYWA data..."
	@docker cp eywa-core:/data/eywa backups/eywa-data-$$(date +%Y%m%d-%H%M%S)
	@echo "Backup complete in backups/"

shell-eywa:
	@docker exec -it eywa-core /bin/bash

shell-db:
	@docker exec -it eywa-postgres psql -U eywa eywa

status:
	@docker-compose ps
