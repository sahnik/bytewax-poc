.PHONY: help install start stop clean test lint pipeline publish-data populate-lookup logs

# Default target
help:
	@echo "Bytewax Data Pipeline POC"
	@echo "========================"
	@echo ""
	@echo "Available targets:"
	@echo "  install        Install Python dependencies"
	@echo "  start          Start Kafka infrastructure"
	@echo "  stop           Stop Kafka infrastructure"
	@echo "  clean          Clean up containers and volumes"
	@echo "  test           Run test suite"
	@echo "  lint           Run code linting and formatting"
	@echo "  pipeline       Run the data pipeline"
	@echo "  pipeline-multi Run the data pipeline with 4 workers"
	@echo "  publish-data   Publish sample data to Kafka"
	@echo "  populate-lookup Populate lookup topic with sample data"
	@echo "  logs           Show Kafka container logs"
	@echo "  check-partitions Check topic partition counts"
	@echo "  update-partitions Update topic partitions for multi-worker support"
	@echo "  metrics        Open metrics dashboard"
	@echo "  kafka-ui       Open Kafka UI"

# Python environment setup
install:
	pip install -r requirements.txt

# Infrastructure management
start:
	docker-compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@echo "Kafka infrastructure started!"
	@echo "Kafka UI: http://localhost:8080"

stop:
	docker-compose down

clean:
	docker-compose down -v
	docker system prune -f

# Development and testing
test:
	pytest tests/ -v

test-cov:
	pytest --cov=pipeline --cov=utils --cov-report=html tests/

lint:
	black pipeline/ utils/ tests/ config/
	ruff check pipeline/ utils/ tests/ config/

# Pipeline operations
pipeline:
	python -m bytewax.run pipeline.main:flow

pipeline-multi:
	python -m bytewax.run pipeline.main:flow -w 4

# Data management
publish-data:
	PYTHONPATH=. python utils/data_publisher.py --rate 10

publish-data-fast:
	PYTHONPATH=. python utils/data_publisher.py --rate 100

publish-data-very-fast:
	PYTHONPATH=. python utils/data_publisher.py --rate 1000

publish-data-crazy-fast:
	PYTHONPATH=. python utils/data_publisher.py --rate 2500

publish-batch:
	PYTHONPATH=. python utils/data_publisher.py --batch 1000

show-sample:
	PYTHONPATH=. python utils/show_sample.py

count-fields:
	PYTHONPATH=. python utils/count_fields.py

populate-lookup:
	PYTHONPATH=. python utils/populate_lookup.py --count 1000

# Monitoring and debugging
logs:
	docker-compose logs -f kafka

kafka-logs:
	docker-compose logs -f kafka

check-offsets:
	PYTHONPATH=. python utils/check_offsets.py

reset-offsets-main:
	PYTHONPATH=. python utils/check_offsets.py --reset main --reset-to latest

reset-offsets-lookup:
	PYTHONPATH=. python utils/check_offsets.py --reset lookup --reset-to earliest

update-partitions:
	PYTHONPATH=. python utils/update_partitions.py

check-partitions:
	PYTHONPATH=. python utils/update_partitions.py --check-only

metrics:
	@echo "Opening metrics at http://localhost:8000"
	@python -c "import webbrowser; webbrowser.open('http://localhost:8000')"

kafka-ui:
	@echo "Opening Kafka UI at http://localhost:8080"
	@python -c "import webbrowser; webbrowser.open('http://localhost:8080')"

# Environment setup
setup: install start populate-lookup
	@echo "Environment setup complete!"
	@echo "Run 'make pipeline' to start the data pipeline"
	@echo "Run 'make publish-data' to start publishing sample data"

# Development workflow
dev: start
	@echo "Starting development environment..."
	@echo "Terminal 1: make pipeline"
	@echo "Terminal 2: make publish-data"
	@echo "Terminal 3: make kafka-ui (for monitoring)"

# Quick demo
demo: setup
	@echo "Starting demo..."
	python utils/data_publisher.py --batch 100 &
	sleep 2
	python -m bytewax.run pipeline.main:flow