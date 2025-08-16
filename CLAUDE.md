# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a proof-of-concept for a near real-time data pipeline using Bytewax, a Python-based stream processing framework. The pipeline processes data from Kafka sources, performs transformations and enrichments, and outputs to Kafka sinks.

## Key Requirements

- **Python 3.12+** with Bytewax framework
- **Kafka integration**: Source and sink topics, plus a compacted lookup topic for enrichment
- **Data processing**: Standardization, quality checks, and enrichment via lookup cache
- **Parallel processing**: Support for multiple processes on partitioned topics
- **Metrics & Logging**: Track throughput and operational metrics

## Development Commands

### Environment Setup
```bash
# Quick setup (recommended)
python setup.py

# Manual setup
python3.12 -m venv venv
source venv/bin/activate  # On macOS/Linux
pip install -r requirements.txt
cp .env.template .env
make start
make populate-lookup
```

### Running the Pipeline
```bash
# Run main pipeline (once implemented)
python -m bytewax.run pipeline.main:flow

# Run with multiple workers for parallel processing
python -m bytewax.run pipeline.main:flow -w 4
```

### Testing
```bash
# Run tests (once test framework is set up)
pytest tests/

# Run with coverage
pytest --cov=pipeline tests/
```

### Utilities
```bash
# Publish sample data to Kafka (once implemented)
python utils/data_publisher.py --rate 100

# Populate lookup topic (once implemented)
python utils/populate_lookup.py
```

## Architecture Guidelines

### Project Structure
```
bytewax-poc/
├── pipeline/
│   ├── __init__.py
│   ├── main.py           # Main dataflow definition
│   ├── sources.py        # Kafka source connectors
│   ├── sinks.py          # Kafka sink connectors
│   ├── transformations/  # Data standardization and quality checks
│   └── enrichment.py     # Lookup cache and enrichment logic
├── utils/
│   ├── data_publisher.py # Sample data generator
│   └── populate_lookup.py # Lookup topic population
├── config/
│   └── settings.py       # Configuration management
├── tests/
├── .env                  # Local secrets (gitignored)
└── requirements.txt
```

### Key Design Patterns

1. **Pluggable Credentials**: Use environment variables via `.env` file for POC. Access through a centralized config module that can be swapped for production secrets management.

2. **In-Memory Cache**: For the lookup enrichment, implement an LRU cache that:
   - Loads from the compacted Kafka topic on startup
   - Updates as new records arrive
   - Maintains high throughput for lookups

3. **Parallel Processing**: Leverage Bytewax's built-in parallelism:
   - Use partition-aware processing
   - Ensure stateful operations are partition-key aware
   - Log worker/partition IDs for debugging

4. **Metrics Collection**: Track at minimum:
   - Records processed per second
   - Processing latency
   - Error rates
   - Cache hit rates

## Bytewax-Specific Notes

- Use `DynamicSource` and `DynamicSink` for Kafka integration
- Implement stateful operations using `StatefulMap` for the lookup cache
- Use `window` operators for any time-based aggregations
- Leverage `branch` for routing failed quality checks to error topics

## Environment Variables

Required in `.env`:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SOURCE_TOPIC=input-topic
KAFKA_SINK_TOPIC=output-topic
KAFKA_LOOKUP_TOPIC=lookup-topic
KAFKA_ERROR_TOPIC=error-topic
```

## Testing Approach

- Unit tests for individual transformation functions
- Integration tests using `bytewax.testing` helpers
- Use Kafka testcontainers for end-to-end tests
- Mock the lookup cache for isolated testing