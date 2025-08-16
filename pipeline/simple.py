#!/usr/bin/env python3
"""
Simple test pipeline for debugging.
Just reads from input topic and writes to output topic.
"""

import sys
import os
import json

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import structlog
except ImportError:
    import logging as structlog

try:
    from bytewax import operators as op
    from bytewax.connectors.kafka import operators as kop
    from bytewax.dataflow import Dataflow
except ImportError as e:
    print(f"Bytewax not installed: {e}")
    print("Install with: pip install bytewax>=0.21.0")
    sys.exit(1)

from config.settings import get_config
from utils.metrics import get_metrics_collector

logger = structlog.get_logger()


def setup_logging():
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def process_message(kafka_message):
    """Simple message processing."""
    try:
        logger.info("Processing message", 
                   topic=kafka_message.topic,
                   partition=kafka_message.partition,
                   offset=kafka_message.offset)
        
        # Try to parse JSON
        if kafka_message.value:
            data = json.loads(kafka_message.value)
            data["processed"] = True
            data["pipeline"] = "simple"
            
            # Create output message
            from bytewax.connectors.kafka import KafkaSinkMessage
            return KafkaSinkMessage(
                key=kafka_message.key,
                value=json.dumps(data),
                topic=None  # Use default output topic
            )
        
        return None
        
    except Exception as e:
        logger.error("Failed to process message", error=str(e))
        return None


def create_simple_dataflow() -> Dataflow:
    """Create a simple test dataflow."""
    config = get_config()
    setup_logging()
    
    # Start metrics server
    metrics = get_metrics_collector()
    logger.info("Simple pipeline starting", 
               metrics_port=config.metrics.port,
               input_topic=config.kafka.source_topic,
               output_topic=config.kafka.sink_topic)
    
    flow = Dataflow("simple-test-pipeline")
    
    # Input stream with consumer group for offset management
    input_stream = kop.input(
        "simple-input",
        flow,
        brokers=config.kafka.brokers,
        topics=[config.kafka.source_topic],
        add_config={
            "group.id": "bytewax-simple-pipeline",
            "auto.offset.reset": "latest",  # Start from latest on first run
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": "1000"
        }
    )
    
    # Handle errors
    errors = op.inspect("input-errors", input_stream.errs)
    
    # Process messages
    processed = op.map("process", input_stream.oks, process_message)
    
    # Filter out None values
    valid = op.filter("filter-valid", processed, lambda x: x is not None)
    
    # Output (no add_config to avoid consumer property warnings)
    kop.output("simple-output", valid,
              brokers=config.kafka.brokers,
              topic=config.kafka.sink_topic)
    
    logger.info("Simple dataflow created")
    return flow


# Create the flow for bytewax.run
flow = create_simple_dataflow()