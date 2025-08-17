"""
Bytewax Data Pipeline - Main Entry Point

This module contains the main dataflow implementation for the Bytewax data pipeline.
The pipeline processes streaming data from Kafka, applies transformations, enriches
with lookup data, and outputs to Kafka topics.

Key Features:
- Real-time stream processing with Bytewax
- Kafka input/output with consumer groups for offset management  
- Data standardization and quality validation
- Lookup-based data enrichment with TTL caching
- Comprehensive metrics and structured logging
- Error handling with dead letter queues
- Multi-worker parallel processing support

Architecture:
    Kafka Input → Deserialize → Standardize → Quality Check → Enrich → Kafka Output
         ↓              ↓             ↓             ↓
    Lookup Cache   Error Handling  Metrics    Dead Letter Queue

Usage:
    Single worker:  python -m bytewax.run pipeline.main:flow
    Multi worker:   python -m bytewax.run pipeline.main:flow -w 4

Author: Claude (Anthropic)
Created: 2025-01-15
"""

import sys
import os
import time
from typing import Dict, Any, Optional

# Add the project root to Python path to enable imports from sibling directories
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
from pipeline.sources import deserialize_json_message, create_error_message
from pipeline.sinks import serialize_to_kafka_message, create_output_message, create_error_output
from pipeline.transformations.standardization import standardize_record
from pipeline.transformations.quality_checks import perform_quality_checks
from pipeline.enrichment.lookup_cache import LookupService, enrich_with_lookup, parse_lookup_message
from utils.metrics import get_metrics_collector

logger = structlog.get_logger()


# Global lookup service instance - shared across all workers
# This service manages the in-memory lookup cache with TTL expiration
# and handles updates from the lookup Kafka topic
lookup_service = LookupService()


def setup_logging():
    """
    Configure structured logging for the pipeline.
    
    Sets up JSON-formatted logging with:
    - ISO timestamps for consistent time formatting
    - Log levels and logger names
    - Exception stack traces and formatting
    - Unicode handling for international data
    
    This configuration ensures logs are machine-readable and
    can be easily parsed by log aggregation systems.
    """
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,        # Filter by configured log level
            structlog.stdlib.add_logger_name,        # Add logger name to each log entry
            structlog.stdlib.add_log_level,          # Add log level (INFO, ERROR, etc.)
            structlog.stdlib.PositionalArgumentsFormatter(),  # Handle positional args
            structlog.processors.TimeStamper(fmt="iso"),       # ISO 8601 timestamps
            structlog.processors.StackInfoRenderer(),          # Stack traces for debugging
            structlog.processors.format_exc_info,             # Exception formatting
            structlog.processors.UnicodeDecoder(),            # Handle international text
            structlog.processors.JSONRenderer()               # Output as JSON
        ],
        context_class=dict,                          # Use dict for log context
        logger_factory=structlog.stdlib.LoggerFactory(),      # Standard library integration
        wrapper_class=structlog.stdlib.BoundLogger,           # Bound logger for context
        cache_logger_on_first_use=True,             # Performance optimization
    )


def process_lookup_message(kafka_message) -> Optional[None]:
    """
    Process lookup messages from the lookup Kafka topic and update the in-memory cache.
    
    This function handles user profile updates that are used for data enrichment.
    The lookup topic is a compacted topic where the latest value for each key
    represents the current user profile.
    
    Args:
        kafka_message: Kafka message containing lookup data (key=user_id, value=profile)
        
    Returns:
        None: Lookup messages are consumed but not passed downstream
        
    Metrics Recorded:
        - lookup_update success/error counts
        - lookup_update processing latency
        - error counts by exception type
    """
    metrics = get_metrics_collector()
    start_time = time.time()
    
    try:
        # Skip null/tombstone messages (used for key deletion in compacted topics)
        if kafka_message.value is None:
            return None
            
        # Parse the lookup message (expects JSON with user profile data)
        parsed = parse_lookup_message(kafka_message.value)
        if parsed:
            key, value = parsed
            
            # Update the in-memory lookup service with new/updated profile
            lookup_service.update_lookup_value(key, value)
            logger.debug("Updated lookup cache", key=key)
            
            # Record successful lookup update metrics
            metrics.record_processed("lookup_update", "success")
            metrics.record_latency("lookup_update", time.time() - start_time)
        
        # Lookup messages are not passed to downstream processing
        return None
        
    except Exception as e:
        # Record lookup processing error metrics
        metrics.record_processed("lookup_update", "error")
        metrics.record_error("lookup_update", type(e).__name__)
        metrics.record_latency("lookup_update", time.time() - start_time)
        
        logger.error("Failed to process lookup message", error=str(e))
        return None


def process_main_stream(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Process main data stream through the complete pipeline: standardization, quality checks, and enrichment.
    
    This is the core processing function that handles the main data flow:
    1. Standardizes field names and data types
    2. Validates data quality according to business rules
    3. Enriches data with user profile information from lookup cache
    4. Routes successful records to output topic or failed records to error topic
    
    Args:
        data: Deserialized JSON record from Kafka (with ~150 fields)
        
    Returns:
        Dict containing enriched data for output topic, or
        Dict containing error information for error topic, or
        None if processing should be skipped
        
    Processing Steps:
        1. Standardization: Normalize field names (camelCase → snake_case), convert data types
        2. Quality Checks: Validate required fields, data ranges, business logic
        3. Enrichment: Look up user profile and add to record
        4. Output Routing: Tag record for appropriate output topic
        
    Metrics Recorded:
        - Per-stage processing latency (standardization, quality_checks, enrichment)
        - Per-stage success/failure counts
        - Total processing time and throughput
        - Cache hit/miss rates during enrichment
        - Error counts by type and stage
    """
    if data is None:
        return None
    
    metrics = get_metrics_collector()
    start_time = time.time()
    
    try:
        # Step 1: Data Standardization
        # Normalize field names (camelCase → snake_case) and standardize data types
        step_start = time.time()
        standardized = standardize_record(data)
        metrics.record_latency("standardization", time.time() - step_start)
        metrics.record_processed("standardization", "success")
        
        # Step 2: Quality Validation
        # Apply business rules and data quality checks
        step_start = time.time()
        validated = perform_quality_checks(standardized)
        metrics.record_latency("quality_checks", time.time() - step_start)
        
        if validated is None:
            # Quality check failed - route to error topic
            metrics.record_processed("quality_checks", "failed")
            metrics.record_error("quality_checks", "validation_failed")
            error_msg = create_error_output(
                standardized, 
                "Quality check failed", 
                "quality_validation"
            )
            return error_msg
        
        metrics.record_processed("quality_checks", "success")
        
        # Step 3: Data Enrichment
        # Add user profile information from lookup cache
        step_start = time.time()
        enriched = enrich_with_lookup(lookup_service, validated)
        metrics.record_latency("enrichment", time.time() - step_start)
        metrics.record_processed("enrichment", "success")
        
        # Step 4: Output Routing
        # Tag record for main output topic
        from config.settings import get_config
        config = get_config()
        output_data = create_output_message(enriched, config.kafka.sink_topic)
        
        # Record overall processing metrics for throughput calculation
        total_duration = time.time() - start_time
        metrics.record_latency("total_processing", total_duration)
        metrics.record_processed("total_processing", "success")
        
        logger.debug("Successfully processed record", 
                    record_id=enriched.get("id"),
                    enriched=enriched.get("_enrichment_metadata", {}).get("enriched", False),
                    processing_time_ms=round(total_duration * 1000, 2))
        
        return output_data
        
    except Exception as e:
        # Record error metrics for any unexpected failures
        total_duration = time.time() - start_time
        metrics.record_latency("total_processing", total_duration)
        metrics.record_processed("total_processing", "error")
        metrics.record_error("main_processing", type(e).__name__)
        
        logger.error("Failed to process record", error=str(e), data=data)
        return create_error_output(data, str(e), "main_processing")


def create_dataflow() -> Dataflow:
    """
    Create and configure the main Bytewax dataflow with all processing stages.
    
    This function sets up the complete data pipeline including:
    - Kafka input streams (main data + lookup data)
    - Processing operators (deserialize, standardize, validate, enrich)
    - Kafka output streams (success + error topics)
    - Metrics collection and structured logging
    - Error handling and dead letter queues
    
    The dataflow uses Bytewax operators to create a processing graph that can
    be executed across multiple workers for parallel processing. Consumer groups
    ensure proper offset management and prevent duplicate processing.
    
    Returns:
        Dataflow: Configured Bytewax dataflow ready for execution
        
    Kafka Topics Used:
        - Input Topic: Raw event data (8 partitions for parallel processing)
        - Output Topic: Successfully processed and enriched data (8 partitions)
        - Error Topic: Failed records with error details (1 partition)
        - Lookup Topic: User profile data for enrichment (compacted, 1 partition)
        
    Performance Optimizations:
        - Consumer fetch timeout: 100ms (vs 500ms default) for faster completion
        - Consumer groups prevent duplicate processing across workers
        - TTL cache reduces lookup latency
        - Parallel processing across multiple workers/partitions
    """
    config = get_config()
    setup_logging()
    
    # Initialize metrics collection server (Prometheus endpoint)
    metrics = get_metrics_collector()
    logger.info("Metrics server started", port=config.metrics.port, enabled=config.metrics.enabled)
    
    logger.info("Creating dataflow", 
               brokers=config.kafka.brokers,
               source_topic=config.kafka.source_topic,
               sink_topic=config.kafka.sink_topic,
               lookup_topic=config.kafka.lookup_topic)
    
    # Create the main dataflow with a descriptive name
    flow = Dataflow("bytewax-data-pipeline")
    
    # ===== INPUT STREAMS =====
    
    # Main data input stream: Processes raw event data from producers
    # Uses consumer group to track offsets and prevent duplicate processing
    # fetch.wait.max.ms=100ms ensures fast completion when producer stops
    main_stream = kop.input(
        "main-input",  # Unique operator name for this input stream
        flow, 
        brokers=config.kafka.brokers, 
        topics=[config.kafka.source_topic],  # Input topic with 8 partitions
        add_config={
            # Consumer group settings for offset management
            "group.id": config.kafka.consumer_group_main,
            "client.id": f"bytewax-main-{os.environ.get('POD_NAME', os.environ.get('HOSTNAME', 'worker'))}",
            "auto.offset.reset": config.kafka.auto_offset_reset_main,  # latest/earliest
            "enable.auto.commit": str(config.kafka.auto_commit_enabled).lower(),
            "auto.commit.interval.ms": str(config.kafka.auto_commit_interval_ms),
            
            # Consumer group coordination settings
            "session.timeout.ms": "30000",     # 30 seconds for group coordination
            "heartbeat.interval.ms": "10000",  # 10 seconds heartbeat
            "max.poll.interval.ms": "300000",  # 5 minutes max between polls
            
            # Performance tuning: Reduced from 500ms default for faster completion
            "fetch.wait.max.ms": str(config.kafka.fetch_max_wait_ms),  # 100ms
            "fetch.min.bytes": str(config.kafka.fetch_min_bytes),     # 1 byte
            
            # Security configuration for external Kafka
            **config.kafka.get_security_config()
        }
    )
    
    # Lookup data input stream: Processes user profile updates
    # Separate consumer group to independently track lookup data consumption
    # Compacted topic ensures we get the latest profile for each user
    lookup_stream = kop.input(
        "lookup-input",  # Unique operator name for lookup stream
        flow,
        brokers=config.kafka.brokers,
        topics=[config.kafka.lookup_topic],  # Lookup topic (compacted, 1 partition)
        add_config={
            # Separate consumer group for lookup data
            "group.id": config.kafka.consumer_group_lookup,
            "client.id": f"bytewax-lookup-{os.environ.get('HOSTNAME', 'unknown')}",
            "auto.offset.reset": config.kafka.auto_offset_reset_lookup,  # earliest
            "enable.auto.commit": str(config.kafka.auto_commit_enabled).lower(),
            "auto.commit.interval.ms": str(config.kafka.auto_commit_interval_ms),
            
            # Consumer group coordination settings
            "session.timeout.ms": "30000",     # 30 seconds for group coordination
            "heartbeat.interval.ms": "10000",  # 10 seconds heartbeat
            "max.poll.interval.ms": "300000",  # 5 minutes max between polls
            
            # Same performance tuning as main stream
            "fetch.wait.max.ms": str(config.kafka.fetch_max_wait_ms),
            "fetch.min.bytes": str(config.kafka.fetch_min_bytes),
            
            # Security configuration for external Kafka
            **config.kafka.get_security_config()
        }
    )
    
    # ===== ERROR HANDLING =====
    
    # Log Kafka connection errors for debugging
    # These errors typically indicate network issues or broker problems
    main_errors = op.inspect("main-input-errors", main_stream.errs)
    lookup_errors = op.inspect("lookup-input-errors", lookup_stream.errs)
    
    # ===== LOOKUP PROCESSING =====
    
    # Process lookup messages to update in-memory cache
    # These messages update user profiles used for data enrichment
    # Returns None (doesn't continue downstream) - updates global lookup_service
    lookup_processed = op.map("process-lookup", lookup_stream.oks, process_lookup_message)
    
    # ===== MAIN DATA PROCESSING PIPELINE =====
    
    # Step 1: Deserialize JSON messages from Kafka
    # Converts raw bytes to Python dictionaries and adds Kafka metadata
    main_deserialized = op.map("deserialize-main", main_stream.oks, deserialize_json_message)
    
    # Step 2: Filter out deserialization failures
    # Invalid JSON or null messages are dropped here
    main_valid = op.filter("filter-valid", main_deserialized, lambda x: x is not None)
    
    # Step 3: Core processing pipeline
    # Applies standardization → quality checks → enrichment → routing
    processed = op.map("process-main", main_valid, process_main_stream)
    
    # Step 4: Filter out processing failures
    # Records that failed core processing are dropped (already routed to error topic)
    processed_valid = op.filter("filter-processed", processed, lambda x: x is not None)
    
    # Step 5: Serialize for Kafka output
    # Converts Python dictionaries back to JSON and creates Kafka messages
    serialized = op.map("serialize", processed_valid, 
                       lambda data: serialize_to_kafka_message(data))
    
    # ===== OUTPUT ROUTING =====
    
    # Route messages to appropriate output topics based on processing results
    # Messages are tagged during processing with their destination topic
    
    # Main output topic: Successfully processed and enriched records
    # Filters for records destined for the main output topic (or unspecified)
    main_output = op.filter("main-output-filter", serialized, 
                           lambda msg: msg.topic == config.kafka.sink_topic or msg.topic is None)
    kop.output("kafka-main-output", main_output, 
              brokers=config.kafka.brokers, 
              topic=config.kafka.sink_topic,
              add_config=config.kafka.get_security_config())  # 8 partitions for parallel consumption
    
    # Error output topic: Failed records with error details
    # Dead letter queue for records that failed processing at any stage
    error_output = op.filter("error-output-filter", serialized,
                            lambda msg: msg.topic == config.kafka.error_topic)
    kop.output("kafka-error-output", error_output,
              brokers=config.kafka.brokers,
              topic=config.kafka.error_topic,
              add_config=config.kafka.get_security_config())  # 1 partition (errors are less frequent)
    
    logger.info("Dataflow created successfully")
    return flow


# ===== DATAFLOW INSTANCE =====

# Create the flow instance for bytewax.run execution
# This is the entry point that Bytewax looks for when running the pipeline
# Usage: python -m bytewax.run pipeline.main:flow [-w num_workers]
flow = create_dataflow()