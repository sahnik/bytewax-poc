"""
Configuration Management for Bytewax Data Pipeline

This module provides centralized configuration management using environment variables
and dataclasses. Configuration is loaded from .env files and environment variables,
with sensible defaults for development and production use.

The configuration is organized into logical groups:
- KafkaConfig: Kafka broker and topic settings, consumer tuning
- ProcessingConfig: Pipeline processing parameters  
- MetricsConfig: Prometheus metrics and monitoring settings
- LoggingConfig: Structured logging configuration

Usage:
    from config.settings import get_config
    config = get_config()
    print(config.kafka.brokers)
"""

import os
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables from .env file (if present)
# This allows for easy configuration in development environments
load_dotenv()


@dataclass
class KafkaConfig:
    """
    Kafka configuration settings for brokers, topics, and consumer behavior.
    
    Attributes:
        bootstrap_servers: Comma-separated list of Kafka broker addresses
        source_topic: Input topic for raw event data
        sink_topic: Output topic for processed and enriched data  
        lookup_topic: Compacted topic containing user profile data
        error_topic: Dead letter queue for failed records
        consumer_group_main: Consumer group ID for main data processing
        consumer_group_lookup: Consumer group ID for lookup data processing
        auto_offset_reset_main: Offset reset behavior for main consumer (latest/earliest)
        auto_offset_reset_lookup: Offset reset behavior for lookup consumer (latest/earliest)
        auto_commit_enabled: Whether to automatically commit consumer offsets
        auto_commit_interval_ms: Interval for automatic offset commits
        fetch_max_wait_ms: Max time to wait for data before returning (performance tuning)
        fetch_min_bytes: Minimum bytes to wait for before returning (batching control)
    """
    bootstrap_servers: str              # Kafka broker addresses
    source_topic: str                   # Input data topic
    sink_topic: str                     # Output data topic
    lookup_topic: str                   # User profile lookup topic
    error_topic: str                    # Dead letter queue topic
    consumer_group_main: str            # Main consumer group ID
    consumer_group_lookup: str          # Lookup consumer group ID  
    auto_offset_reset_main: str         # Main offset reset strategy
    auto_offset_reset_lookup: str       # Lookup offset reset strategy
    auto_commit_enabled: bool           # Enable automatic offset commits
    auto_commit_interval_ms: int        # Offset commit interval
    fetch_max_wait_ms: int             # Fetch timeout (100ms for fast completion)
    fetch_min_bytes: int               # Minimum fetch size (1 byte for low latency)

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            source_topic=os.getenv("KAFKA_SOURCE_TOPIC", "input-topic"),
            sink_topic=os.getenv("KAFKA_SINK_TOPIC", "output-topic"),
            lookup_topic=os.getenv("KAFKA_LOOKUP_TOPIC", "lookup-topic"),
            error_topic=os.getenv("KAFKA_ERROR_TOPIC", "error-topic"),
            consumer_group_main=os.getenv("KAFKA_CONSUMER_GROUP_MAIN", "bytewax-pipeline-main"),
            consumer_group_lookup=os.getenv("KAFKA_CONSUMER_GROUP_LOOKUP", "bytewax-pipeline-lookup"),
            auto_offset_reset_main=os.getenv("KAFKA_AUTO_OFFSET_RESET_MAIN", "latest"),
            auto_offset_reset_lookup=os.getenv("KAFKA_AUTO_OFFSET_RESET_LOOKUP", "earliest"),
            auto_commit_enabled=os.getenv("KAFKA_AUTO_COMMIT_ENABLED", "true").lower() == "true",
            auto_commit_interval_ms=int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS", "1000")),
            fetch_max_wait_ms=int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "100")),
            fetch_min_bytes=int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1")),
        )

    @property
    def brokers(self) -> List[str]:
        return self.bootstrap_servers.split(",")


@dataclass
class ProcessingConfig:
    workers: int
    records_per_second: int
    cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "ProcessingConfig":
        return cls(
            workers=int(os.getenv("PIPELINE_WORKERS", "2")),
            records_per_second=int(os.getenv("RECORDS_PER_SECOND", "100")),
            cache_ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "300")),
        )


@dataclass
class MetricsConfig:
    port: int
    enabled: bool

    @classmethod
    def from_env(cls) -> "MetricsConfig":
        return cls(
            port=int(os.getenv("METRICS_PORT", "8000")),
            enabled=os.getenv("METRICS_ENABLED", "true").lower() == "true",
        )


@dataclass
class LoggingConfig:
    level: str
    format: str

    @classmethod
    def from_env(cls) -> "LoggingConfig":
        return cls(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format=os.getenv("LOG_FORMAT", "json"),
        )


@dataclass
class AppConfig:
    kafka: KafkaConfig
    processing: ProcessingConfig
    metrics: MetricsConfig
    logging: LoggingConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            kafka=KafkaConfig.from_env(),
            processing=ProcessingConfig.from_env(),
            metrics=MetricsConfig.from_env(),
            logging=LoggingConfig.from_env(),
        )


def get_config() -> AppConfig:
    return AppConfig.from_env()