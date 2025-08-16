import json
from typing import Dict, Any, Optional

try:
    import structlog
except ImportError:
    import logging as structlog

try:
    from bytewax.connectors.kafka import KafkaSourceMessage
except ImportError:
    # Fallback for development without bytewax installed
    class KafkaSourceMessage:
        def __init__(self, key, value, topic=None, partition=None, offset=None, timestamp=None, headers=None):
            self.key = key
            self.value = value
            self.topic = topic
            self.partition = partition
            self.offset = offset
            self.timestamp = timestamp
            self.headers = headers

logger = structlog.get_logger()


def deserialize_json_message(kafka_message: KafkaSourceMessage) -> Optional[Dict[str, Any]]:
    """Deserialize JSON from Kafka message value."""
    try:
        # Record input message received
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        metrics.record_processed("input", "received")
        
        if kafka_message.value is None:
            logger.warning("Received null message value", key=kafka_message.key)
            metrics.record_processed("deserialization", "null_value")
            return None
            
        data = json.loads(kafka_message.value)
        
        # Add metadata to the message
        data["_kafka_metadata"] = {
            "key": kafka_message.key,
            "topic": kafka_message.topic,
            "partition": kafka_message.partition,
            "offset": kafka_message.offset,
            "timestamp": kafka_message.timestamp,
            "headers": dict(kafka_message.headers) if kafka_message.headers else {}
        }
        
        # Record successful deserialization
        metrics.record_processed("deserialization", "success")
        
        logger.debug(
            "Deserialized message",
            topic=kafka_message.topic,
            partition=kafka_message.partition,
            offset=kafka_message.offset,
            data_keys=list(data.keys())
        )
        
        return data
        
    except json.JSONDecodeError as e:
        # Record deserialization error
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        metrics.record_processed("deserialization", "json_error")
        metrics.record_error("deserialization", "JSONDecodeError")
        
        logger.error(
            "Failed to deserialize JSON message",
            error=str(e),
            topic=kafka_message.topic,
            partition=kafka_message.partition,
            offset=kafka_message.offset,
            raw_value=kafka_message.value[:100] if kafka_message.value else None
        )
        return None
    except Exception as e:
        # Record unexpected error
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        metrics.record_processed("deserialization", "error")
        metrics.record_error("deserialization", type(e).__name__)
        
        logger.error(
            "Unexpected error deserializing message",
            error=str(e),
            topic=kafka_message.topic,
            partition=kafka_message.partition,
            offset=kafka_message.offset
        )
        return None


def create_error_message(original_data: Dict[str, Any], error: str, step: str) -> Dict[str, Any]:
    """Create an error message for failed processing."""
    return {
        "error": error,
        "step": step,
        "original_data": original_data,
        "timestamp": original_data.get("_kafka_metadata", {}).get("timestamp"),
        "_error_metadata": {
            "source_topic": original_data.get("_kafka_metadata", {}).get("topic"),
            "source_partition": original_data.get("_kafka_metadata", {}).get("partition"),
            "source_offset": original_data.get("_kafka_metadata", {}).get("offset")
        }
    }