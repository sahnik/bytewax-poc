import json
from typing import Dict, Any, Optional

try:
    import structlog
except ImportError:
    import logging as structlog

try:
    from bytewax.connectors.kafka import KafkaSinkMessage
except ImportError:
    # Fallback for development without bytewax installed
    class KafkaSinkMessage:
        def __init__(self, key, value, topic=None):
            self.key = key
            self.value = value
            self.topic = topic

logger = structlog.get_logger()


def serialize_to_kafka_message(data: Dict[str, Any], topic: Optional[str] = None) -> KafkaSinkMessage:
    """Serialize data to KafkaSinkMessage for output."""
    try:
        # Record output attempt
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        
        # Extract key from metadata or use a default
        key = None
        if "_kafka_metadata" in data:
            key = data["_kafka_metadata"].get("key")
        
        # Use dynamic topic if specified in data, otherwise use provided topic
        output_topic = data.get("_output_topic", topic)
        
        # Remove metadata before serialization to keep output clean
        clean_data = {k: v for k, v in data.items() if not k.startswith("_")}
        
        # Serialize to JSON
        value = json.dumps(clean_data, ensure_ascii=False)
        
        # Record successful serialization
        if output_topic and "error" in output_topic:
            metrics.record_processed("output", "error")
        else:
            metrics.record_processed("output", "success")
        
        logger.debug(
            "Serializing message for output",
            topic=output_topic,
            key=key,
            data_keys=list(clean_data.keys())
        )
        
        return KafkaSinkMessage(
            key=key,
            value=value,
            topic=output_topic
        )
        
    except Exception as e:
        # Record serialization error
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        metrics.record_processed("serialization", "error")
        metrics.record_error("serialization", type(e).__name__)
        
        logger.error(
            "Failed to serialize message",
            error=str(e),
            data=data
        )
        # Return error message
        error_data = {
            "error": f"Serialization failed: {str(e)}",
            "original_data": data
        }
        return KafkaSinkMessage(
            key=None,
            value=json.dumps(error_data),
            topic="error-topic"
        )


def create_output_message(data: Dict[str, Any], target_topic: str) -> Dict[str, Any]:
    """Add output topic routing to data."""
    data["_output_topic"] = target_topic
    return data


def create_error_output(data: Dict[str, Any], error: str, step: str) -> Dict[str, Any]:
    """Create error output routed to error topic."""
    from config.settings import get_config
    config = get_config()
    
    error_data = {
        "error": error,
        "step": step,
        "original_data": data,
        "_output_topic": config.kafka.error_topic
    }
    return error_data