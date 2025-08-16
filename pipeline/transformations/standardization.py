import re
import structlog
from datetime import datetime
from typing import Dict, Any, Optional, Union

logger = structlog.get_logger()


def standardize_timestamp(timestamp: Union[str, int, float]) -> Optional[str]:
    """Standardize timestamp to ISO format."""
    try:
        if isinstance(timestamp, str):
            # First try to parse ISO format (most common case)
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.isoformat()
            except ValueError:
                pass
            
            # Try to parse various string formats
            formats_to_try = [
                "%Y-%m-%dT%H:%M:%S.%f%z",      # ISO with microseconds and timezone
                "%Y-%m-%dT%H:%M:%S%z",         # ISO with timezone
                "%Y-%m-%dT%H:%M:%S.%f",        # ISO with microseconds
                "%Y-%m-%dT%H:%M:%S",           # Basic ISO
                "%Y-%m-%d %H:%M:%S.%f",        # Space separated with microseconds
                "%Y-%m-%d %H:%M:%S",           # Space separated
                "%Y-%m-%d",                    # Date only
                "%m/%d/%Y %H:%M:%S",           # US format
                "%d/%m/%Y %H:%M:%S",           # EU format
            ]
            
            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If string parsing fails, try to convert to float
            try:
                timestamp = float(timestamp)
            except ValueError:
                logger.warning("Could not parse timestamp string", timestamp=timestamp[:50])
                return None
        
        if isinstance(timestamp, (int, float)):
            # Handle Unix timestamps (both seconds and milliseconds)
            if timestamp > 1e10:  # Likely milliseconds
                timestamp = timestamp / 1000.0
            
            dt = datetime.fromtimestamp(timestamp)
            return dt.isoformat()
            
    except Exception as e:
        logger.warning("Failed to standardize timestamp", timestamp=timestamp, error=str(e))
        return None
    
    return None


def normalize_field_names(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize field names to snake_case."""
    normalized = {}
    
    for key, value in data.items():
        # Skip metadata fields
        if key.startswith("_"):
            normalized[key] = value
            continue
            
        # Convert camelCase and PascalCase to snake_case
        snake_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
        
        # Replace common patterns
        snake_key = snake_key.replace('-', '_').replace(' ', '_')
        
        # Remove duplicate underscores
        snake_key = re.sub(r'_+', '_', snake_key)
        
        normalized[snake_key] = value
    
    return normalized


def standardize_data_types(data: Dict[str, Any]) -> Dict[str, Any]:
    """Standardize common data types."""
    standardized = {}
    
    for key, value in data.items():
        # Skip metadata fields
        if key.startswith("_"):
            standardized[key] = value
            continue
        
        # Handle common type conversions
        if key.endswith(('_timestamp', '_time', '_date')):
            standardized[key] = standardize_timestamp(value)
        elif key.endswith(('_id', '_count', '_number')):
            try:
                standardized[key] = int(value) if value is not None else None
            except (ValueError, TypeError):
                standardized[key] = value
        elif key.endswith(('_amount', '_price', '_rate')):
            try:
                standardized[key] = float(value) if value is not None else None
            except (ValueError, TypeError):
                standardized[key] = value
        elif key.endswith('_email'):
            standardized[key] = value.lower() if isinstance(value, str) else value
        else:
            standardized[key] = value
    
    return standardized


def add_processing_metadata(data: Dict[str, Any]) -> Dict[str, Any]:
    """Add processing timestamp and version."""
    data["_processing_metadata"] = {
        "processed_at": datetime.utcnow().isoformat(),
        "pipeline_version": "1.0.0",
        "standardized": True
    }
    return data


def standardize_record(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply all standardization transformations to a record."""
    try:
        logger.debug("Starting record standardization", data_keys=list(data.keys()))
        
        # Apply transformations in order
        standardized = normalize_field_names(data)
        standardized = standardize_data_types(standardized)
        standardized = add_processing_metadata(standardized)
        
        logger.debug("Completed record standardization", 
                    original_keys=list(data.keys()),
                    standardized_keys=list(standardized.keys()))
        
        return standardized
        
    except Exception as e:
        logger.error("Failed to standardize record", error=str(e), data=data)
        # Return original data with error flag
        data["_standardization_error"] = str(e)
        return data