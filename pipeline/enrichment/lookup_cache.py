import json
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

try:
    import structlog
except ImportError:
    import logging as structlog

logger = structlog.get_logger()


@dataclass
class CacheEntry:
    """Represents a single cache entry with TTL."""
    value: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: int = 300  # 5 minutes default
    
    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        return time.time() - self.timestamp > self.ttl_seconds


class LookupCache:
    """TTL-based lookup cache for data enrichment."""
    
    def __init__(self, default_ttl: int = 300):
        self.cache: Dict[str, CacheEntry] = {}
        self.default_ttl = default_ttl
        self.stats = {
            "hits": 0,
            "misses": 0,
            "expired": 0,
            "evictions": 0
        }
        self._last_cleanup = time.time()
        
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache if exists and not expired."""
        if key not in self.cache:
            self.stats["misses"] += 1
            logger.debug("Cache miss", key=key)
            return None
        
        entry = self.cache[key]
        
        if entry.is_expired():
            self.stats["expired"] += 1
            del self.cache[key]
            logger.debug("Cache entry expired", key=key)
            return None
        
        self.stats["hits"] += 1
        logger.debug("Cache hit", key=key)
        return entry.value
    
    def put(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """Store value in cache with TTL."""
        ttl = ttl or self.default_ttl
        self.cache[key] = CacheEntry(
            value=value,
            timestamp=time.time(),
            ttl_seconds=ttl
        )
        logger.debug("Cache put", key=key, ttl=ttl)
        
        # Periodic cleanup
        if time.time() - self._last_cleanup > 60:  # Cleanup every minute
            self._cleanup_expired()
    
    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        expired_keys = [
            key for key, entry in self.cache.items()
            if entry.is_expired()
        ]
        
        for key in expired_keys:
            del self.cache[key]
            self.stats["evictions"] += 1
        
        self._last_cleanup = time.time()
        
        if expired_keys:
            logger.debug("Cleaned up expired cache entries", count=len(expired_keys))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.stats["hits"] + self.stats["misses"]
        hit_rate = self.stats["hits"] / total_requests if total_requests > 0 else 0
        
        return {
            **self.stats,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache),
            "total_requests": total_requests
        }
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()
        logger.info("Cache cleared")


class LookupService:
    """Service for managing lookup data from Kafka topic."""
    
    def __init__(self, cache_ttl: int = 300):
        self.cache = LookupCache(cache_ttl)
        self.lookup_data: Dict[str, Dict[str, Any]] = {}
        self._initialized = False
        
    def initialize_from_topic_data(self, lookup_records: Dict[str, Dict[str, Any]]) -> None:
        """Initialize cache from compacted Kafka topic data."""
        logger.info("Initializing lookup cache", record_count=len(lookup_records))
        
        self.lookup_data = lookup_records
        
        # Pre-populate cache with all lookup data
        for key, value in lookup_records.items():
            self.cache.put(key, value)
        
        self._initialized = True
        logger.info("Lookup cache initialized", 
                   records_loaded=len(lookup_records),
                   cache_stats=self.cache.get_stats())
    
    def get_lookup_value(self, key: str) -> Optional[Dict[str, Any]]:
        """Get lookup value with cache-first strategy."""
        # First check cache
        cached_value = self.cache.get(key)
        if cached_value is not None:
            return cached_value
        
        # If not in cache but in lookup data, cache it
        if key in self.lookup_data:
            value = self.lookup_data[key]
            self.cache.put(key, value)
            return value
        
        # Not found
        logger.debug("Lookup key not found", key=key)
        return None
    
    def update_lookup_value(self, key: str, value: Dict[str, Any]) -> None:
        """Update lookup value (from new Kafka messages)."""
        self.lookup_data[key] = value
        self.cache.put(key, value)
        logger.debug("Lookup value updated", key=key)
    
    def is_initialized(self) -> bool:
        """Check if lookup service has been initialized."""
        return self._initialized
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self.cache.get_stats()


def enrich_with_lookup(cache_service: LookupService, data: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich data record using lookup cache."""
    try:
        # Get metrics collector for cache operations
        from utils.metrics import get_metrics_collector
        metrics = get_metrics_collector()
        
        # Extract lookup key from the data
        lookup_key = data.get("user_id") or data.get("customer_id") or data.get("id")
        
        if not lookup_key:
            logger.warning("No lookup key found in data", data_keys=list(data.keys()))
            return data
        
        # Get lookup value
        lookup_value = cache_service.get_lookup_value(str(lookup_key))
        
        if lookup_value:
            # Record cache hit
            metrics.record_cache_operation("get", "hit")
            
            # Add enrichment data
            data["enrichment"] = lookup_value
            data["_enrichment_metadata"] = {
                "enriched_at": datetime.now(timezone.utc).isoformat(),
                "lookup_key": str(lookup_key),
                "enriched": True
            }
            logger.debug("Data enriched successfully", lookup_key=lookup_key)
        else:
            # Record cache miss
            metrics.record_cache_operation("get", "miss")
            
            data["_enrichment_metadata"] = {
                "enriched_at": datetime.now(timezone.utc).isoformat(),
                "lookup_key": str(lookup_key),
                "enriched": False,
                "reason": "lookup_key_not_found"
            }
            logger.debug("Lookup key not found", lookup_key=lookup_key)
        
        # Update cache metrics periodically
        cache_stats = cache_service.get_cache_stats()
        metrics.update_cache_metrics(cache_stats)
        
        return data
        
    except Exception as e:
        logger.error("Failed to enrich data", error=str(e), data=data)
        data["_enrichment_error"] = str(e)
        return data


def parse_lookup_message(kafka_message: str) -> Optional[tuple[str, Dict[str, Any]]]:
    """Parse lookup message from Kafka and extract key-value pair."""
    try:
        data = json.loads(kafka_message)
        
        # Extract key - could be from different fields
        key = data.get("key") or data.get("id") or data.get("user_id")
        
        if not key:
            logger.warning("No key found in lookup message", data=data)
            return None
        
        # Remove the key field from the value to avoid duplication
        value = {k: v for k, v in data.items() if k != "key"}
        
        return str(key), value
        
    except json.JSONDecodeError as e:
        logger.error("Failed to parse lookup message JSON", error=str(e), message=kafka_message[:100])
        return None
    except Exception as e:
        logger.error("Unexpected error parsing lookup message", error=str(e), message=kafka_message[:100])
        return None