import pytest
import time
from pipeline.enrichment.lookup_cache import (
    CacheEntry,
    LookupCache,
    LookupService,
    enrich_with_lookup,
    parse_lookup_message
)


class TestCacheEntry:
    """Test CacheEntry functionality."""
    
    def test_cache_entry_not_expired(self):
        """Test cache entry within TTL."""
        entry = CacheEntry({"data": "test"}, ttl_seconds=60)
        assert not entry.is_expired()
    
    def test_cache_entry_expired(self):
        """Test cache entry beyond TTL."""
        entry = CacheEntry({"data": "test"}, timestamp=time.time() - 70, ttl_seconds=60)
        assert entry.is_expired()


class TestLookupCache:
    """Test LookupCache functionality."""
    
    def test_cache_put_and_get(self):
        """Test basic cache put and get operations."""
        cache = LookupCache()
        
        cache.put("user_123", {"name": "John", "segment": "premium"})
        result = cache.get("user_123")
        
        assert result is not None
        assert result["name"] == "John"
        assert result["segment"] == "premium"
    
    def test_cache_miss(self):
        """Test cache miss for non-existent key."""
        cache = LookupCache()
        result = cache.get("non_existent")
        
        assert result is None
        assert cache.stats["misses"] == 1
    
    def test_cache_hit_stats(self):
        """Test cache hit statistics."""
        cache = LookupCache()
        
        cache.put("user_123", {"name": "John"})
        result = cache.get("user_123")
        
        assert result is not None
        assert cache.stats["hits"] == 1
        assert cache.stats["misses"] == 0
    
    def test_cache_expiration(self):
        """Test cache entry expiration."""
        cache = LookupCache(default_ttl=1)  # 1 second TTL
        
        cache.put("user_123", {"name": "John"})
        
        # Should be available immediately
        result = cache.get("user_123")
        assert result is not None
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired now
        result = cache.get("user_123")
        assert result is None
        assert cache.stats["expired"] == 1
    
    def test_cache_stats(self):
        """Test cache statistics calculation."""
        cache = LookupCache()
        
        # Add some data
        cache.put("user_123", {"name": "John"})
        cache.put("user_456", {"name": "Jane"})
        
        # Mix of hits and misses
        cache.get("user_123")  # hit
        cache.get("user_456")  # hit
        cache.get("user_789")  # miss
        
        stats = cache.get_stats()
        
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 2/3
        assert stats["cache_size"] == 2
        assert stats["total_requests"] == 3


class TestLookupService:
    """Test LookupService functionality."""
    
    def test_lookup_service_initialization(self):
        """Test lookup service initialization from topic data."""
        service = LookupService()
        
        lookup_data = {
            "user_123": {"name": "John", "segment": "premium"},
            "user_456": {"name": "Jane", "segment": "standard"}
        }
        
        service.initialize_from_topic_data(lookup_data)
        
        assert service.is_initialized()
        
        # Test retrieval
        result = service.get_lookup_value("user_123")
        assert result is not None
        assert result["name"] == "John"
        
        result = service.get_lookup_value("user_789")
        assert result is None
    
    def test_lookup_service_update(self):
        """Test lookup service value updates."""
        service = LookupService()
        service.initialize_from_topic_data({})
        
        # Add new value
        service.update_lookup_value("user_123", {"name": "John", "segment": "premium"})
        
        result = service.get_lookup_value("user_123")
        assert result is not None
        assert result["name"] == "John"
        
        # Update existing value
        service.update_lookup_value("user_123", {"name": "John", "segment": "vip"})
        
        result = service.get_lookup_value("user_123")
        assert result["segment"] == "vip"
    
    def test_lookup_service_cache_behavior(self):
        """Test that lookup service uses cache correctly."""
        service = LookupService(cache_ttl=60)
        
        lookup_data = {
            "user_123": {"name": "John", "segment": "premium"}
        }
        service.initialize_from_topic_data(lookup_data)
        
        # First access should populate cache
        result1 = service.get_lookup_value("user_123")
        
        # Second access should come from cache
        result2 = service.get_lookup_value("user_123")
        
        assert result1 == result2
        
        # Check cache stats show hits
        stats = service.get_cache_stats()
        assert stats["hits"] >= 1


class TestEnrichment:
    """Test data enrichment functionality."""
    
    def test_enrich_with_lookup_success(self):
        """Test successful data enrichment."""
        service = LookupService()
        lookup_data = {
            "user_123": {"name": "John Doe", "segment": "premium", "country": "US"}
        }
        service.initialize_from_topic_data(lookup_data)
        
        data = {
            "id": "event_456",
            "user_id": "user_123",
            "event_type": "purchase"
        }
        
        result = enrich_with_lookup(service, data)
        
        assert "enrichment" in result
        assert result["enrichment"]["name"] == "John Doe"
        assert result["enrichment"]["segment"] == "premium"
        
        assert "_enrichment_metadata" in result
        assert result["_enrichment_metadata"]["enriched"] is True
        assert result["_enrichment_metadata"]["lookup_key"] == "user_123"
    
    def test_enrich_with_lookup_not_found(self):
        """Test enrichment when lookup key not found."""
        service = LookupService()
        service.initialize_from_topic_data({})
        
        data = {
            "id": "event_456",
            "user_id": "user_999",
            "event_type": "purchase"
        }
        
        result = enrich_with_lookup(service, data)
        
        assert "enrichment" not in result
        assert "_enrichment_metadata" in result
        assert result["_enrichment_metadata"]["enriched"] is False
        assert result["_enrichment_metadata"]["reason"] == "lookup_key_not_found"
    
    def test_enrich_with_lookup_no_key(self):
        """Test enrichment when no lookup key found in data."""
        service = LookupService()
        service.initialize_from_topic_data({})
        
        data = {
            "id": "event_456",
            "event_type": "purchase"
            # No user_id, customer_id, or id fields for lookup
        }
        
        result = enrich_with_lookup(service, data)
        
        # Should return original data unchanged (except for any error metadata)
        assert result["id"] == "event_456"
        assert result["event_type"] == "purchase"


class TestLookupMessageParsing:
    """Test lookup message parsing functionality."""
    
    def test_parse_lookup_message_with_key(self):
        """Test parsing lookup message with explicit key field."""
        message = '{"key": "user_123", "name": "John", "segment": "premium"}'
        
        result = parse_lookup_message(message)
        
        assert result is not None
        key, value = result
        assert key == "user_123"
        assert value["name"] == "John"
        assert value["segment"] == "premium"
        assert "key" not in value  # Key should be removed from value
    
    def test_parse_lookup_message_with_id(self):
        """Test parsing lookup message with id field as key."""
        message = '{"id": "user_456", "name": "Jane", "segment": "standard"}'
        
        result = parse_lookup_message(message)
        
        assert result is not None
        key, value = result
        assert key == "user_456"
        assert value["name"] == "Jane"
        assert "id" not in value  # id should be removed from value
    
    def test_parse_lookup_message_invalid_json(self):
        """Test parsing invalid JSON message."""
        message = '{"invalid": json}'
        
        result = parse_lookup_message(message)
        
        assert result is None
    
    def test_parse_lookup_message_no_key(self):
        """Test parsing message without key or id field."""
        message = '{"name": "John", "segment": "premium"}'
        
        result = parse_lookup_message(message)
        
        assert result is None