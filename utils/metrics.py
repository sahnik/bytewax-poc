import time
import threading
import structlog
from typing import Dict, Any
from prometheus_client import Counter, Histogram, Gauge, start_http_server, CollectorRegistry

logger = structlog.get_logger()


class PipelineMetrics:
    """Prometheus metrics for the data pipeline."""
    
    def __init__(self, registry: CollectorRegistry = None):
        self.registry = registry or CollectorRegistry()
        
        # Records processed counter
        self.records_processed = Counter(
            'pipeline_records_processed_total',
            'Total number of records processed',
            ['stage', 'status'],
            registry=self.registry
        )
        
        # Processing latency histogram
        self.processing_latency = Histogram(
            'pipeline_processing_duration_seconds',
            'Time spent processing records',
            ['stage'],
            registry=self.registry
        )
        
        # Cache metrics
        self.cache_operations = Counter(
            'pipeline_cache_operations_total',
            'Total cache operations',
            ['operation', 'result'],
            registry=self.registry
        )
        
        # Error counter
        self.errors = Counter(
            'pipeline_errors_total',
            'Total number of errors',
            ['stage', 'error_type'],
            registry=self.registry
        )
        
        # Active records gauge
        self.active_records = Gauge(
            'pipeline_active_records',
            'Number of records currently being processed',
            registry=self.registry
        )
        
        # Throughput gauge (records per second)
        self.throughput = Gauge(
            'pipeline_throughput_records_per_second',
            'Current throughput in records per second',
            registry=self.registry
        )
        
        # Cache size gauge
        self.cache_size = Gauge(
            'pipeline_cache_size',
            'Current number of entries in cache',
            registry=self.registry
        )
        
        # Cache hit rate gauge
        self.cache_hit_rate = Gauge(
            'pipeline_cache_hit_rate',
            'Cache hit rate as a percentage',
            registry=self.registry
        )
        
        # Throughput calculation
        self._throughput_lock = threading.Lock()
        self._record_timestamps = []
        self._throughput_window = 60  # 1 minute window
        
    def record_processed(self, stage: str, status: str = 'success'):
        """Record a processed record."""
        self.records_processed.labels(stage=stage, status=status).inc()
        
        # Update throughput
        with self._throughput_lock:
            current_time = time.time()
            self._record_timestamps.append(current_time)
            
            # Remove old timestamps outside the window
            cutoff = current_time - self._throughput_window
            self._record_timestamps = [ts for ts in self._record_timestamps if ts >= cutoff]
            
            # Calculate and update throughput
            throughput = len(self._record_timestamps) / self._throughput_window
            self.throughput.set(throughput)
    
    def record_latency(self, stage: str, duration: float):
        """Record processing latency."""
        self.processing_latency.labels(stage=stage).observe(duration)
    
    def record_cache_operation(self, operation: str, result: str):
        """Record cache operation."""
        self.cache_operations.labels(operation=operation, result=result).inc()
    
    def record_error(self, stage: str, error_type: str):
        """Record an error."""
        self.errors.labels(stage=stage, error_type=error_type).inc()
    
    def set_active_records(self, count: int):
        """Set number of active records."""
        self.active_records.set(count)
    
    def update_cache_metrics(self, cache_stats: Dict[str, Any]):
        """Update cache-related metrics."""
        self.cache_size.set(cache_stats.get('cache_size', 0))
        self.cache_hit_rate.set(cache_stats.get('hit_rate', 0) * 100)  # Convert to percentage
        
        # Update cache operation counters if available
        if 'hits' in cache_stats:
            self.cache_operations.labels(operation='get', result='hit')._value._value = cache_stats['hits']
        if 'misses' in cache_stats:
            self.cache_operations.labels(operation='get', result='miss')._value._value = cache_stats['misses']


class MetricsCollector:
    """Centralized metrics collection for the pipeline."""
    
    def __init__(self, port: int = 8000, enabled: bool = True):
        self.port = port
        self.enabled = enabled
        self.metrics = PipelineMetrics() if enabled else None
        self._server_started = False
        
    def start_server(self):
        """Start Prometheus metrics server."""
        if not self.enabled or self._server_started:
            return
            
        try:
            start_http_server(self.port, registry=self.metrics.registry)
            self._server_started = True
            logger.info("Metrics server started", port=self.port)
        except Exception as e:
            logger.error("Failed to start metrics server", error=str(e), port=self.port)
    
    def record_processed(self, stage: str, status: str = 'success'):
        """Record a processed record."""
        if self.enabled:
            self.metrics.record_processed(stage, status)
    
    def record_latency(self, stage: str, duration: float):
        """Record processing latency."""
        if self.enabled:
            self.metrics.record_latency(stage, duration)
    
    def record_cache_operation(self, operation: str, result: str):
        """Record cache operation."""
        if self.enabled:
            self.metrics.record_cache_operation(operation, result)
    
    def record_error(self, stage: str, error_type: str):
        """Record an error."""
        if self.enabled:
            self.metrics.record_error(stage, error_type)
    
    def update_cache_metrics(self, cache_stats: Dict[str, Any]):
        """Update cache-related metrics."""
        if self.enabled:
            self.metrics.update_cache_metrics(cache_stats)


class TimingContext:
    """Context manager for timing operations."""
    
    def __init__(self, metrics_collector: MetricsCollector, stage: str):
        self.metrics_collector = metrics_collector
        self.stage = stage
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.metrics_collector.record_latency(self.stage, duration)
            
            if exc_type:
                self.metrics_collector.record_error(self.stage, exc_type.__name__)


# Global metrics collector instance
_metrics_collector = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        from config.settings import get_config
        config = get_config()
        _metrics_collector = MetricsCollector(
            port=config.metrics.port,
            enabled=config.metrics.enabled
        )
        _metrics_collector.start_server()
    return _metrics_collector


def time_operation(stage: str):
    """Decorator for timing operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            metrics = get_metrics_collector()
            with TimingContext(metrics, stage):
                return func(*args, **kwargs)
        return wrapper
    return decorator