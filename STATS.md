This is with a producer writing 1000 records per second.  Pipeline is running on 2 pods with 1 Bytewax worker on each (1 worker per pod is best practice with Kafka)
# HELP pipeline_records_processed_total Total number of records processed
# TYPE pipeline_records_processed_total counter
pipeline_records_processed_total{stage="input",status="received"} 44328.0
pipeline_records_processed_total{stage="deserialization",status="success"} 44304.0
pipeline_records_processed_total{stage="standardization",status="success"} 44304.0
pipeline_records_processed_total{stage="quality_checks",status="success"} 44287.0
pipeline_records_processed_total{stage="enrichment",status="success"} 44287.0
pipeline_records_processed_total{stage="total_processing",status="success"} 44287.0
pipeline_records_processed_total{stage="output",status="success"} 44287.0
pipeline_records_processed_total{stage="lookup_update",status="success"} 1000.0
pipeline_records_processed_total{stage="deserialization",status="json_error"} 24.0
pipeline_records_processed_total{stage="quality_checks",status="failed"} 17.0
pipeline_records_processed_total{stage="output",status="error"} 17.0
# HELP pipeline_records_processed_created Total number of records processed
# TYPE pipeline_records_processed_created gauge
pipeline_records_processed_created{stage="input",status="received"} 1.7553949649930403e+09
pipeline_records_processed_created{stage="deserialization",status="success"} 1.7553949649931726e+09
pipeline_records_processed_created{stage="standardization",status="success"} 1.7553949650269265e+09
pipeline_records_processed_created{stage="quality_checks",status="success"} 1.7553949650271785e+09
pipeline_records_processed_created{stage="enrichment",status="success"} 1.7553949650273898e+09
pipeline_records_processed_created{stage="total_processing",status="success"} 1.7553949650275264e+09
pipeline_records_processed_created{stage="output",status="success"} 1.755394965566005e+09
pipeline_records_processed_created{stage="lookup_update",status="success"} 1.7553949658819783e+09
pipeline_records_processed_created{stage="deserialization",status="json_error"} 1.755394966774327e+09
pipeline_records_processed_created{stage="quality_checks",status="failed"} 1.755394981574198e+09
pipeline_records_processed_created{stage="output",status="error"} 1.755395019361621e+09
# HELP pipeline_processing_duration_seconds Time spent processing records
# TYPE pipeline_processing_duration_seconds histogram
pipeline_processing_duration_seconds_bucket{le="0.005",stage="standardization"} 44290.0
pipeline_processing_duration_seconds_bucket{le="0.01",stage="standardization"} 44298.0
pipeline_processing_duration_seconds_bucket{le="0.025",stage="standardization"} 44299.0
pipeline_processing_duration_seconds_bucket{le="0.05",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.075",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.1",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.25",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.5",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.75",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="1.0",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="2.5",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="5.0",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="7.5",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="10.0",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_bucket{le="+Inf",stage="standardization"} 44304.0
pipeline_processing_duration_seconds_count{stage="standardization"} 44304.0
pipeline_processing_duration_seconds_sum{stage="standardization"} 30.009090185165405
pipeline_processing_duration_seconds_bucket{le="0.005",stage="quality_checks"} 44286.0
pipeline_processing_duration_seconds_bucket{le="0.01",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.025",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.05",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.075",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.1",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.25",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.5",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="0.75",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="1.0",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="2.5",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="5.0",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="7.5",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="10.0",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_bucket{le="+Inf",stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_count{stage="quality_checks"} 44304.0
pipeline_processing_duration_seconds_sum{stage="quality_checks"} 2.2255778312683105
pipeline_processing_duration_seconds_bucket{le="0.005",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.01",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.025",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.05",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.075",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.1",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.25",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.5",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.75",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="1.0",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="2.5",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="5.0",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="7.5",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="10.0",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_bucket{le="+Inf",stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_count{stage="enrichment"} 44287.0
pipeline_processing_duration_seconds_sum{stage="enrichment"} 2.564544916152954
pipeline_processing_duration_seconds_bucket{le="0.005",stage="total_processing"} 28178.0
pipeline_processing_duration_seconds_bucket{le="0.01",stage="total_processing"} 44231.0
pipeline_processing_duration_seconds_bucket{le="0.025",stage="total_processing"} 44278.0
pipeline_processing_duration_seconds_bucket{le="0.05",stage="total_processing"} 44286.0
pipeline_processing_duration_seconds_bucket{le="0.075",stage="total_processing"} 44286.0
pipeline_processing_duration_seconds_bucket{le="0.1",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.25",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.5",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="0.75",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="1.0",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="2.5",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="5.0",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="7.5",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="10.0",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_bucket{le="+Inf",stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_count{stage="total_processing"} 44287.0
pipeline_processing_duration_seconds_sum{stage="total_processing"} 206.2324481010437
pipeline_processing_duration_seconds_bucket{le="0.005",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.01",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.025",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.05",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.075",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.1",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.25",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.5",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="0.75",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="1.0",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="2.5",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="5.0",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="7.5",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="10.0",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_bucket{le="+Inf",stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_count{stage="lookup_update"} 1000.0
pipeline_processing_duration_seconds_sum{stage="lookup_update"} 0.10710930824279785
# HELP pipeline_processing_duration_seconds_created Time spent processing records
# TYPE pipeline_processing_duration_seconds_created gauge
pipeline_processing_duration_seconds_created{stage="standardization"} 1.7553949650268643e+09
pipeline_processing_duration_seconds_created{stage="quality_checks"} 1.7553949650271335e+09
pipeline_processing_duration_seconds_created{stage="enrichment"} 1.7553949650273192e+09
pipeline_processing_duration_seconds_created{stage="total_processing"} 1.7553949650274885e+09
pipeline_processing_duration_seconds_created{stage="lookup_update"} 1.7553949658821087e+09
# HELP pipeline_cache_operations_total Total cache operations
# TYPE pipeline_cache_operations_total counter
pipeline_cache_operations_total{operation="get",result="miss"} 1269.0
pipeline_cache_operations_total{operation="get",result="hit"} 43017.0
# HELP pipeline_cache_operations_created Total cache operations
# TYPE pipeline_cache_operations_created gauge
pipeline_cache_operations_created{operation="get",result="miss"} 1.7553949650272586e+09
pipeline_cache_operations_created{operation="get",result="hit"} 1.7553949650273032e+09
# HELP pipeline_errors_total Total number of errors
# TYPE pipeline_errors_total counter
pipeline_errors_total{error_type="JSONDecodeError",stage="deserialization"} 24.0
pipeline_errors_total{error_type="validation_failed",stage="quality_checks"} 17.0
# HELP pipeline_errors_created Total number of errors
# TYPE pipeline_errors_created gauge
pipeline_errors_created{error_type="JSONDecodeError",stage="deserialization"} 1.755394966774639e+09
pipeline_errors_created{error_type="validation_failed",stage="quality_checks"} 1.7553949815750864e+09
# HELP pipeline_active_records Number of records currently being processed
# TYPE pipeline_active_records gauge
pipeline_active_records 0.0
# HELP pipeline_throughput_records_per_second Current throughput in records per second
# TYPE pipeline_throughput_records_per_second gauge
pipeline_throughput_records_per_second 620.3166666666667
# HELP pipeline_cache_size Current number of entries in cache
# TYPE pipeline_cache_size gauge
pipeline_cache_size 1000.0
# HELP pipeline_cache_hit_rate Cache hit rate as a percentage
# TYPE pipeline_cache_hit_rate gauge
pipeline_cache_hit_rate 97.1345346159057