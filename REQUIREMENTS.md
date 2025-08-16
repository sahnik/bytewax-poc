# Bytewax Data Pipeline POC
This is a POC for a near real time data pipeline using Bytewax.

## Requirements
- Python 3.12+ using Bytewax
- Kafka source
- Kafka sink
- We will want a pluggable architecture for credentials and secrets.  For POC they can be stored in a .env file
- Perform some example data standardization tasks on the data
- Perform some example data quality checks on the data.
- Perform a data lookup enrichment task using a compacted third Kafka topic.  Include code to create an in memory cache for this topic to keep throughput high.
- Include sufficent logging so we know this is working.  For example, we may want to run multiple parallel processes against a partitioned Kafka topic.
- Keep track of any interesting metrics, like throughput.
- We will need a separate utility for publish sample data to the Kafka source, with a configurable records per second.  We will also need a one-time population of the lookup topic.
