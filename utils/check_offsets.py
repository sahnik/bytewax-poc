#!/usr/bin/env python3
"""
Utility to check Kafka consumer group offsets.
Helps verify that offset management is working correctly.
"""

import sys
import os
import argparse
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import Consumer

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_config

try:
    import structlog
    logger = structlog.get_logger()
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


def check_consumer_groups():
    """Check status of Bytewax consumer groups."""
    config = get_config()
    
    print("🔍 Checking Kafka Consumer Groups")
    print("=" * 50)
    
    groups_to_check = [
        config.kafka.consumer_group_main,
        config.kafka.consumer_group_lookup,
        "bytewax-simple-pipeline"
    ]
    
    for group_id in groups_to_check:
        print(f"\n📊 Consumer Group: {group_id}")
        print("-" * 30)
        
        try:
            # Create consumer to check offsets
            consumer = Consumer({
                'bootstrap.servers': ','.join(config.kafka.brokers),
                'group.id': group_id,
                'auto.offset.reset': 'latest'
            })
            
            # Get committed offsets for topics
            topics_to_check = [config.kafka.source_topic, config.kafka.lookup_topic]
            
            for topic in topics_to_check:
                try:
                    # Get topic partitions
                    topic_metadata = consumer.list_topics(topic, timeout=5)
                    if topic in topic_metadata.topics:
                        partitions = topic_metadata.topics[topic].partitions
                        
                        print(f"  Topic: {topic}")
                        for partition_id in partitions:
                            # Get committed offset
                            from confluent_kafka import TopicPartition
                            tp = TopicPartition(topic, partition_id)
                            committed = consumer.committed([tp], timeout=5)
                            
                            if committed and len(committed) > 0:
                                offset = committed[0].offset
                                if offset >= 0:
                                    print(f"    Partition {partition_id}: Committed offset {offset}")
                                else:
                                    print(f"    Partition {partition_id}: No committed offset")
                            else:
                                print(f"    Partition {partition_id}: No committed offset")
                            
                            # Get high water mark (latest available)
                            try:
                                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                                print(f"    Partition {partition_id}: High water mark {high}")
                            except Exception:
                                pass
                                
                except Exception as e:
                    print(f"    Error checking topic {topic}: {e}")
            
            consumer.close()
            
        except Exception as e:
            print(f"  Error: {e}")


def reset_consumer_group(group_id: str, reset_type: str = "latest"):
    """Reset consumer group offsets."""
    config = get_config()
    
    print(f"🔄 Resetting consumer group: {group_id} to {reset_type}")
    
    # Note: This requires kafka-consumer-groups command line tool
    # or implementing with confluent-kafka admin APIs
    import subprocess
    
    try:
        # Reset using kafka-consumer-groups tool (if available)
        cmd = [
            "kafka-consumer-groups",
            "--bootstrap-server", config.kafka.bootstrap_servers,
            "--group", group_id,
            "--reset-offsets",
            f"--to-{reset_type}",
            "--all-topics",
            "--execute"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Consumer group reset successfully")
            print(result.stdout)
        else:
            print("❌ Failed to reset consumer group")
            print(result.stderr)
            
    except FileNotFoundError:
        print("⚠️  kafka-consumer-groups tool not found")
        print("Install Kafka tools or reset manually via Kafka UI")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Check Kafka consumer group offsets")
    parser.add_argument("--reset", type=str, 
                       help="Reset consumer group (main|lookup|simple)")
    parser.add_argument("--reset-to", type=str, default="latest",
                       choices=["earliest", "latest"],
                       help="Reset offset position")
    
    args = parser.parse_args()
    
    if args.reset:
        config = get_config()
        group_mapping = {
            "main": config.kafka.consumer_group_main,
            "lookup": config.kafka.consumer_group_lookup,
            "simple": "bytewax-simple-pipeline"
        }
        
        if args.reset in group_mapping:
            reset_consumer_group(group_mapping[args.reset], args.reset_to)
        else:
            print(f"Unknown group: {args.reset}")
    else:
        check_consumer_groups()


if __name__ == "__main__":
    main()