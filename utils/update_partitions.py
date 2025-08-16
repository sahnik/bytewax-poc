#!/usr/bin/env python3
"""
Update Kafka topic partition counts.
Note: Kafka only allows increasing partition counts, not decreasing.
"""

import argparse
import subprocess
import sys
import time
from typing import List


def run_kafka_command(command: List[str]) -> bool:
    """Run a kafka command via docker exec."""
    docker_cmd = [
        "docker", "exec", "kafka"
    ] + command
    
    try:
        result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print(f"✅ Command succeeded: {' '.join(command)}")
            if result.stdout:
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Command failed: {' '.join(command)}")
            print(f"   Error: {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ Command timed out: {' '.join(command)}")
        return False
    except Exception as e:
        print(f"💥 Command error: {' '.join(command)} - {e}")
        return False


def wait_for_kafka(max_retries: int = 30) -> bool:
    """Wait for Kafka to be ready."""
    print("🔄 Waiting for Kafka to be ready...")
    
    for attempt in range(max_retries):
        if run_kafka_command([
            "kafka-topics", 
            "--bootstrap-server", "kafka:29092", 
            "--list"
        ]):
            print("✅ Kafka is ready!")
            return True
        
        if attempt < max_retries - 1:
            print(f"   Attempt {attempt + 1}/{max_retries} failed, retrying in 2 seconds...")
            time.sleep(2)
    
    print("❌ Kafka did not become ready in time")
    return False


def get_topic_info(topic: str) -> dict:
    """Get current partition count for a topic."""
    cmd = [
        "kafka-topics",
        "--bootstrap-server", "kafka:29092",
        "--describe",
        "--topic", topic
    ]
    
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka"] + cmd,
            capture_output=True, text=True, timeout=10
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            partition_count = len([line for line in lines if 'Partition:' in line])
            return {"exists": True, "partitions": partition_count}
        else:
            return {"exists": False, "partitions": 0}
            
    except Exception as e:
        print(f"Error getting topic info for {topic}: {e}")
        return {"exists": False, "partitions": 0}


def update_topic_partitions(topic: str, new_partition_count: int) -> bool:
    """Update partition count for a topic."""
    # Get current info
    info = get_topic_info(topic)
    
    if not info["exists"]:
        print(f"❌ Topic {topic} does not exist")
        return False
    
    current_partitions = info["partitions"]
    print(f"📊 Topic {topic}: current partitions = {current_partitions}")
    
    if current_partitions >= new_partition_count:
        print(f"✅ Topic {topic} already has {current_partitions} partitions (>= {new_partition_count})")
        return True
    
    # Increase partition count
    cmd = [
        "kafka-topics",
        "--bootstrap-server", "kafka:29092",
        "--alter",
        "--topic", topic,
        "--partitions", str(new_partition_count)
    ]
    
    print(f"🔧 Updating {topic} from {current_partitions} to {new_partition_count} partitions...")
    return run_kafka_command(cmd)


def main():
    parser = argparse.ArgumentParser(description="Update Kafka topic partition counts")
    parser.add_argument("--input-partitions", type=int, default=8, 
                       help="Number of partitions for input-topic (default: 8)")
    parser.add_argument("--output-partitions", type=int, default=8,
                       help="Number of partitions for output-topic (default: 8)")
    parser.add_argument("--check-only", action="store_true",
                       help="Only check current partition counts, don't update")
    
    args = parser.parse_args()
    
    if not wait_for_kafka():
        return 1
    
    topics_to_update = [
        ("input-topic", args.input_partitions),
        ("output-topic", args.output_partitions),
    ]
    
    print(f"\n📋 Topic Partition Update Plan:")
    print(f"   input-topic: {args.input_partitions} partitions")
    print(f"   output-topic: {args.output_partitions} partitions")
    print(f"   check-only: {args.check_only}\n")
    
    all_success = True
    
    for topic, target_partitions in topics_to_update:
        if args.check_only:
            info = get_topic_info(topic)
            if info["exists"]:
                print(f"📊 {topic}: {info['partitions']} partitions")
            else:
                print(f"❌ {topic}: does not exist")
        else:
            success = update_topic_partitions(topic, target_partitions)
            if not success:
                all_success = False
    
    if not args.check_only:
        print(f"\n{'✅ All updates completed successfully!' if all_success else '❌ Some updates failed!'}")
        
        # Show final status
        print("\n📊 Final partition counts:")
        for topic, _ in topics_to_update:
            info = get_topic_info(topic)
            if info["exists"]:
                print(f"   {topic}: {info['partitions']} partitions")
    
    return 0 if all_success else 1


if __name__ == "__main__":
    exit(main())