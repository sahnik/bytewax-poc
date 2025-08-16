#!/usr/bin/env python3
"""
Populate lookup topic with sample user data.
This script creates lookup data for user enrichment.
"""

import json
import argparse
import csv
import sys
import os
from typing import Dict, Any, List
from confluent_kafka import Producer

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_config

try:
    import structlog
    logger = structlog.get_logger()
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


class LookupDataGenerator:
    """Generates sample lookup data for user enrichment."""
    
    def __init__(self):
        self.countries = ["US", "CA", "UK", "DE", "FR", "JP", "AU", "BR"]
        self.cities = {
            "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
            "CA": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
            "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
            "DE": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"],
            "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
            "JP": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
            "AU": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
            "BR": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"]
        }
        self.segments = ["premium", "standard", "basic", "trial"]
        self.industries = ["technology", "finance", "healthcare", "retail", "manufacturing", "education"]
    
    def generate_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Generate a user profile for lookup."""
        import random
        
        # Extract user number for consistent data generation
        user_num = int(user_id.split('_')[1]) if '_' in user_id else hash(user_id) % 10000
        random.seed(user_num)  # Consistent random data for same user
        
        country = random.choice(self.countries)
        city = random.choice(self.cities[country])
        
        # VIP users (ending in 001-005) get premium segment
        if user_id.endswith(('001', '002', '003', '004', '005')):
            segment = "premium"
            join_date = f"2020-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        else:
            segment = random.choice(self.segments)
            join_date = f"{random.randint(2021, 2024)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        profile = {
            "key": user_id,  # Include key for clarity
            "name": f"User {user_id.split('_')[1] if '_' in user_id else user_id}",
            "email": f"{user_id}@example.com",
            "segment": segment,
            "country": country,
            "city": city,
            "joinDate": join_date,
            "industry": random.choice(self.industries),
            "companySize": random.choice(["1-10", "11-50", "51-200", "201-1000", "1000+"]),
            "isVip": user_id.endswith(('001', '002', '003', '004', '005')),
            "lifetimeValue": round(random.uniform(100, 10000), 2),
            "lastLoginDate": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "preferences": {
                "newsletter": random.choice([True, False]),
                "marketing": random.choice([True, False]),
                "language": random.choice(["en", "es", "fr", "de", "ja"])
            }
        }
        
        return profile


class LookupDataPublisher:
    """Publishes lookup data to Kafka topic."""
    
    def __init__(self, brokers: List[str], topic: str):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': ','.join(brokers),
            'client.id': 'lookup-data-publisher'
        })
        self.generator = LookupDataGenerator()
    
    def delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error("Lookup message delivery failed", error=str(err))
        else:
            logger.debug("Lookup message delivered",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset())
    
    def publish_user_profile(self, user_id: str):
        """Publish a single user profile to lookup topic."""
        try:
            profile = self.generator.generate_user_profile(user_id)
            message_value = json.dumps(profile)
            
            # Use user_id as key for compaction
            self.producer.produce(
                topic=self.topic,
                key=user_id,
                value=message_value,
                callback=self.delivery_report
            )
            
            logger.debug("Published user profile", user_id=user_id)
            
        except Exception as e:
            logger.error("Failed to publish user profile", 
                        error=str(e), user_id=user_id)
    
    def publish_from_csv(self, csv_file: str):
        """Publish lookup data from CSV file."""
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                count = 0
                
                for row in reader:
                    # Expect CSV with at least 'user_id' column
                    user_id = row.get('user_id') or row.get('id')
                    if not user_id:
                        logger.warning("No user_id found in CSV row", row=row)
                        continue
                    
                    # Use CSV data as lookup data
                    # Remove user_id from the value since it's used as key
                    lookup_value = {k: v for k, v in row.items() if k not in ['user_id', 'id']}
                    
                    message_value = json.dumps(lookup_value)
                    self.producer.produce(
                        topic=self.topic,
                        key=user_id,
                        value=message_value,
                        callback=self.delivery_report
                    )
                    
                    count += 1
                    
                    # Flush every 100 records
                    if count % 100 == 0:
                        self.producer.flush()
                
                self.producer.flush()
                logger.info("Published lookup data from CSV", 
                           file=csv_file, count=count)
                
        except Exception as e:
            logger.error("Failed to publish from CSV", 
                        error=str(e), file=csv_file)
    
    def populate_sample_users(self, count: int = 1000):
        """Populate lookup topic with sample user data."""
        logger.info("Populating lookup topic with sample users", count=count)
        
        for i in range(1, count + 1):
            user_id = f"user_{i:05d}"
            self.publish_user_profile(user_id)
            
            # Flush every 100 users
            if i % 100 == 0:
                self.producer.flush()
                logger.info("Progress", published=i, total=count)
        
        # Final flush
        self.producer.flush()
        logger.info("Completed populating lookup topic", total_users=count)
    
    def update_user_profile(self, user_id: str, updates: Dict[str, Any]):
        """Update specific fields in a user profile."""
        try:
            # Get existing profile or create new one
            existing_profile = self.generator.generate_user_profile(user_id)
            
            # Apply updates
            existing_profile.update(updates)
            
            message_value = json.dumps(existing_profile)
            self.producer.produce(
                topic=self.topic,
                key=user_id,
                value=message_value,
                callback=self.delivery_report
            )
            
            self.producer.flush()
            logger.info("Updated user profile", user_id=user_id, updates=updates)
            
        except Exception as e:
            logger.error("Failed to update user profile", 
                        error=str(e), user_id=user_id)


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Populate lookup topic with user data")
    parser.add_argument("--count", type=int, default=1000,
                       help="Number of sample users to create (default: 1000)")
    parser.add_argument("--csv", type=str,
                       help="CSV file to load lookup data from")
    parser.add_argument("--topic", type=str,
                       help="Override lookup topic name")
    parser.add_argument("--update-user", type=str,
                       help="Update specific user profile")
    parser.add_argument("--segment", type=str,
                       help="Set segment for user update")
    parser.add_argument("--vip", action="store_true",
                       help="Mark user as VIP")
    
    args = parser.parse_args()
    
    # Load configuration
    config = get_config()
    topic = args.topic or config.kafka.lookup_topic
    
    # Create publisher
    publisher = LookupDataPublisher(config.kafka.brokers, topic)
    
    logger.info("Starting lookup data population", 
               topic=topic,
               brokers=config.kafka.brokers)
    
    try:
        if args.csv:
            # Load from CSV
            publisher.publish_from_csv(args.csv)
        elif args.update_user:
            # Update specific user
            updates = {}
            if args.segment:
                updates["segment"] = args.segment
            if args.vip:
                updates["isVip"] = True
            
            publisher.update_user_profile(args.update_user, updates)
        else:
            # Generate sample users
            publisher.populate_sample_users(args.count)
    
    except Exception as e:
        logger.error("Lookup population failed", error=str(e))
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())