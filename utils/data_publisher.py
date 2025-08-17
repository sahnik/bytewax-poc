#!/usr/bin/env python3
"""
Sample data publisher for testing the Bytewax pipeline.
Publishes configurable sample data to Kafka at a specified rate.
"""

import json
import time
import random
import argparse
import sys
import os
from datetime import datetime, timezone, timedelta
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


class SampleDataGenerator:
    """Generates realistic sample data with ~150 fields for testing."""
    
    def __init__(self):
        self.user_ids = [f"user_{i:05d}" for i in range(1, 1001)]  # 1000 users
        self.event_types = ["login", "purchase", "view", "click", "logout", "search", "add_to_cart", "remove_from_cart", "checkout", "payment"]
        self.product_categories = ["electronics", "clothing", "books", "home", "sports", "automotive", "health", "beauty", "toys", "food"]
        self.countries = ["US", "CA", "UK", "DE", "FR", "JP", "AU", "BR", "IN", "CN", "IT", "ES", "NL", "SE", "NO"]
        
        # Extended data for rich field generation
        self.browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
        self.operating_systems = ["Windows", "macOS", "Linux", "iOS", "Android"]
        self.devices = ["Desktop", "Mobile", "Tablet", "Smart TV", "Gaming Console"]
        self.languages = ["en", "es", "fr", "de", "ja", "zh", "pt", "it", "ru", "ar"]
        self.timezones = ["UTC", "EST", "PST", "GMT", "CET", "JST", "CST", "IST", "AEST", "MST"]
        self.currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "BRL"]
        self.payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer", "crypto"]
        self.subscription_tiers = ["free", "basic", "premium", "enterprise", "trial"]
        self.marketing_channels = ["organic", "paid_search", "social", "email", "direct", "affiliate", "referral"]
        self.content_types = ["article", "video", "podcast", "webinar", "ebook", "infographic", "case_study"]
        self.feature_flags = ["new_ui", "dark_mode", "beta_features", "analytics_v2", "mobile_app", "ai_recommendations"]
        
    def generate_user_event(self) -> Dict[str, Any]:
        """Generate a comprehensive user event record with ~150 fields."""
        user_id = random.choice(self.user_ids)
        
        # Create some correlation - certain users are more active
        if user_id.endswith(('001', '002', '003', '004', '005')):
            # VIP users - more activity
            event_type = random.choices(
                self.event_types,
                weights=[5, 25, 15, 20, 5, 10, 15, 5, 0, 0]  # More purchases/cart actions
            )[0]
            is_vip = True
        else:
            # Regular users
            event_type = random.choices(
                self.event_types,
                weights=[15, 5, 25, 20, 10, 15, 5, 3, 1, 1]  # More views/searches
            )[0]
            is_vip = False
        
        # Base event data (20 fields)
        record = {
            # Core identifiers
            "id": f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "userId": user_id,
            "sessionId": f"sess_{random.randint(100000, 999999)}",
            "eventType": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "eventVersion": "2.1",
            "schemaVersion": "1.0",
            "correlationId": f"corr_{random.randint(10000, 99999)}",
            "traceId": f"trace_{random.randint(100000, 999999)}",
            "spanId": f"span_{random.randint(1000, 9999)}",
            
            # User context
            "isAuthenticated": random.choice([True, False]),
            "isVip": is_vip,
            "userTier": random.choice(self.subscription_tiers),
            "accountAge": random.randint(1, 2000),  # days
            "totalPurchases": random.randint(0, 500),
            "lifetimeValue": round(random.uniform(0, 10000), 2),
            "loyaltyPoints": random.randint(0, 50000),
            "riskScore": round(random.uniform(0.0, 1.0), 3),
            "creditScore": random.randint(300, 850),
            "lastLoginDate": (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))).isoformat(),
        }
        
        # Technical context (25 fields)
        browser = random.choice(self.browsers)
        os = random.choice(self.operating_systems)
        device = random.choice(self.devices)
        
        record.update({
            # Device and browser
            "browser": browser,
            "browserVersion": f"{random.randint(90, 120)}.{random.randint(0, 10)}.{random.randint(0, 100)}",
            "operatingSystem": os,
            "osVersion": f"{random.randint(10, 15)}.{random.randint(0, 5)}.{random.randint(0, 10)}",
            "deviceType": device,
            "deviceModel": f"Model_{random.randint(1, 100)}",
            "deviceBrand": random.choice(["Apple", "Samsung", "Google", "Microsoft", "Dell", "HP"]),
            "screenResolution": random.choice(["1920x1080", "1366x768", "1440x900", "2560x1440", "3840x2160"]),
            "colorDepth": random.choice([24, 32]),
            "pixelRatio": random.choice([1.0, 1.5, 2.0, 3.0]),
            
            # Network and location
            "ipAddress": f"{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "userAgent": f"Mozilla/5.0 ({os}; {device}) {browser}/{random.randint(90, 120)}",
            "country": random.choice(self.countries),
            "region": f"Region_{random.randint(1, 20)}",
            "city": f"City_{random.randint(1, 100)}",
            "postalCode": f"{random.randint(10000, 99999)}",
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6),
            "timezone": random.choice(self.timezones),
            "language": random.choice(self.languages),
            "locale": f"{random.choice(self.languages)}-{random.choice(self.countries)}",
            "currency": random.choice(self.currencies),
            "connectionType": random.choice(["wifi", "cellular", "ethernet", "unknown"]),
            "isp": random.choice(["Verizon", "AT&T", "Comcast", "Spectrum", "T-Mobile"]),
            "networkSpeed": random.choice(["slow-2g", "2g", "3g", "4g", "5g", "broadband"]),
        })
        
        # Page and content context (25 fields)
        record.update({
            "pageUrl": f"/page/{random.choice(['home', 'products', 'checkout', 'profile', 'search', 'category'])}",
            "pageTitle": f"Page Title {random.randint(1, 100)}",
            "referrer": random.choice(["", "google.com", "facebook.com", "twitter.com", "direct"]),
            "referrerType": random.choice(["search", "social", "email", "direct", "paid"]),
            "marketingChannel": random.choice(self.marketing_channels),
            "campaignId": f"campaign_{random.randint(1000, 9999)}",
            "campaignName": f"Campaign {random.randint(1, 50)}",
            "adGroupId": f"adgroup_{random.randint(100, 999)}",
            "keywordId": f"keyword_{random.randint(1, 1000)}",
            "creativeId": f"creative_{random.randint(1, 500)}",
            "contentId": f"content_{random.randint(1, 10000)}",
            "contentType": random.choice(self.content_types),
            "contentCategory": random.choice(self.product_categories),
            "contentLength": random.randint(100, 5000),
            "readingTime": random.randint(30, 600),  # seconds
            "scrollDepth": round(random.uniform(0, 100), 2),  # percentage
            "timeOnPage": random.randint(5, 1200),  # seconds
            "bounceRate": round(random.uniform(0, 1), 3),
            "exitRate": round(random.uniform(0, 1), 3),
            "pageLoadTime": random.randint(500, 5000),  # milliseconds
            "domContentLoadedTime": random.randint(200, 2000),
            "firstContentfulPaint": random.randint(300, 3000),
            "largestContentfulPaint": random.randint(1000, 4000),
            "cumulativeLayoutShift": round(random.uniform(0, 0.5), 3),
            "firstInputDelay": random.randint(10, 300),
        })
        
        # Business context (25 fields)
        record.update({
            "productId": f"prod_{random.randint(1, 10000)}",
            "productName": f"Product {random.randint(1, 1000)}",
            "productCategory": random.choice(self.product_categories),
            "productSubcategory": f"Subcategory_{random.randint(1, 50)}",
            "productBrand": f"Brand_{random.randint(1, 100)}",
            "productPrice": round(random.uniform(10.0, 1000.0), 2),
            "productCost": round(random.uniform(5.0, 500.0), 2),
            "productMargin": round(random.uniform(0.1, 0.8), 3),
            "productRating": round(random.uniform(1.0, 5.0), 1),
            "productReviews": random.randint(0, 1000),
            "inventoryLevel": random.randint(0, 1000),
            "supplierId": f"supplier_{random.randint(1, 100)}",
            "warehouseId": f"warehouse_{random.randint(1, 20)}",
            "shippingCost": round(random.uniform(0, 50.0), 2),
            "taxAmount": round(random.uniform(0, 100.0), 2),
            "discountAmount": round(random.uniform(0, 200.0), 2),
            "couponCode": f"COUPON{random.randint(1000, 9999)}" if random.random() < 0.3 else None,
            "paymentMethod": random.choice(self.payment_methods),
            "transactionId": f"txn_{random.randint(100000, 999999)}",
            "orderId": f"order_{random.randint(10000, 99999)}",
            "cartValue": round(random.uniform(0, 2000.0), 2),
            "cartItems": random.randint(1, 20),
            "shippingMethod": random.choice(["standard", "express", "overnight", "pickup"]),
            "expectedDelivery": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 14))).isoformat(),
            "fulfillmentCenter": f"FC_{random.randint(1, 50)}",
        })
        
        # Behavioral and engagement (25 fields)
        record.update({
            "engagementScore": round(random.uniform(0, 100), 2),
            "interactionCount": random.randint(0, 100),
            "clickCount": random.randint(0, 50),
            "impressionCount": random.randint(0, 200),
            "conversionRate": round(random.uniform(0, 0.5), 4),
            "clickThroughRate": round(random.uniform(0, 0.3), 4),
            "abandonmentRate": round(random.uniform(0, 0.8), 4),
            "returnVisitor": random.choice([True, False]),
            "sessionCount": random.randint(1, 500),
            "pageViews": random.randint(1, 100),
            "uniquePageViews": random.randint(1, 50),
            "eventSequence": random.randint(1, 1000),
            "funnelStep": random.randint(1, 10),
            "experimentId": f"exp_{random.randint(1, 100)}",
            "experimentVariant": random.choice(["control", "variant_a", "variant_b", "variant_c"]),
            "personalizedContent": random.choice([True, False]),
            "recommendationId": f"rec_{random.randint(1, 10000)}",
            "similarProducts": [f"prod_{random.randint(1, 10000)}" for _ in range(random.randint(0, 5))],
            "searchQuery": f"search term {random.randint(1, 1000)}" if event_type == "search" else None,
            "searchResults": random.randint(0, 1000) if event_type == "search" else None,
            "filterOptions": random.choice([None, "price", "category", "brand", "rating"]),
            "sortOrder": random.choice(["relevance", "price_asc", "price_desc", "rating", "newest"]),
            "viewedProducts": [f"prod_{random.randint(1, 10000)}" for _ in range(random.randint(1, 10))],
            "addedToWishlist": random.choice([True, False]),
            "sharedContent": random.choice([True, False]),
        })
        
        # Feature flags and configuration (20 fields)
        record.update({
            "featureFlags": {flag: random.choice([True, False]) for flag in random.sample(self.feature_flags, 3)},
            "apiVersion": f"v{random.randint(1, 5)}.{random.randint(0, 10)}",
            "clientVersion": f"mobile-{random.randint(1, 20)}.{random.randint(0, 50)}",
            "buildNumber": random.randint(1000, 9999),
            "deploymentId": f"deploy_{random.randint(1, 1000)}",
            "serverRegion": random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]),
            "loadBalancerId": f"lb_{random.randint(1, 10)}",
            "serverInstance": f"i-{random.randint(100000, 999999)}",
            "processingTime": random.randint(1, 1000),  # milliseconds
            "queueDepth": random.randint(0, 1000),
            "errorCode": None if random.random() > 0.05 else f"ERR_{random.randint(1000, 9999)}",
            "errorMessage": None if random.random() > 0.05 else "Sample error message",
            "warningCount": random.randint(0, 5),
            "debugEnabled": random.choice([True, False]),
            "samplingRate": round(random.uniform(0.01, 1.0), 3),
            "dataRetentionDays": random.choice([30, 90, 365, 2555]),  # 7 years
            "complianceFlags": {
                "gdpr": random.choice([True, False]),
                "ccpa": random.choice([True, False]),
                "coppa": random.choice([True, False])
            },
            "consentGiven": random.choice([True, False]),
            "anonymizedData": random.choice([True, False]),
            "dataProcessingPurpose": random.choice(["analytics", "personalization", "marketing", "fraud_detection"]),
        })
        
        # Event-specific data (varies by event type)
        if event_type == "purchase":
            transaction_amount = round(random.uniform(10.0, 500.0), 2)
            tax = round(transaction_amount * 0.08, 2)
            shipping = round(random.uniform(0, 25.0), 2)
            
            record.update({
                "transactionAmount": transaction_amount,
                "tax": tax,
                "shipping": shipping,
                "total": round(transaction_amount + tax + shipping, 2),
                "paymentStatus": random.choice(["pending", "completed", "failed", "refunded"]),
                "fraudScore": round(random.uniform(0, 1), 3),
            })
        elif event_type == "view":
            record.update({
                "viewDuration": random.randint(5, 300),
                "viewDepth": round(random.uniform(0, 100), 2),
                "mediaPlayed": random.choice([True, False]),
                "videoPosition": random.randint(0, 3600) if record.get("mediaPlayed") else None,
            })
        elif event_type in ["add_to_cart", "remove_from_cart"]:
            record.update({
                "quantity": random.randint(1, 10),
                "cartSize": random.randint(1, 50),
                "cartUpdatedAt": datetime.now(timezone.utc).isoformat(),
            })
        
        return record
    
    def generate_malformed_record(self) -> str:
        """Generate malformed JSON for testing error handling."""
        malformed_types = [
            '{"id": "malformed1", "timestamp":}',  # Invalid JSON
            '{"userId": null, "eventType": ""}',    # Missing required fields
            '{"id": "malformed2", "userAge": "not_a_number"}',  # Wrong type
            '{"id": "malformed3"'  # Incomplete JSON
        ]
        return random.choice(malformed_types)


class KafkaDataPublisher:
    """Publishes sample data to Kafka topic."""
    
    def __init__(self, config):
        self.topic = config.kafka.source_topic
        
        # Build producer configuration with security settings
        producer_config = {
            'bootstrap.servers': ','.join(config.kafka.brokers),
            'client.id': 'sample-data-publisher'
        }
        
        # Add security configuration if present
        if hasattr(config.kafka, 'security_protocol') and config.kafka.security_protocol != 'PLAINTEXT':
            producer_config.update({
                'security.protocol': config.kafka.security_protocol,
                'sasl.mechanism': config.kafka.sasl_mechanism,
                'sasl.username': config.kafka.sasl_username,
                'sasl.password': config.kafka.sasl_password
            })
        
        self.producer = Producer(producer_config)
        self.generator = SampleDataGenerator()
        
    def delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error("Message delivery failed", error=str(err))
        else:
            logger.debug("Message delivered", 
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset())
    
    def publish_record(self, record: Dict[str, Any], key: str = None):
        """Publish a single record to Kafka."""
        try:
            message_value = json.dumps(record)
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=message_value,
                callback=self.delivery_report
            )
        except Exception as e:
            logger.error("Failed to publish record", error=str(e), record=record)
    
    def publish_malformed_record(self):
        """Publish a malformed record for testing."""
        try:
            malformed_json = self.generator.generate_malformed_record()
            self.producer.produce(
                topic=self.topic,
                value=malformed_json,
                callback=self.delivery_report
            )
            logger.info("Published malformed record for testing")
        except Exception as e:
            logger.error("Failed to publish malformed record", error=str(e))
    
    def publish_batch(self, count: int, malformed_rate: float = 0.05):
        """Publish a batch of records."""
        for i in range(count):
            # Occasionally publish malformed records for testing
            if random.random() < malformed_rate:
                self.publish_malformed_record()
            else:
                record = self.generator.generate_user_event()
                self.publish_record(record, key=record["userId"])
            
            # Flush every 100 messages
            if i % 100 == 0:
                self.producer.flush()
        
        # Final flush
        self.producer.flush()
        logger.info("Published batch", count=count)
    
    def publish_continuous(self, rate_per_second: int, duration_seconds: int = None):
        """Publish records continuously at specified rate."""
        interval = 1.0 / rate_per_second
        start_time = time.time()
        records_published = 0
        
        logger.info("Starting continuous publishing", 
                   rate_per_second=rate_per_second,
                   duration_seconds=duration_seconds)
        
        try:
            while True:
                # Check duration limit
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                # Publish record
                record = self.generator.generate_user_event()
                self.publish_record(record, key=record["userId"])
                records_published += 1
                
                # Rate limiting
                time.sleep(interval)
                
                # Periodic flush and stats
                if records_published % 100 == 0:
                    self.producer.flush()
                    elapsed = time.time() - start_time
                    actual_rate = records_published / elapsed if elapsed > 0 else 0
                    logger.info("Publishing stats", 
                               records_published=records_published,
                               elapsed_seconds=int(elapsed),
                               actual_rate=round(actual_rate, 2))
        
        except KeyboardInterrupt:
            logger.info("Publishing interrupted by user")
        
        finally:
            self.producer.flush()
            elapsed = time.time() - start_time
            logger.info("Publishing completed", 
                       total_records=records_published,
                       total_time=round(elapsed, 2),
                       average_rate=round(records_published / elapsed if elapsed > 0 else 0, 2))


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Publish sample data to Kafka")
    parser.add_argument("--rate", type=int, default=10, 
                       help="Records per second (default: 10)")
    parser.add_argument("--duration", type=int, 
                       help="Duration in seconds (default: continuous)")
    parser.add_argument("--batch", type=int, 
                       help="Publish a batch of N records and exit")
    parser.add_argument("--topic", type=str, 
                       help="Override topic name")
    parser.add_argument("--malformed-rate", type=float, default=0.05,
                       help="Rate of malformed records (default: 0.05)")
    
    args = parser.parse_args()
    
    # Load configuration
    config = get_config()
    
    # Override topic if specified
    if args.topic:
        config.kafka.source_topic = args.topic
    
    # Create publisher
    publisher = KafkaDataPublisher(config)
    
    logger.info("Starting data publisher", 
               topic=config.kafka.source_topic,
               brokers=config.kafka.brokers)
    
    try:
        if args.batch:
            # Batch mode
            publisher.publish_batch(args.batch, args.malformed_rate)
        else:
            # Continuous mode
            publisher.publish_continuous(args.rate, args.duration)
    
    except Exception as e:
        logger.error("Publisher failed", error=str(e))
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())