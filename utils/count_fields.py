#!/usr/bin/env python3
"""
Utility to count fields in generated sample data.
"""

import sys
import os
import json

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_publisher import SampleDataGenerator


def count_fields_recursively(obj, prefix=""):
    """Count fields recursively in nested objects."""
    count = 0
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{prefix}.{key}" if prefix else key
            count += 1  # Count the key itself
            
            if isinstance(value, (dict, list)):
                count += count_fields_recursively(value, current_path)
    
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                count += count_fields_recursively(item, f"{prefix}[{i}]")
    
    return count


def analyze_sample_data():
    """Generate and analyze sample data."""
    generator = SampleDataGenerator()
    
    print("🔍 Analyzing Sample Data Structure")
    print("=" * 50)
    
    # Generate a few samples to see variation
    for i in range(3):
        print(f"\n📊 Sample {i+1}:")
        print("-" * 20)
        
        sample = generator.generate_user_event()
        
        # Count top-level fields
        top_level_count = len(sample.keys())
        
        # Count all fields recursively
        total_count = count_fields_recursively(sample)
        
        print(f"Top-level fields: {top_level_count}")
        print(f"Total fields (including nested): {total_count}")
        print(f"Event type: {sample['eventType']}")
        print(f"User ID: {sample['userId']}")
        print(f"Is VIP: {sample['isVip']}")
        
        # Show field categories
        categories = {
            "Core identifiers": ["id", "userId", "sessionId", "eventType", "timestamp"],
            "User context": ["isAuthenticated", "isVip", "userTier", "accountAge"],
            "Technical": ["browser", "operatingSystem", "deviceType", "ipAddress"],
            "Location": ["country", "region", "city", "latitude", "longitude"],
            "Business": ["productId", "productCategory", "productPrice", "paymentMethod"],
            "Engagement": ["engagementScore", "clickCount", "pageViews", "sessionCount"],
            "Configuration": ["featureFlags", "apiVersion", "serverRegion"]
        }
        
        print(f"\nField categories present:")
        for category, fields in categories.items():
            present = sum(1 for field in fields if field in sample)
            print(f"  {category}: {present}/{len(fields)} fields")
    
    # Show sample JSON size
    sample_json = json.dumps(sample, indent=2)
    json_size = len(sample_json.encode('utf-8'))
    
    print(f"\n📏 Sample Data Size:")
    print(f"JSON size: {json_size:,} bytes ({json_size/1024:.1f} KB)")
    print(f"Compressed estimate: ~{json_size//3:,} bytes")
    
    # Show some sample fields
    print(f"\n🔍 Sample Field Values:")
    interesting_fields = [
        "eventType", "country", "browser", "deviceType", "productCategory",
        "paymentMethod", "marketingChannel", "experimentVariant"
    ]
    
    for field in interesting_fields:
        if field in sample:
            print(f"  {field}: {sample[field]}")


if __name__ == "__main__":
    analyze_sample_data()