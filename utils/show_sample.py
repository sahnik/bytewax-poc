#!/usr/bin/env python3
"""
Show a sample record with all fields.
"""

import sys
import os
import json

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_publisher import SampleDataGenerator


def main():
    """Show a sample record."""
    generator = SampleDataGenerator()
    sample = generator.generate_user_event()
    
    print("🔍 Sample Record with ~150 Fields")
    print("=" * 50)
    print(json.dumps(sample, indent=2, default=str))
    
    print(f"\n📊 Summary:")
    print(f"Total fields: {len(sample)}")
    print(f"Event type: {sample['eventType']}")
    print(f"User: {sample['userId']} ({'VIP' if sample['isVip'] else 'Regular'})")
    print(f"JSON size: {len(json.dumps(sample)):,} bytes")


if __name__ == "__main__":
    main()