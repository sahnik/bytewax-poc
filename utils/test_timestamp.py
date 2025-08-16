#!/usr/bin/env python3
"""
Test timestamp parsing functionality.
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.transformations.standardization import standardize_timestamp
from utils.data_publisher import SampleDataGenerator


def test_timestamp_parsing():
    """Test timestamp parsing with various formats."""
    
    # Test current data format
    generator = SampleDataGenerator()
    sample = generator.generate_user_event()
    
    print("🔍 Testing Timestamp Parsing")
    print("=" * 40)
    
    original_timestamp = sample["timestamp"]
    print(f"Original timestamp: {original_timestamp}")
    
    result = standardize_timestamp(original_timestamp)
    print(f"Standardized: {result}")
    print(f"Success: {'✅' if result else '❌'}")
    
    # Test various formats
    test_cases = [
        "2025-08-16T20:52:02.994637+00:00",  # Current format
        "2025-08-16T20:52:02+00:00",         # Without microseconds
        "2025-08-16T20:52:02Z",              # Z timezone
        "2025-08-16T20:52:02",               # No timezone
        "2025-08-16 20:52:02",               # Space separated
        "2025-08-16",                        # Date only
        "1755377250",                        # Unix timestamp (seconds)
        "1755377250000",                     # Unix timestamp (milliseconds)
        "invalid_timestamp",                 # Invalid
    ]
    
    print(f"\n📊 Testing Various Timestamp Formats")
    print("-" * 40)
    
    for test_timestamp in test_cases:
        result = standardize_timestamp(test_timestamp)
        status = "✅" if result else "❌"
        print(f"{status} {test_timestamp[:30]:<30} → {result}")


if __name__ == "__main__":
    test_timestamp_parsing()