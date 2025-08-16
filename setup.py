#!/usr/bin/env python3
"""
Setup script for Bytewax Data Pipeline POC.
Initializes the environment and verifies everything is working.
"""

import os
import sys
import subprocess
import time

def run_command(cmd, description, cwd=None):
    """Run a command and handle errors."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False
        print(f"✅ {description} completed")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_requirements():
    """Check if required tools are available."""
    print("🔍 Checking requirements...")
    
    # Check Python version
    if sys.version_info < (3, 12):
        print("❌ Python 3.12+ required")
        return False
    print("✅ Python version OK")
    
    # Check Docker
    if not run_command("docker --version", "Checking Docker"):
        return False
    
    # Check Docker Compose
    if not run_command("docker-compose --version", "Checking Docker Compose"):
        return False
    
    return True

def setup_python_environment():
    """Set up Python virtual environment and install dependencies."""
    print("\n📦 Setting up Python environment...")
    
    # Create .env from template if it doesn't exist
    if not os.path.exists(".env"):
        if os.path.exists(".env.template"):
            run_command("cp .env.template .env", "Creating .env file")
        else:
            print("❌ .env.template not found")
            return False
    
    # Install Python dependencies
    if not run_command("pip install -r requirements.txt", "Installing Python dependencies"):
        return False
    
    return True

def start_infrastructure():
    """Start Kafka infrastructure."""
    print("\n🚀 Starting Kafka infrastructure...")
    
    if not run_command("docker-compose up -d", "Starting Docker containers"):
        return False
    
    print("⏳ Waiting for Kafka to be ready...")
    time.sleep(15)  # Give Kafka time to start
    
    return True

def populate_test_data():
    """Populate lookup topic with test data."""
    print("\n📊 Populating test data...")
    
    # Populate lookup data
    if not run_command("PYTHONPATH=. python utils/populate_lookup.py --count 100", 
                      "Populating lookup topic"):
        return False
    
    return True

def run_tests():
    """Run the test suite."""
    print("\n🧪 Running tests...")
    
    if not run_command("python -m pytest tests/ -v", "Running test suite"):
        print("⚠️  Some tests failed, but setup can continue")
    
    return True

def main():
    """Main setup function."""
    print("🎯 Bytewax Data Pipeline POC Setup")
    print("=" * 40)
    
    # Check requirements
    if not check_requirements():
        print("\n❌ Requirements check failed. Please install required tools.")
        return 1
    
    # Setup Python environment
    if not setup_python_environment():
        print("\n❌ Python environment setup failed.")
        return 1
    
    # Start infrastructure
    if not start_infrastructure():
        print("\n❌ Infrastructure startup failed.")
        return 1
    
    # Populate test data
    if not populate_test_data():
        print("\n❌ Test data population failed.")
        return 1
    
    # Run tests
    run_tests()
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Run the pipeline:     make pipeline")
    print("2. Publish test data:    make publish-data")
    print("3. Monitor with Kafka UI: make kafka-ui")
    print("4. View metrics:         make metrics")
    print("\nOr run a quick demo:     make demo")
    
    return 0

if __name__ == "__main__":
    exit(main())