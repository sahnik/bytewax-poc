import pytest
from datetime import datetime, timezone
from pipeline.transformations.standardization import (
    standardize_timestamp,
    normalize_field_names,
    standardize_data_types,
    standardize_record
)
from pipeline.transformations.quality_checks import (
    check_required_fields,
    check_data_types,
    check_email_format,
    perform_quality_checks
)


class TestStandardization:
    """Test data standardization functions."""
    
    def test_standardize_timestamp_string(self):
        """Test timestamp standardization from string."""
        result = standardize_timestamp("2024-01-15 10:30:00")
        assert result == "2024-01-15T10:30:00"
    
    def test_standardize_timestamp_unix(self):
        """Test timestamp standardization from Unix timestamp."""
        # 1705317000 = 2024-01-15 10:30:00 UTC
        result = standardize_timestamp(1705317000)
        assert result.startswith("2024-01-15T")
    
    def test_standardize_timestamp_invalid(self):
        """Test timestamp standardization with invalid input."""
        result = standardize_timestamp("invalid")
        assert result is None
    
    def test_normalize_field_names(self):
        """Test field name normalization."""
        data = {
            "userId": "123",
            "userName": "test",
            "user-email": "test@example.com",
            "accountNumber": "456",
            "_metadata": "keep"
        }
        
        result = normalize_field_names(data)
        
        assert result["user_id"] == "123"
        assert result["user_name"] == "test"
        assert result["user_email"] == "test@example.com"
        assert result["account_number"] == "456"
        assert result["_metadata"] == "keep"
    
    def test_standardize_data_types(self):
        """Test data type standardization."""
        data = {
            "user_id": "123",
            "user_count": "42",
            "transaction_amount": "99.99",
            "user_email": "TEST@EXAMPLE.COM",
            "created_timestamp": "2024-01-15 10:30:00"
        }
        
        result = standardize_data_types(data)
        
        assert result["user_id"] == "123"  # String ID unchanged
        assert result["user_count"] == 42  # Converted to int
        assert result["transaction_amount"] == 99.99  # Converted to float
        assert result["user_email"] == "test@example.com"  # Lowercased
        assert result["created_timestamp"].startswith("2024-01-15T")  # Standardized timestamp
    
    def test_standardize_record_complete(self):
        """Test complete record standardization."""
        data = {
            "userId": "123",
            "userName": "test",
            "transactionAmount": "99.99",
            "createdTimestamp": "2024-01-15 10:30:00"
        }
        
        result = standardize_record(data)
        
        # Check field normalization
        assert "user_id" in result
        assert "user_name" in result
        assert "transaction_amount" in result
        assert "created_timestamp" in result
        
        # Check type conversion
        assert result["transaction_amount"] == 99.99
        assert result["created_timestamp"].startswith("2024-01-15T")
        
        # Check metadata was added
        assert "_processing_metadata" in result
        assert result["_processing_metadata"]["standardized"] is True


class TestQualityChecks:
    """Test data quality check functions."""
    
    def test_check_required_fields_pass(self):
        """Test required fields check with valid data."""
        data = {"id": "123", "timestamp": "2024-01-15T10:30:00"}
        result = check_required_fields(data, ["id", "timestamp"])
        
        assert result.passed is True
        assert len(result.errors) == 0
    
    def test_check_required_fields_fail(self):
        """Test required fields check with missing data."""
        data = {"id": "123"}
        result = check_required_fields(data, ["id", "timestamp"])
        
        assert result.passed is False
        assert len(result.errors) == 1
        assert "timestamp" in result.errors[0]
    
    def test_check_data_types_pass(self):
        """Test data type check with correct types."""
        data = {"id": "123", "user_age": 25}
        result = check_data_types(data, {"id": str, "user_age": int})
        
        assert result.passed is True
        assert len(result.errors) == 0
    
    def test_check_data_types_fail(self):
        """Test data type check with wrong types."""
        data = {"id": "123", "user_age": "twenty-five"}
        result = check_data_types(data, {"id": str, "user_age": int})
        
        assert result.passed is False
        assert len(result.errors) == 1
        assert "user_age" in result.errors[0]
    
    def test_check_email_format_valid(self):
        """Test email format validation with valid emails."""
        assert check_email_format("test@example.com") is True
        assert check_email_format("user.name+tag@domain.co.uk") is True
    
    def test_check_email_format_invalid(self):
        """Test email format validation with invalid emails."""
        assert check_email_format("invalid-email") is False
        assert check_email_format("@domain.com") is False
        assert check_email_format("test@") is False
        assert check_email_format(123) is False
    
    def test_perform_quality_checks_pass(self):
        """Test complete quality check with valid data."""
        data = {
            "id": "test_123",
            "timestamp": "2024-01-15T10:30:00",
            "email": "test@example.com",
            "user_age": 25,
            "transaction_amount": 99.99
        }
        
        result = perform_quality_checks(data)
        
        assert result is not None
        assert "_quality_metadata" in result
        assert result["_quality_metadata"]["passed"] is True
        assert result["_quality_metadata"]["error_count"] == 0
    
    def test_perform_quality_checks_fail(self):
        """Test complete quality check with invalid data."""
        data = {
            "id": "test_123",
            # Missing required timestamp
            "email": "invalid-email",
            "user_age": -5,  # Invalid age
            "transaction_amount": -10  # Invalid amount
        }
        
        result = perform_quality_checks(data)
        
        assert result is None  # Filtered out due to quality failures


class TestIntegration:
    """Integration tests for transformation pipeline."""
    
    def test_full_transformation_pipeline(self):
        """Test complete transformation from raw to processed data."""
        raw_data = {
            "userId": "user_12345",
            "userName": "John Doe",
            "userEmail": "JOHN.DOE@EXAMPLE.COM",
            "transactionAmount": "125.50",
            "createdTimestamp": "2024-01-15 10:30:00",
            "userAge": "30"
        }
        
        # Step 1: Standardization
        standardized = standardize_record(raw_data)
        
        # Step 2: Quality checks
        validated = perform_quality_checks(standardized)
        
        assert validated is not None
        
        # Verify transformations
        assert validated["user_id"] == "user_12345"
        assert validated["user_email"] == "john.doe@example.com"
        assert validated["transaction_amount"] == 125.50
        assert validated["user_age"] == 30
        assert validated["created_timestamp"].startswith("2024-01-15T")
        
        # Verify metadata
        assert "_processing_metadata" in validated
        assert "_quality_metadata" in validated
        assert validated["_quality_metadata"]["passed"] is True