import re
import structlog
from typing import Dict, Any, List, Optional, Union

logger = structlog.get_logger()


class QualityCheckResult:
    def __init__(self, passed: bool, errors: List[str] = None):
        self.passed = passed
        self.errors = errors or []
    
    def add_error(self, error: str):
        self.errors.append(error)
        self.passed = False


def check_required_fields(data: Dict[str, Any], required_fields: List[str]) -> QualityCheckResult:
    """Check that all required fields are present and not null."""
    result = QualityCheckResult(True)
    
    for field in required_fields:
        if field not in data:
            result.add_error(f"Missing required field: {field}")
        elif data[field] is None:
            result.add_error(f"Required field is null: {field}")
        elif isinstance(data[field], str) and not data[field].strip():
            result.add_error(f"Required field is empty: {field}")
    
    return result


def check_data_types(data: Dict[str, Any], type_specs: Dict[str, type]) -> QualityCheckResult:
    """Check that fields have the expected data types."""
    result = QualityCheckResult(True)
    
    for field, expected_type in type_specs.items():
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_type):
                result.add_error(f"Field {field} has wrong type: expected {expected_type.__name__}, got {type(data[field]).__name__}")
    
    return result


def check_email_format(email: str) -> bool:
    """Validate email format."""
    if not isinstance(email, str):
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def check_value_ranges(data: Dict[str, Any], range_specs: Dict[str, Dict[str, Union[int, float]]]) -> QualityCheckResult:
    """Check that numeric fields are within expected ranges."""
    result = QualityCheckResult(True)
    
    for field, range_spec in range_specs.items():
        if field in data and data[field] is not None:
            value = data[field]
            
            if not isinstance(value, (int, float)):
                continue
            
            min_val = range_spec.get('min')
            max_val = range_spec.get('max')
            
            if min_val is not None and value < min_val:
                result.add_error(f"Field {field} value {value} is below minimum {min_val}")
            
            if max_val is not None and value > max_val:
                result.add_error(f"Field {field} value {value} is above maximum {max_val}")
    
    return result


def check_string_patterns(data: Dict[str, Any], pattern_specs: Dict[str, str]) -> QualityCheckResult:
    """Check that string fields match expected patterns."""
    result = QualityCheckResult(True)
    
    for field, pattern in pattern_specs.items():
        if field in data and data[field] is not None:
            value = data[field]
            
            if not isinstance(value, str):
                continue
            
            if not re.match(pattern, value):
                result.add_error(f"Field {field} value '{value}' does not match pattern '{pattern}'")
    
    return result


def check_business_rules(data: Dict[str, Any]) -> QualityCheckResult:
    """Apply custom business logic validation."""
    result = QualityCheckResult(True)
    
    # Example business rules
    
    # Rule 1: If user_age is present, it should be reasonable
    if "user_age" in data and data["user_age"] is not None:
        age = data["user_age"]
        if isinstance(age, (int, float)) and (age < 0 or age > 150):
            result.add_error(f"Unrealistic age value: {age}")
    
    # Rule 2: Email format validation
    if "email" in data and data["email"] is not None:
        if not check_email_format(data["email"]):
            result.add_error(f"Invalid email format: {data['email']}")
    
    # Rule 3: Transaction amount should be positive
    if "transaction_amount" in data and data["transaction_amount"] is not None:
        amount = data["transaction_amount"]
        if isinstance(amount, (int, float)) and amount <= 0:
            result.add_error(f"Transaction amount must be positive: {amount}")
    
    return result


def perform_quality_checks(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Perform comprehensive quality checks on a record."""
    try:
        logger.debug("Starting quality checks", data_keys=list(data.keys()))
        
        all_errors = []
        
        # Define validation rules
        required_fields = ["id", "timestamp"]
        type_specs = {
            "id": str,
            "user_age": int,
            "transaction_amount": (int, float)
        }
        range_specs = {
            "user_age": {"min": 0, "max": 150},
            "transaction_amount": {"min": 0}
        }
        pattern_specs = {
            "id": r'^[a-zA-Z0-9_-]+$'
        }
        
        # Run all checks
        checks = [
            check_required_fields(data, required_fields),
            check_data_types(data, type_specs),
            check_value_ranges(data, range_specs),
            check_string_patterns(data, pattern_specs),
            check_business_rules(data)
        ]
        
        # Collect all errors
        for check in checks:
            all_errors.extend(check.errors)
        
        # Add quality metadata
        data["_quality_metadata"] = {
            "checked_at": data.get("_processing_metadata", {}).get("processed_at"),
            "passed": len(all_errors) == 0,
            "errors": all_errors,
            "error_count": len(all_errors)
        }
        
        if all_errors:
            logger.warning("Quality check failed", 
                         errors=all_errors, 
                         data_id=data.get("id"))
            return None  # Filter out failed records
        
        logger.debug("Quality checks passed", data_id=data.get("id"))
        return data
        
    except Exception as e:
        logger.error("Failed to perform quality checks", error=str(e), data=data)
        # Return None to filter out problematic records
        return None