from core.migration_engine import process_record


def test_actual_column_exclusion():
    """
    REAL TEST: Verifies the actual 'process_record' logic for exclusions.
    No simulation here - calling the production function.
    """
    # Real production-style record
    item = {
        "id": "123",
        "secret_token": "PRIVATE",
        "email": "user@example.com",
        "internal_id": "999",
    }

    # Real production-style config
    migration_config = {
        "column_mappings": {
            "__PASSTHROUGH__": "true",
            "__EXCLUDE__": ["secret_token", "internal_id"],
        }
    }

    # CALL THE REAL PRODUCTION FUNCTION
    result = process_record(item, migration_config)

    # Verify the results
    assert "secret_token" not in result
    assert "internal_id" not in result
    assert result["id"] == "123"
    assert result["email"] == "user@example.com"
    assert len(result) == 2


def test_actual_mapping_logic():
    """
    REAL TEST: Verifies the actual 'process_record' logic for custom mappings.
    """
    item = {"first_name": "john", "last_name": "doe", "age": 30}

    migration_config = {
        "column_mappings": {
            "full_name": "{first_name title} {last_name title}",
            "years": "{age}",
            "constant": "STATIC_VAL",
        }
    }

    # CALL THE REAL PRODUCTION FUNCTION
    result = process_record(item, migration_config)

    # Verify the results
    assert result["full_name"] == "John Doe"
    assert result["years"] == 30
    assert result["constant"] == "STATIC_VAL"
    assert len(result) == 3


def test_passthrough_disabled():
    """
    REAL TEST: Verify that passthrough=false only copies mapped columns.
    """
    item = {"id": "123", "other": "data"}

    migration_config = {"column_mappings": {"id": "{id}"}}

    result = process_record(item, migration_config)

    assert "id" in result
    assert "other" not in result
