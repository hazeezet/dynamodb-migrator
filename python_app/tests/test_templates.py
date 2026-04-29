import pytest
from core.utils.template_processor import validate_template, apply_template
from core.utils.transformations import REGISTRY, TransformationError

def test_template_validation_success():
    """Test that valid templates pass validation."""
    assert validate_template("{name upper}") is True
    assert validate_template("{age add 10}") is True
    assert validate_template("USER#{id upper}") is True
    assert validate_template("{price multiply 1.5}") is True
    assert validate_template("{text replace old new}") is True

def test_template_validation_failures():
    """Test that invalid templates raise ValueError."""
    # Unknown transformation
    with pytest.raises(ValueError, match="Unknown transformation: 'magic'"):
        validate_template("{name magic}")
    
    # Unbalanced brackets
    with pytest.raises(ValueError, match="Brackets are not balanced"):
        validate_template("{name upper")
    
    # Wrong argument counts
    with pytest.raises(ValueError, match="expects exactly 0 arguments"):
        validate_template("{name upper arg1}")
    
    with pytest.raises(ValueError, match="expects exactly 1 argument"):
        validate_template("{age add 10 20}")
    
    with pytest.raises(ValueError, match="expects exactly 2 arguments"):
        validate_template("{text replace onlyone}")

def test_apply_template_logic():
    """Test the actual transformation logic in the template processor."""
    item = {
        "first_name": "john",
        "last_name": "doe",
        "age": "25",
        "score": 10.5
    }
    
    # Simple substitution
    assert apply_template("{first_name}", item) == "john"
    
    # String transformation
    assert apply_template("{first_name upper}", item) == "JOHN"
    
    # Combined string
    assert apply_template("{first_name title} {last_name title}", item) == "John Doe"
    
    # Numeric transformation (handling string input)
    assert apply_template("{age add 5}", item) == "30"
    
    # Numeric transformation (handling float input)
    assert apply_template("{score multiply 2}", item) == "21"

def test_registry_integrity():
    """Verify that all items in the REGISTRY have correct structure."""
    for name, config in REGISTRY.items():
        assert len(config) == 4
        func, min_args, max_args, category = config
        assert callable(func)
        assert isinstance(min_args, int)
        assert isinstance(max_args, int)
        assert category in ["string", "number"]

def test_numeric_edge_cases():
    """Test how numeric operations handle nulls or invalid strings."""
    from core.utils.transformations import apply_transformation, TransformationError
    
    # Should handle None by defaulting to 0
    assert apply_transformation(None, "add 10") == 10
    
    # Invalid string for numeric op should raise an error
    with pytest.raises(TransformationError):
        apply_transformation("not_a_number", "add 10")
