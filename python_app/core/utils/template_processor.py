import re
import numbers
from .logger import get_logger
from .transformations import apply_transformation, TransformationError

logger = get_logger()


def validate_template(template):
    """
    Validate template string for syntax and transformation rules.
    """
    # 1. Balanced brackets check
    if template.count('{') != template.count('}'):
        raise ValueError("Brackets are not balanced. Ensure every '{' has a closing '}'.")

    # 2. Extract and validate each placeholder
    placeholder_pattern = r'\{([^}]+)\}'
    placeholders = re.findall(placeholder_pattern, template)
    
    for content in placeholders:
        parts = content.strip().split()
        if len(parts) > 1:
            op = parts[1].lower()
            args = parts[2:]
            _validate_operation(op, args)
    
    return True

def _validate_operation(op, args):
    """Verify operation and argument counts using the central REGISTRY."""
    from .transformations import REGISTRY
    
    if op not in REGISTRY:
        raise ValueError(f"Unknown transformation: '{op}'")
    
    _, min_args, max_args, _ = REGISTRY[op]
    if len(args) < min_args or len(args) > max_args:
        if min_args == max_args:
            raise ValueError(f"Transformation '{op}' expects exactly {min_args} arguments, but got {len(args)}")
        else:
            raise ValueError(f"Transformation '{op}' expects {min_args} to {max_args} arguments, but got {len(args)}")

def apply_template(template, item):
    """
    Apply template with placeholders and transformations to item data.
    """
    # First validate
    validate_template(template)
    
    try:
        placeholder_pattern = r'\{([^}]+)\}'
        placeholders = re.findall(placeholder_pattern, template)
        
        for placeholder_content in placeholders:
            # Split placeholder content into field name and transformation
            parts = placeholder_content.strip().split(' ', 1)
            field_name = parts[0]
            transformation = parts[1] if len(parts) > 1 else None
            
            # Get value from item
            value = item.get(field_name, None)
            
            # Apply transformation if specified
            if transformation:
                try:
                    value = apply_transformation(value, transformation)
                except TransformationError as e:
                    logger.error(f"Transformation error for field '{field_name}': {e}")
            
            # Convert value to appropriate format
            replacement = _format_value(value)
            
            # Replace the placeholder in template
            full_placeholder = f"{{{placeholder_content}}}"
            template = template.replace(full_placeholder, str(replacement))
        
        return template
        
    except Exception as e:
        logger.error(f"Template Processing Error: {e}")
        return ""


def _format_value(value):
    """Format value for template replacement."""
    if isinstance(value, (dict, list)):
        return value  # Keep as dict or list
    elif value is None:
        return 'null'
    elif isinstance(value, numbers.Number):
        return str(value)
    elif isinstance(value, str):
        return value.replace('"', '\\"')
    else:
        return str(value)

