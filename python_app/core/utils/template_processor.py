import re
import numbers
import logging
from .logger import get_logger
from .transformations import apply_transformation, TransformationError

logger = get_logger()


def apply_template(template, item):
    """
    Apply template with placeholders and transformations to item data.
    
    Supports:
    - Simple placeholders: {field_name}
    - Transformations: {field_name transformation}
    
    Examples:
    - {id} -> direct field value
    - {id upper} -> uppercase transformation
    - {price add 10} -> add 10 to price
    - {name replace John Jane} -> replace John with Jane
    """
    try:
        # Enhanced regex to capture placeholder and optional transformation
        # Pattern: {field_name optional_transformation}
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
                    logger.info(f"Applied transformation '{transformation}' to field '{field_name}': {value}")
                except TransformationError as e:
                    logger.error(f"Transformation error for field '{field_name}': {e}")
                    # Continue with original value if transformation fails
            
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
        return 'null'  # Replace None with 'null' for JSON compatibility
    elif isinstance(value, numbers.Number):
        return str(value)  # Preserve original data types (int, float, Decimal)
    elif isinstance(value, str):
        # Escape quotes for strings
        return value.replace('"', '\\"')
    else:
        return str(value)
