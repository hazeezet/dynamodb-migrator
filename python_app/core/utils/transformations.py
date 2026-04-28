import re
import math
from typing import Any, Union


class TransformationError(Exception):
    """Custom exception for transformation errors."""
    pass


class StringTransformations:
    """String transformation operations."""
    
    @staticmethod
    def upper(value: str) -> str:
        """Convert to uppercase."""
        return str(value).upper()
    
    @staticmethod
    def lower(value: str) -> str:
        """Convert to lowercase."""
        return str(value).lower()
    
    @staticmethod
    def title(value: str) -> str:
        """Convert to title case."""
        return str(value).title()
    
    @staticmethod
    def strip(value: str) -> str:
        """Remove leading/trailing whitespace."""
        return str(value).strip()
    
    @staticmethod
    def replace(value: str, old: str, new: str) -> str:
        """Replace substring."""
        return str(value).replace(old, new)
    
    @staticmethod
    def split(value: str, delimiter: str = ',') -> list:
        """Split string into list."""
        return str(value).split(delimiter)
    
    @staticmethod
    def join(value: list, delimiter: str = ',') -> str:
        """Join list into string."""
        return delimiter.join(str(item) for item in value)
    
    @staticmethod
    def substring(value: str, start: int, end: int = None) -> str:
        """Extract substring."""
        if end is None:
            return str(value)[start:]
        return str(value)[start:end]
    
    @staticmethod
    def pad_left(value: str, width: int, fillchar: str = '0') -> str:
        """Pad string on the left."""
        return str(value).ljust(width, fillchar)
    
    @staticmethod
    def pad_right(value: str, width: int, fillchar: str = '0') -> str:
        """Pad string on the right."""
        return str(value).rjust(width, fillchar)


class NumberTransformations:
    """Number transformation operations."""
    
    @staticmethod
    def add(value: Union[int, float], amount: Union[int, float]) -> Union[int, float]:
        """Add to number."""
        return float(value) + float(amount)
    
    @staticmethod
    def subtract(value: Union[int, float], amount: Union[int, float]) -> Union[int, float]:
        """Subtract from number."""
        return float(value) - float(amount)
    
    @staticmethod
    def multiply(value: Union[int, float], factor: Union[int, float]) -> Union[int, float]:
        """Multiply number."""
        return float(value) * float(factor)
    
    @staticmethod
    def divide(value: Union[int, float], divisor: Union[int, float]) -> Union[int, float]:
        """Divide number."""
        if float(divisor) == 0:
            raise TransformationError("Cannot divide by zero")
        return float(value) / float(divisor)
    
    @staticmethod
    def round_to(value: Union[int, float], decimals: int = 0) -> Union[int, float]:
        """Round to specified decimal places."""
        return round(float(value), decimals)
    
    @staticmethod
    def abs_value(value: Union[int, float]) -> Union[int, float]:
        """Get absolute value."""
        return abs(float(value))
    
    @staticmethod
    def power(value: Union[int, float], exponent: Union[int, float]) -> Union[int, float]:
        """Raise to power."""
        return pow(float(value), float(exponent))
    
    @staticmethod
    def sqrt(value: Union[int, float]) -> float:
        """Square root."""
        if float(value) < 0:
            raise TransformationError("Cannot take square root of negative number")
        return math.sqrt(float(value))
    
    @staticmethod
    def floor(value: Union[int, float]) -> int:
        """Floor division."""
        return math.floor(float(value))
    
    @staticmethod
    def ceil(value: Union[int, float]) -> int:
        """Ceiling division."""
        return math.ceil(float(value))
    
    @staticmethod
    def mod(value: Union[int, float], divisor: Union[int, float]) -> Union[int, float]:
        """Modulo operation."""
        return float(value) % float(divisor)


class TransformationProcessor:
    """Main processor for applying transformations."""
    
    def __init__(self):
        self.string_ops = StringTransformations()
        self.number_ops = NumberTransformations()
    
    def apply_transformation(self, value: Any, transformation: str) -> Any:
        """
        Apply transformation to value.
        
        Transformation format examples:
        - "upper" -> apply upper()
        - "add 5" -> apply add(5)
        - "replace old new" -> apply replace("old", "new")
        - "substring 0 5" -> apply substring(0, 5)
        """
        if not transformation or not transformation.strip():
            return value
        
        parts = transformation.strip().split()
        operation = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        try:
            # Determine if value is numeric
            is_numeric = isinstance(value, (int, float)) or (
                isinstance(value, str) and self._is_numeric_string(value)
            )
            
            # String transformations
            if hasattr(self.string_ops, operation):
                return self._apply_string_operation(value, operation, args)
            
            # Number transformations
            elif hasattr(self.number_ops, operation) and is_numeric:
                return self._apply_number_operation(value, operation, args)
            
            else:
                raise TransformationError(f"Unknown transformation: {operation}")
                
        except Exception as e:
            raise TransformationError(f"Error applying transformation '{transformation}' to value '{value}': {str(e)}")
    
    def _is_numeric_string(self, value: str) -> bool:
        """Check if string represents a number."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _apply_string_operation(self, value: Any, operation: str, args: list) -> Any:
        """Apply string operation with arguments."""
        method = getattr(self.string_ops, operation)
        
        # Convert args to appropriate types based on operation
        if operation == "replace" and len(args) >= 2:
            return method(value, args[0], args[1])
        elif operation == "split":
            delimiter = args[0] if args else ','
            return method(value, delimiter)
        elif operation == "join":
            delimiter = args[0] if args else ','
            return method(value, delimiter)
        elif operation == "substring":
            start = int(args[0]) if args else 0
            end = int(args[1]) if len(args) > 1 else None
            return method(value, start, end)
        elif operation in ["pad_left", "pad_right"]:
            width = int(args[0]) if args else 10
            fillchar = args[1] if len(args) > 1 else '0'
            return method(value, width, fillchar)
        else:
            return method(value)
    
    def _apply_number_operation(self, value: Any, operation: str, args: list) -> Any:
        """Apply number operation with arguments."""
        method = getattr(self.number_ops, operation)
        numeric_value = float(value) if not isinstance(value, (int, float)) else value
        
        if operation in ["add", "subtract", "multiply", "divide", "power", "mod"]:
            if not args:
                raise TransformationError(f"Operation '{operation}' requires an argument")
            arg_value = float(args[0])
            return method(numeric_value, arg_value)
        elif operation == "round_to":
            decimals = int(args[0]) if args else 0
            return method(numeric_value, decimals)
        else:
            return method(numeric_value)


# Global processor instance
transformation_processor = TransformationProcessor()


def apply_transformation(value: Any, transformation: str) -> Any:
    """
    Convenience function to apply transformation.
    
    Args:
        value: The value to transform
        transformation: The transformation string (e.g., "upper", "add 5")
    
    Returns:
        Transformed value
    """
    return transformation_processor.apply_transformation(value, transformation)
