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
    def split(value: str, delimiter: str = ",") -> list:
        """Split string into list."""
        return str(value).split(delimiter)

    @staticmethod
    def join(value: list, delimiter: str = ",") -> str:
        """Join list into string."""
        return delimiter.join(str(item) for item in value)

    @staticmethod
    def substring(value: str, start: int, end: int = None) -> str:
        """Extract substring."""
        if end is None:
            return str(value)[start:]
        return str(value)[start:end]

    @staticmethod
    def pad_left(value: str, width: int, fillchar: str = "0") -> str:
        """Pad string on the left."""
        return str(value).ljust(width, fillchar)

    @staticmethod
    def pad_right(value: str, width: int, fillchar: str = "0") -> str:
        """Pad string on the right."""
        return str(value).rjust(width, fillchar)


class NumberTransformations:
    """Number transformation operations."""

    @staticmethod
    def add(value: Union[int, float], amount: Union[int, float]) -> Union[int, float]:
        """Add to number."""
        return float(value) + float(amount)

    @staticmethod
    def subtract(
        value: Union[int, float], amount: Union[int, float]
    ) -> Union[int, float]:
        """Subtract from number."""
        return float(value) - float(amount)

    @staticmethod
    def multiply(
        value: Union[int, float], factor: Union[int, float]
    ) -> Union[int, float]:
        """Multiply number."""
        return float(value) * float(factor)

    @staticmethod
    def divide(
        value: Union[int, float], divisor: Union[int, float]
    ) -> Union[int, float]:
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
    def power(
        value: Union[int, float], exponent: Union[int, float]
    ) -> Union[int, float]:
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


# Registry of all supported transformations.
# This is the SINGLE SOURCE OF TRUTH for the entire application.
# Format: "name": (function_ref, min_args, max_args, category)
REGISTRY = {
    # String operations
    "upper": (StringTransformations.upper, 0, 0, "string"),
    "lower": (StringTransformations.lower, 0, 0, "string"),
    "title": (StringTransformations.title, 0, 0, "string"),
    "strip": (StringTransformations.strip, 0, 0, "string"),
    "replace": (StringTransformations.replace, 2, 2, "string"),
    "split": (StringTransformations.split, 0, 1, "string"),
    "join": (StringTransformations.join, 0, 1, "string"),
    "substring": (StringTransformations.substring, 1, 2, "string"),
    "pad_left": (StringTransformations.pad_left, 1, 2, "string"),
    "pad_right": (StringTransformations.pad_right, 1, 2, "string"),
    # Number operations
    "add": (NumberTransformations.add, 1, 1, "number"),
    "subtract": (NumberTransformations.subtract, 1, 1, "number"),
    "multiply": (NumberTransformations.multiply, 1, 1, "number"),
    "divide": (NumberTransformations.divide, 1, 1, "number"),
    "round_to": (NumberTransformations.round_to, 0, 1, "number"),
    "abs_value": (NumberTransformations.abs_value, 0, 0, "number"),
    "power": (NumberTransformations.power, 1, 1, "number"),
    "sqrt": (NumberTransformations.sqrt, 0, 0, "number"),
    "floor": (NumberTransformations.floor, 0, 0, "number"),
    "ceil": (NumberTransformations.ceil, 0, 0, "number"),
    "mod": (NumberTransformations.mod, 1, 1, "number"),
}


class TransformationProcessor:
    """
    Main processor for applying transformations using the central REGISTRY.

    This class handles the dispatching of transformation calls based on the
    rules defined in the global REGISTRY.
    """

    def apply_transformation(self, value: Any, transformation_str: str) -> Any:
        """
        Apply a transformation to a value using the registry rules.

        Args:
            value: The input value to transform.
            transformation_str: The full transformation string (e.g. 'add 5').

        Returns:
            The transformed value.

        Raises:
            TransformationError: If the operation is unknown or args are invalid.
        """
        if not transformation_str or not transformation_str.strip():
            return value

        parts = transformation_str.strip().split()
        op_name = parts[0].lower()
        args = parts[1:]

        if op_name not in REGISTRY:
            raise TransformationError(f"Unknown transformation: {op_name}")

        func, min_args, max_args, category = REGISTRY[op_name]

        # Argument count validation (Double-check at runtime)
        if len(args) < min_args or len(args) > max_args:
            raise TransformationError(
                f"Transformation '{op_name}' expects {min_args}-{max_args} args, got {len(args)}"
            )

        try:
            # Automatic numeric conversion based on Registry Category
            if category == "number":
                # For numeric ops, ensure the input and args are treated as floats
                val = (
                    float(value)
                    if value is not None and str(value).lower() != "null"
                    else 0
                )
                return func(val, *[float(a) for a in args])
            else:
                # String operations use the value as-is (converted to string by the methods)
                return func(value, *args)

        except Exception as e:
            raise TransformationError(f"Error executing '{op_name}': {str(e)}")


# Global processor instance
transformation_processor = TransformationProcessor()


def apply_transformation(value: Any, transformation: str) -> Any:
    return transformation_processor.apply_transformation(value, transformation)
