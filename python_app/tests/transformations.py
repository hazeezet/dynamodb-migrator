#!/usr/bin/env python3
"""
Test script to demonstrate the transformation functionality.
"""

from src.utils.template_processor import apply_template
from src.utils.transformations import apply_transformation

# Test data
test_item = {
    'id': 'user123',
    'name': 'john doe',
    'email': 'JOHN.DOE@EXAMPLE.COM',
    'age': 25,
    'price': '100.50',
    'description': 'This is a test description',
    'tags': 'python,programming,aws'
}

print("=== Transformation Test Examples ===\n")

# String transformations
print("1. String Transformations:")
print(f"Original name: '{test_item['name']}'")
print(f"Upper: '{apply_template('{name upper}', test_item)}'")
print(f"Title: '{apply_template('{name title}', test_item)}'")
print()

print(f"Original email: '{test_item['email']}'")
print(f"Lower: '{apply_template('{email lower}', test_item)}'")
print()

# Number transformations
print("2. Number Transformations:")
print(f"Original age: {test_item['age']}")
print(f"Age + 5: {apply_template('{age add 5}', test_item)}")
print(f"Age * 2: {apply_template('{age multiply 2}', test_item)}")
print()

print(f"Original price: '{test_item['price']}'")
print(f"Price + 50: {apply_template('{price add 50}', test_item)}")
print(f"Price rounded: {apply_template('{price round_to 0}', test_item)}")
print()

# String operations
print("3. String Operations:")
print(f"Original description: '{test_item['description']}'")
print(f"Substring (0-4): '{apply_template('{description substring 0 4}', test_item)}'")
print(f"Replace 'test' with 'sample': '{apply_template('{description replace test sample}', test_item)}'")
print()

print(f"Original tags: '{test_item['tags']}'")
print(f"Split tags: {apply_template('{tags split ,}', test_item)}")
print()

# Complex template examples
print("4. Complex Template Examples:")
complex_templates = [
    "User ID: {id upper}",
    "Full name: {name title}",
    "Age in 5 years: {age add 5}",
    "Discounted price: {price multiply 0.9}",
    "Short description: {description substring 0 10}...",
    "Clean email: {email lower}",
]

for template in complex_templates:
    result = apply_template(template, test_item)
    print(f"Template: '{template}' -> '{result}'")

print("\n=== Direct Transformation Tests ===\n")

# Direct transformation tests
print("5. Direct Transformation Tests:")
test_values = [
    ("hello world", "upper"),
    ("HELLO WORLD", "lower"),
    ("hello world", "title"),
    (42, "add 10"),
    (100.75, "multiply 2"),
    ("123.456", "round_to 2"),
    ("hello,world,python", "split ,"),
    ("test string", "replace test demo"),
    ("short", "pad_left 10 0"),
]

for value, transform in test_values:
    try:
        result = apply_transformation(value, transform)
        print(f"Value: {value} | Transform: '{transform}' -> {result}")
    except Exception as e:
        print(f"Value: {value} | Transform: '{transform}' -> ERROR: {e}")

print("\n=== Test Complete ===")
