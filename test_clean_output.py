#!/usr/bin/env python3
"""
Test script for clean_json_to_csv function
Run: python test_clean_output.py
"""

import json

def clean_json_to_csv(value) -> str:
    """
    Convert JSON array/object to clean comma-separated string.
    Removes [], {}, "", '' and returns comma-separated values.
    """
    if not value:
        return ''

    # Helper to clean a string from brackets and quotes
    def clean_str(s):
        for char in ['[', ']', '{', '}', '"', "'"]:
            s = s.replace(char, '')
        return s.strip()

    # If it's already a list, join it
    if isinstance(value, list):
        cleaned_items = [clean_str(str(item)) for item in value if item]
        return ', '.join(item for item in cleaned_items if item)

    # If it's a dict, format as key: value pairs
    if isinstance(value, dict):
        return ', '.join(f"{k}: {clean_str(str(v))}" for k, v in value.items() if v)

    # If it's a string, try to parse as JSON
    if isinstance(value, str):
        value = value.strip()

        # Try to parse as JSON
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                cleaned_items = [clean_str(str(item)) for item in parsed if item]
                return ', '.join(item for item in cleaned_items if item)
            if isinstance(parsed, dict):
                return ', '.join(f"{k}: {clean_str(str(v))}" for k, v in parsed.items() if v)
        except (json.JSONDecodeError, TypeError):
            pass

        # If not valid JSON, just clean the string manually
        cleaned = value
        for char in ['[', ']', '{', '}', '"', "'"]:
            cleaned = cleaned.replace(char, '')

        # Clean up extra spaces and commas
        cleaned = ', '.join(part.strip() for part in cleaned.split(',') if part.strip())
        return cleaned

    return str(value)


# ============================================
# TEST CASES - Dummy Data
# ============================================

test_cases = [
    # Test 1: JSON array string
    {
        'name': 'JSON array string',
        'input': '["Product A", "Product B", "Product C"]',
        'expected': 'Product A, Product B, Product C'
    },
    # Test 2: JSON array with single quotes inside
    {
        'name': 'JSON array with single quotes',
        'input': "[\"Product 'A'\", \"Service 'B'\"]",
        'expected': 'Product A, Service B'
    },
    # Test 3: Python list
    {
        'name': 'Python list',
        'input': ['Item1', 'Item2', 'Item3'],
        'expected': 'Item1, Item2, Item3'
    },
    # Test 4: Dict/object
    {
        'name': 'Python dict',
        'input': {'key1': 'value1', 'key2': 'value2'},
        'expected': 'key1: value1, key2: value2'
    },
    # Test 5: JSON object string
    {
        'name': 'JSON object string',
        'input': '{"product": "iPhone", "price": "999"}',
        'expected': 'product: iPhone, price: 999'
    },
    # Test 6: Messy string with brackets
    {
        'name': 'Messy string with brackets',
        'input': '{"items": ["a", "b"]}',
        'expected': 'items: a, b'
    },
    # Test 7: String with single quotes only
    {
        'name': 'String with single quotes',
        'input': "['Item A', 'Item B']",
        'expected': 'Item A, Item B'
    },
    # Test 8: Empty value
    {
        'name': 'Empty string',
        'input': '',
        'expected': ''
    },
    # Test 9: None
    {
        'name': 'None value',
        'input': None,
        'expected': ''
    },
    # Test 10: Real ML output example - products
    {
        'name': 'Real ML products output',
        'input': '["Mobile Plan", "Internet Package", "TV Bundle"]',
        'expected': 'Mobile Plan, Internet Package, TV Bundle'
    },
    # Test 11: Real ML output example - action items
    {
        'name': 'Real ML action items output',
        'input': '["Follow up with customer", "Send invoice", "Schedule callback"]',
        'expected': 'Follow up with customer, Send invoice, Schedule callback'
    },
    # Test 12: Nested brackets and quotes
    {
        'name': 'Nested brackets and quotes',
        'input': '{["value\'s here"]}',
        'expected': 'values here'
    },
    # Test 13: Hebrew text (real use case)
    {
        'name': 'Hebrew text in array',
        'input': '["חבילת סלולר", "אינטרנט"]',
        'expected': 'חבילת סלולר, אינטרנט'
    },
]


def run_tests():
    print("=" * 60)
    print("Testing clean_json_to_csv function")
    print("=" * 60)

    passed = 0
    failed = 0

    for i, test in enumerate(test_cases, 1):
        result = clean_json_to_csv(test['input'])
        status = "PASS" if result == test['expected'] else "FAIL"

        if status == "PASS":
            passed += 1
        else:
            failed += 1

        print(f"\nTest {i}: {test['name']}")
        print(f"  Input:    {repr(test['input'])}")
        print(f"  Expected: {repr(test['expected'])}")
        print(f"  Got:      {repr(result)}")
        print(f"  Status:   [{status}]")

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
