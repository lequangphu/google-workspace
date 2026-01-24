import re

# Test the regex in clean_product_names.py
pattern = r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)"

test_cases = [
    ("70/90-17 TT (N)", "70/90-17 T/T-N"),
    ("70/90-17 TT (N, S)", "70/90-17 T/T-N/S"),
    ("70/90-17 TT (N, )", "70/90-17 T/T-N"),
]

for test_input, expected in test_cases:
    result = re.sub(
        pattern,
        lambda m: f"-{m.group(1).replace(', ', '/').replace(',', '/').replace(' ', '')}",
        test_input,
    )

    print(f"Input: {repr(test_input)}")
    print(f"Result: {repr(result)}")
    print(f"Expected: {repr(expected)}")
    print(f"Match: {result == expected}")
    print()
