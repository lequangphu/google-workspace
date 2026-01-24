import re

test_cases = [
    ("70/90-17 TT (N)", "70/90-17 T/T-N"),
    ("70/90-17 TT (N, S)", "70/90-17 T/T-N/S"),
    ("70/90-17 TT (N, )", "70/90-17 T/T-N"),
]

pattern = r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)"

for input, expected in test_cases:
    result = re.sub(
        pattern,
        lambda m: f"-{m.group(1).replace(', ', '/').replace(',', '/').replace(' ', '')}",
        input,
    )
    status = "PASS" if result == expected else "FAIL"
    print(f"{status}: {repr(input)} -> {repr(result)} (expected {repr(expected)})")
