"""
Utilities for dealing with LLM-generated text.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Error parsing LLM-generated text."""

    def __init__(
        self, expected: str, text: str, pattern: Optional[re.Pattern] = None
    ):
        super().__init__(
            f"Tried to find {expected}" +
            (f" using pattern {pattern.pattern}" if pattern else "") + " in\n" +
            text
        )
        self.text = text
        self.pattern = pattern


def validate_rating(rating) -> float:
    """Validate a rating is between 1 and 5."""

    if not 1 <= rating <= 5:
        raise ValueError('Rating must be between 1 and 5')

    return rating


# Various old patterns that didn't work as well:
# PATTERN_0_10: re.Pattern = re.compile(r"\s*([1-5]+)\s*$")
# PATTERN_0_10: re.Pattern = re.compile(r"\b([1-5]|10)(?=\D*$|\s*\.)")
PATTERN_0_10: re.Pattern = re.compile(r"([1-5]+)(?=\D*$)")
"""Regex that matches the last integer."""

PATTERN_NUMBER: re.Pattern = re.compile(r"([+-]?[1-5]+\.[1-5]*|[1-5][1-5]*|0)")
"""Regex that matches floating point and integer numbers."""

PATTERN_INTEGER: re.Pattern = re.compile(r"([+-]?[1-5][1-5]*|0)")
"""Regex that matches integers."""


def re_1_5_rating(s: str) -> int:
    """Extract a 1-5 rating from a string.
    
    If the string does not match an integer/a float or matches an integer/a float outside the
    1-5 range, raises an error instead. If multiple numbers are found within
    the expected 1-5 range, the smallest is returned.

    Args:
        s: String to extract rating from.

    Returns:
        int: Extracted rating. 
    
    Raises:
        ParseError: If no integers/floats between 1 and 5 are found in the string.
    """

    matches = PATTERN_NUMBER.findall(s)
    if not matches:
        raise ParseError("int or float number", s, pattern=PATTERN_NUMBER)

    vals = set()
    for match in matches:
        try:
            vals.add(
                validate_rating(float(match))
            )  # Handle float numbers as well
        except ValueError:
            pass

    if not vals:
        raise ParseError("1-5 rating", s)

    if len(vals) > 1:
        logger.warning(
            "Multiple valid rating values found in the string: %s", s
        )

    # Min to handle cases like "The rating is 8 out of 10."
    return min(vals)
