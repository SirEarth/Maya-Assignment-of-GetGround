"""
Text normalization pipeline.

Pipeline order:
    1. Lowercase
    2. Strip quotes / parens / punctuation (keep hyphens inside numbers like 13-inch)
    3. Apply PATTERN_EXPANSIONS (regex) — handles contextual abbreviations
    4. Tokenize on whitespace
    5. Apply TOKEN_ABBREVIATIONS (whole-token replace)
    6. Drop NOISE_WORDS
    7. Return cleaned token list
"""

import re
from typing import List

from .dictionary import (
    PATTERN_EXPANSIONS,
    TOKEN_ABBREVIATIONS,
    NOISE_WORDS,
)


_PUNCT_STRIP = re.compile(r"[,()+\[\]{}]")    # punctuation to strip (keep dots/hyphens/slashes)
_WHITESPACE = re.compile(r"\s+")


def normalise(text: str) -> List[str]:
    """
    Normalize a raw product name to a list of clean, canonical tokens.

    Example:
        >>> normalise("iP 17 PM 512GB")
        ['iphone', '17', 'pro', 'max', '512', 'gb']

        >>> normalise("Apple iPad Air 13-inch (M3) - Starlight 256GB Storage - WiFi")
        ['ipad', 'air', '13inch', 'm3', '256', 'gb', 'wifi']
    """
    if not text:
        return []

    # Step 1–2: lowercase + strip noisy punctuation
    s = text.lower()
    s = _PUNCT_STRIP.sub(" ", s)

    # Step 3: regex-level expansions (fused abbreviations, unit normalization)
    for pattern, replacement in PATTERN_EXPANSIONS:
        s = pattern.sub(replacement, s)

    # Step 4: tokenize
    tokens = _WHITESPACE.split(s.strip())
    tokens = [t for t in tokens if t]

    # Step 5: token-level abbreviation expansion (may produce multi-word expansions)
    expanded: List[str] = []
    for tok in tokens:
        # Strip residual punctuation from token boundaries
        tok = tok.strip("-.,;:'\"")
        if not tok:
            continue
        if tok in TOKEN_ABBREVIATIONS:
            expanded.extend(TOKEN_ABBREVIATIONS[tok].split())
        else:
            expanded.append(tok)

    # Step 6: drop noise words
    cleaned = [t for t in expanded if t not in NOISE_WORDS]

    return cleaned
