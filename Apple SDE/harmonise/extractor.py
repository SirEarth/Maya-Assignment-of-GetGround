"""
Structured attribute extraction from normalized tokens.

Produces a ProductAttributes tuple used both for registry building and for
scoring. The structured form is the DOMINANT signal in our hybrid scorer —
two products are very likely the same model iff their (category, storage,
chip, model_line, connectivity) tuples match.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional

from .dictionary import (
    CATEGORY_KEYWORDS,
    CHIP_PATTERNS,
    CONNECTIVITY_KEYWORDS,
    SIZE_PATTERN,
    STORAGE_PATTERN,
)


# Apple SKU storage sizes in GB. Used as a fallback when a partner omits the
# "GB" suffix (e.g. Partner A's "iP15P 128" meaning 128GB). A bare digit in
# this set is treated as storage when no explicit GB marker was found.
#
# Design note: this list is tight enough (6 values) that false positives are
# unlikely — 11/13/14 (iPad sizes) and 15/16/17 (iPhone model years) are
# deliberately NOT in the set.
APPLE_STORAGE_SIZES_GB = {64, 128, 256, 512, 1024, 2048}


@dataclass
class ProductAttributes:
    """Structured representation of a product name."""

    category:     Optional[str] = None       # IPHONE / IPAD / MAC / AIRPODS / WATCH
    model_line:   Optional[str] = None       # e.g. "17 pro max" / "air" / "(a16)"
    chip:         Optional[str] = None       # A17 Pro / M3 / M4 / A16
    storage_gb:   Optional[int] = None
    connectivity: Optional[str] = None       # WiFi / Cellular
    size_inch:    Optional[float] = None     # iPad / Mac screen size
    tokens:       List[str] = field(default_factory=list)

    def signature(self) -> str:
        """Stable canonical key used to dedupe SKU-level rows into one model."""
        parts = [
            self.category or "?",
            (self.model_line or "").replace(" ", "_"),
            self.chip or "",
            f"{self.storage_gb}gb" if self.storage_gb else "",
            self.connectivity or "",
            f"{self.size_inch}in" if self.size_inch else "",
        ]
        return "|".join(p.lower() for p in parts if p)


def extract(tokens: List[str]) -> ProductAttributes:
    """
    Parse a cleaned token list into a ProductAttributes.

    Example:
        >>> extract(['iphone', '17', 'pro', 'max', '512', 'gb'])
        ProductAttributes(category='IPHONE', model_line='17 pro max',
                          storage_gb=512, ...)
    """
    if not tokens:
        return ProductAttributes()

    joined = " ".join(tokens)
    attrs = ProductAttributes(tokens=list(tokens))

    # --- Category ------------------------------------------------------------
    for keyword, category in CATEGORY_KEYWORDS:
        if keyword in joined:
            attrs.category = category
            break

    # --- Storage -------------------------------------------------------------
    # Primary: explicit "NNN GB" pattern
    m = STORAGE_PATTERN.search(joined)
    if m:
        attrs.storage_gb = int(m.group(1))

    # Fallback: standalone digit token matching a known Apple storage size.
    # Handles partner inputs that omit the GB suffix, e.g. "iP15P 128" → 128GB.
    # Intentionally conservative: set is {64, 128, 256, 512, 1024, 2048} —
    # iPad screen sizes (11/13) and iPhone model years (15/16/17) are NOT in
    # the set, so they won't be mis-detected as storage.
    if attrs.storage_gb is None:
        for tok in tokens:
            if tok.isdigit() and int(tok) in APPLE_STORAGE_SIZES_GB:
                attrs.storage_gb = int(tok)
                break

    # --- Screen size (iPad / Mac) --------------------------------------------
    m = SIZE_PATTERN.search(joined)
    if m:
        try:
            attrs.size_inch = float(m.group(1))
        except ValueError:
            pass

    # --- Chip ---------------------------------------------------------------
    for pattern, replacement in CHIP_PATTERNS:
        m = pattern.search(joined)
        if m:
            attrs.chip = replacement(m) if callable(replacement) else replacement
            break

    # --- Connectivity (cellular precedes wifi — "WiFi + Cellular" = Cellular)
    for keyword, canonical in CONNECTIVITY_KEYWORDS.items():
        if keyword in joined:
            attrs.connectivity = canonical
            if canonical == "Cellular":
                break

    # --- Model line ---------------------------------------------------------
    # Remove known attribute tokens, keep what's left as the model line.
    # Model line for iPhone: "17 pro max" / "air" / "16e"
    # Model line for iPad:   "air" / "mini" / "pro" / "" (base iPad)
    attrs.model_line = _build_model_line(tokens, attrs)

    return attrs


def _build_model_line(tokens: List[str], attrs: ProductAttributes) -> Optional[str]:
    """
    Extract the model descriptor — everything that's not storage/chip/connectivity.

    Keeps these tokens:
        - numeric model designators (15, 16, 16e, 17, ...)
        - product line descriptors (pro, max, air, mini)
        - chip markers embedded as "(a16)", "(m3)" — kept for disambiguation
          between e.g. "iPad (A16)" base vs "iPad Air (M3)"
    """
    # Tokens to drop from the model_line
    skip = set()
    if attrs.category:
        # Remove category keyword from tokens
        for keyword, category in CATEGORY_KEYWORDS:
            if category == attrs.category:
                skip.add(keyword)
    if attrs.storage_gb:
        skip.add("gb")
        skip.add(str(attrs.storage_gb))
    if attrs.connectivity:
        skip.add(attrs.connectivity.lower())
    if attrs.size_inch:
        # Drop size tokens in both forms:
        #   - "13inch" (normaliser's canonical form)
        #   - "13"     (bare digit as it may appear in long descriptions like
        #              "IPAD AIR 13 WF 256GB STL" where ref stores size inline)
        # Leaving bare "13" in tokens pollutes model_line and drags the
        # similarity score down.
        s_int = str(int(attrs.size_inch)) if attrs.size_inch.is_integer() else str(attrs.size_inch)
        skip.add(f"{s_int}inch")
        skip.add(s_int)

    kept = [t for t in tokens if t not in skip]
    # Deduplicate while preserving order — prevents ref-side duplicates
    # (short_desc + long_desc often repeat terms like "ipad air") from
    # dragging down token-set similarity.
    seen = set()
    deduped = []
    for t in kept:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return " ".join(deduped) if deduped else None
