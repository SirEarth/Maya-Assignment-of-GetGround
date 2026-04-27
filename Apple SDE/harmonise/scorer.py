"""
Three independent similarity signals, combined with weights 0.5 / 0.3 / 0.2.

Signals:
    attr_match_score    — structured attribute overlap (DOMINANT, weight 0.5)
    token_jaccard_score — cleaned token set Jaccard similarity (0.3)
    char_fuzz_score     — character-level SequenceMatcher ratio (0.2)

Weights are hardcoded here for the demo; in production they should be
loaded from `dim_anomaly_threshold` (keys signal_weight_*) — same
calibration philosophy as the anomaly detector.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import List, Optional, Tuple

from .extractor import ProductAttributes


# Signal weights — mirror dim_anomaly_threshold design: centralized, tunable.
WEIGHT_ATTR    = 0.5
WEIGHT_JACCARD = 0.3
WEIGHT_FUZZ    = 0.2

# Confidence tier cutoffs.
CONFIDENCE_HIGH   = 0.85
CONFIDENCE_MEDIUM = 0.60

# Structural override: when the structured-attribute score alone is very high,
# force HIGH confidence regardless of combined score.
#
# Rationale: partner-side verbosity (extra color/marketing tokens) or
# abbreviations (no "GB" suffix) can drag token_jaccard and char_fuzz down
# even when the CORE identifying attributes (category + storage + model_line)
# are perfectly aligned. The user observation: if keywords identify the
# product, we shouldn't bounce it to human review — that's wasted effort.
#
# Calibration: 0.95 was picked so that "category + storage + model_line all
# match" easily crosses it (typical attr score in that case is 0.96–1.00).
# Candidates for dim_anomaly_threshold in production.
STRUCTURAL_OVERRIDE_ATTR = 0.95


@dataclass
class ScoreBreakdown:
    """Explainable scoring output — useful for DQ review UI."""
    attr_match:    float
    token_jaccard: float
    char_fuzz:     float
    combined:      float
    confidence:    str        # HIGH / MEDIUM / LOW
    # Diagnostic detail — shows business user WHY this was matched
    attr_matches_on:  List[str] = field(default_factory=list)   # e.g. ['category', 'storage']
    attr_mismatches:  List[str] = field(default_factory=list)


def attribute_match_score(a: ProductAttributes, b: ProductAttributes) -> Tuple[float, List[str], List[str]]:
    """
    Score structured attribute agreement.

    Rules (design choices):
        - `category` hard-match: different categories → score 0 (iPad can never
          be iPhone, no matter how the other signals align).
        - `storage_gb` and `chip` are high-value — each contributes heavily.
        - `model_line` matched via loose equality (to allow "17 pro max" vs
          "17pm" if the latter gets preserved; currently the normalizer
          unifies them).
        - Missing attribute on EITHER side: partial credit.
    """
    matches: List[str] = []
    mismatches: List[str] = []

    # Hard filter: category mismatch is disqualifying
    if a.category and b.category and a.category != b.category:
        return 0.0, [], ["category"]

    attr_points = {
        "category":     (1.5, _equal_or_missing),
        "storage_gb":   (2.0, _equal_or_missing),    # crucial for pricing
        "chip":         (1.0, _equal_or_missing),
        "model_line":   (1.5, _model_line_similarity),
        "connectivity": (0.5, _equal_or_missing),
        "size_inch":    (0.5, _equal_or_missing),
    }

    earned = 0.0
    possible = 0.0
    for field_, (weight, compare) in attr_points.items():
        va = getattr(a, field_)
        vb = getattr(b, field_)
        sim = compare(va, vb)
        if sim is None:                # both missing → ignore this attribute
            continue
        possible += weight
        earned   += weight * sim
        if sim >= 0.8:
            matches.append(field_)
        elif sim < 0.3:
            mismatches.append(field_)

    score = earned / possible if possible > 0 else 0.0
    return score, matches, mismatches


def token_jaccard_score(tokens_a: List[str], tokens_b: List[str]) -> float:
    """Jaccard similarity over cleaned token sets."""
    if not tokens_a or not tokens_b:
        return 0.0
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    intersection = len(set_a & set_b)
    union        = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def char_fuzz_score(text_a: str, text_b: str) -> float:
    """Character-level fuzzy ratio (SequenceMatcher). Cheap backup signal."""
    if not text_a or not text_b:
        return 0.0
    return SequenceMatcher(None, text_a, text_b).ratio()


def combined_score(
    a_attrs: ProductAttributes,
    b_attrs: ProductAttributes,
    a_tokens: List[str],
    b_tokens: List[str],
    a_text: str,
    b_text: str,
) -> ScoreBreakdown:
    """Run all three signals and combine with weights."""
    attr, matches, mismatches = attribute_match_score(a_attrs, b_attrs)
    jacc = token_jaccard_score(a_tokens, b_tokens)
    fuzz = char_fuzz_score(a_text, b_text)

    combined = WEIGHT_ATTR * attr + WEIGHT_JACCARD * jacc + WEIGHT_FUZZ * fuzz

    # Structural override: strong attribute alignment is sufficient by itself
    # — even if partner verbosity / abbreviation idiosyncrasies drag the other
    # signals down. See STRUCTURAL_OVERRIDE_ATTR comment above.
    if attr >= STRUCTURAL_OVERRIDE_ATTR:
        confidence = "HIGH"
    elif combined >= CONFIDENCE_HIGH:
        confidence = "HIGH"
    elif combined >= CONFIDENCE_MEDIUM:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return ScoreBreakdown(
        attr_match    = round(attr, 3),
        token_jaccard = round(jacc, 3),
        char_fuzz     = round(fuzz, 3),
        combined      = round(combined, 3),
        confidence    = confidence,
        attr_matches_on  = matches,
        attr_mismatches  = mismatches,
    )


# ---------------------------------------------------------------------------
# Private comparators
# ---------------------------------------------------------------------------
def _equal_or_missing(a, b) -> Optional[float]:
    """
    Exact equality comparator tolerant of None.

    Returns:
        None if both sides missing (attribute not applicable for this comparison)
        1.0  if equal
        0.5  if one side missing (partial — we don't penalize heavily for absence
             because partner data often omits fields that Ref has)
        0.0  otherwise
    """
    if a is None and b is None:
        return None
    if a is None or b is None:
        return 0.5
    return 1.0 if a == b else 0.0


def _model_line_similarity(a: Optional[str], b: Optional[str]) -> Optional[float]:
    """Fuzzy compare model_line strings — allow minor token ordering differences."""
    if a is None and b is None:
        return None
    if a is None or b is None:
        return 0.5
    if a == b:
        return 1.0
    # Token set equality (ignore order)
    a_set = set(a.split())
    b_set = set(b.split())
    if a_set == b_set:
        return 1.0
    if a_set and b_set:
        overlap = len(a_set & b_set) / max(len(a_set), len(b_set))
        return overlap
    return 0.0
