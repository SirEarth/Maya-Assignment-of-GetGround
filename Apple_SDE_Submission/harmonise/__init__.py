"""
Harmonise module — maps raw partner product names to canonical product models.

Entry point:
    from harmonise import Harmoniser
    h = Harmoniser("Product Ref.csv")
    results = h.match("iP 17 PM 512GB", k=5)
"""

from .harmoniser import Harmoniser, MatchResult, RefEntry
from .extractor import ProductAttributes
from .scorer import ScoreBreakdown

__all__ = [
    "Harmoniser",
    "MatchResult",
    "RefEntry",
    "ProductAttributes",
    "ScoreBreakdown",
]
