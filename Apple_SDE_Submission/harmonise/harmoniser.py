"""
Main Harmoniser engine.

Registry building:
    Product Ref.csv has SKU-level rows (per color). We aggregate them into
    product-model-level entries by the attribute signature, preserving the
    list of original SKU IDs for traceability (matches dim_product_sku.ref
    _product_id <-> dim_product_model.product_model_id structure in schema.sql).

Match flow:
    1. Normalize + extract attributes from the query
    2. Score query against EVERY registry entry
    3. Return top-K ranked by combined score
    4. Tag each with confidence tier (HIGH / MEDIUM / LOW)

Low-confidence matches should be written to dq_bad_records for business
review (the dictionary's Layer 3 promotion path).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .normaliser import normalise
from .extractor import extract, ProductAttributes
from .scorer import combined_score, ScoreBreakdown


@dataclass
class RefEntry:
    """Aggregated product-model-level entry from Product Ref.csv."""
    model_key:      str                           # e.g. 'iphone|17_pro_max|a17_pro|512gb'
    canonical_name: str                           # e.g. 'iPhone 17 Pro Max 512GB'
    category:       str
    attributes:     ProductAttributes
    tokens:         List[str]                      # cleaned tokens for Jaccard
    sku_ids:        List[int] = field(default_factory=list)   # original Ref Product IDs


@dataclass
class MatchResult:
    """One candidate match, ready to serve back to /harmonise-product API."""
    model_key:      str
    canonical_name: str
    sku_ids:        List[int]
    score:          float
    confidence:     str                           # HIGH / MEDIUM / LOW
    breakdown:      ScoreBreakdown

    def to_dict(self) -> dict:
        return {
            "model_key":      self.model_key,
            "canonical_name": self.canonical_name,
            "sku_ids":        self.sku_ids,
            "score":          self.score,
            "confidence":     self.confidence,
            "signal_breakdown": {
                "attr_match":     self.breakdown.attr_match,
                "token_jaccard":  self.breakdown.token_jaccard,
                "char_fuzz":      self.breakdown.char_fuzz,
                "attr_matched":   self.breakdown.attr_matches_on,
                "attr_mismatched": self.breakdown.attr_mismatches,
            },
        }


class Harmoniser:
    """
    Hybrid structured + fuzzy product name harmoniser.

    Usage:
        h = Harmoniser("Product Ref.csv")
        results = h.match("iP 17 PM 512GB", k=5)
        for r in results:
            print(r.model_key, r.score, r.confidence)
    """

    def __init__(self, ref_csv_path: str | Path):
        self.ref_csv_path = Path(ref_csv_path)
        self.registry: List[RefEntry] = self._build_registry()

    # ------------------------------------------------------------------ API
    def match(self, product_name: str, k: int = 5) -> List[MatchResult]:
        """
        Return top-K candidate matches for a raw product name.

        Args:
            product_name: raw name from partner feed
            k: number of candidates to return

        Returns:
            list of MatchResult, sorted descending by score.
            Empty list if product_name is empty.
        """
        if not product_name or not product_name.strip():
            return []

        query_tokens = normalise(product_name)
        query_attrs  = extract(query_tokens)
        query_text   = " ".join(query_tokens)

        scored: List[MatchResult] = []
        for entry in self.registry:
            breakdown = combined_score(
                query_attrs, entry.attributes,
                query_tokens, entry.tokens,
                query_text, " ".join(entry.tokens),
            )
            # Hard filter: if the structured category mismatches, skip entirely
            if breakdown.attr_match == 0 and "category" in breakdown.attr_mismatches:
                continue

            scored.append(MatchResult(
                model_key      = entry.model_key,
                canonical_name = entry.canonical_name,
                sku_ids        = entry.sku_ids,
                score          = breakdown.combined,
                confidence     = breakdown.confidence,
                breakdown      = breakdown,
            ))

        scored.sort(key=lambda r: r.score, reverse=True)
        return scored[:k]

    # -------------------------------------------------------- Registry build
    def _build_registry(self) -> List[RefEntry]:
        """
        Parse Product Ref.csv → aggregate SKU rows into product-model entries.

        Ref CSV columns: Product Category, Product Short Description,
                         Product Long Description, Product ID
        """
        groups: Dict[str, RefEntry] = {}

        with self.ref_csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                category     = (row.get("Product Category") or "").strip()
                short_desc   = (row.get("Product Short Description") or "").strip()
                long_desc    = (row.get("Product Long Description") or "").strip()
                product_id   = row.get("Product ID")
                if not category or not short_desc or not product_id:
                    continue

                # Short desc + long desc combined — short gives clean model line,
                # long provides storage info missing from some short descriptions.
                combined_text = f"{category} {short_desc} {long_desc}"
                tokens = normalise(combined_text)
                attrs  = extract(tokens)

                # Ref table doesn't always include explicit category in short desc
                # (some rows start with just "iPad (A16) Cell") — the category
                # column in the CSV is authoritative.
                if not attrs.category:
                    if category.lower() == "iphone":
                        attrs.category = "IPHONE"
                    elif category.lower() == "ipad":
                        attrs.category = "IPAD"
                    elif category.lower() == "airpods":
                        attrs.category = "AIRPODS"
                    elif category.lower().startswith("mac"):
                        attrs.category = "MAC"
                    elif category.lower() == "watch":
                        attrs.category = "WATCH"

                sig = attrs.signature()
                if sig not in groups:
                    groups[sig] = RefEntry(
                        model_key      = sig,
                        canonical_name = short_desc,
                        category       = attrs.category or category,
                        attributes     = attrs,
                        tokens         = tokens,
                        sku_ids        = [],
                    )
                try:
                    groups[sig].sku_ids.append(int(product_id))
                except ValueError:
                    pass

        return list(groups.values())

    # ----------------------------------------------------------- Diagnostics
    def __len__(self) -> int:
        return len(self.registry)

    def summary(self) -> dict:
        """Aggregate registry stats — useful for debugging and sanity checks."""
        categories: Dict[str, int] = {}
        for e in self.registry:
            cat = e.category or "UNKNOWN"
            categories[cat] = categories.get(cat, 0) + 1
        return {
            "total_models": len(self.registry),
            "total_skus":   sum(len(e.sku_ids) for e in self.registry),
            "by_category":  categories,
        }
