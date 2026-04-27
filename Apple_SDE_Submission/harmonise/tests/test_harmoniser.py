"""
Minimal pytest unit tests for the harmonise module.

Run from the unzipped project folder (containing this README):
    python -m pytest harmonise/tests -v

These tests use targeted inputs rather than the real Product Ref CSV, so the
logic is verified in isolation from dataset quirks.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harmonise.normaliser import normalise
from harmonise.extractor import extract
from harmonise.scorer import (
    attribute_match_score,
    token_jaccard_score,
    char_fuzz_score,
    combined_score,
)


# --------------------------------------------------------------------------
# Normaliser tests
# --------------------------------------------------------------------------
class TestNormalise:
    def test_partner_a_heavy_abbr(self):
        tokens = normalise("iP 17 PM 512GB")
        assert "iphone" in tokens
        assert "17" in tokens
        assert "pro" in tokens
        assert "max" in tokens
        assert "512" in tokens
        assert "gb" in tokens

    def test_partner_a_fused(self):
        tokens = normalise("iP15P 128")
        assert "iphone" in tokens
        assert "15" in tokens
        assert "pro" in tokens

    def test_partner_a_pro_max_fused(self):
        tokens = normalise("iP17PM 1TB")
        assert "iphone" in tokens
        assert "17" in tokens
        assert "pro" in tokens
        assert "max" in tokens
        # 1TB should have been normalized to 1024 gb
        assert "1024" in tokens
        assert "gb" in tokens

    def test_partner_b_verbose(self):
        tokens = normalise(
            "Apple iPad Air 13-inch (M3) - Starlight 256GB Storage - WiFi"
        )
        assert "ipad" in tokens
        assert "air" in tokens
        assert "13inch" in tokens
        assert "m3" in tokens
        assert "256" in tokens
        assert "wifi" in tokens
        # Noise words should be dropped
        assert "apple" not in tokens
        assert "storage" not in tokens
        # Colors should be dropped (they are SKU-level, not model-level)
        assert "starlight" not in tokens

    def test_empty(self):
        assert normalise("") == []
        assert normalise("   ") == []

    def test_tb_normalization(self):
        tokens = normalise("512 TB")
        # 512TB → 524288gb (normalized via PATTERN_EXPANSIONS)
        assert "524288" in tokens
        assert "gb" in tokens


# --------------------------------------------------------------------------
# Extractor tests
# --------------------------------------------------------------------------
class TestExtract:
    def test_iphone_pro_max(self):
        tokens = normalise("iPhone 17 Pro Max 512GB")
        attrs = extract(tokens)
        assert attrs.category == "IPHONE"
        assert attrs.storage_gb == 512
        assert "17" in (attrs.model_line or "")
        assert "pro" in (attrs.model_line or "")
        assert "max" in (attrs.model_line or "")

    def test_ipad_with_chip(self):
        tokens = normalise("iPad (A16) 128GB")
        attrs = extract(tokens)
        assert attrs.category == "IPAD"
        assert attrs.chip == "A16"
        assert attrs.storage_gb == 128

    def test_ipad_air_m3(self):
        tokens = normalise("iPad Air 13-inch (M3) 256GB WiFi")
        attrs = extract(tokens)
        assert attrs.category == "IPAD"
        assert attrs.chip == "M3"
        assert attrs.storage_gb == 256
        assert attrs.size_inch == 13.0
        assert attrs.connectivity == "WiFi"

    def test_cellular_priority(self):
        tokens = normalise("iPad Air WiFi + Cellular 256GB")
        attrs = extract(tokens)
        # "Cellular" should win when both appear
        assert attrs.connectivity == "Cellular"

    def test_signature_dedup(self):
        """Same attributes → identical signature → dedupe target."""
        tokens_a = normalise("iPhone 17 Pro Max 512GB")
        tokens_b = normalise("iPhone 17 Pro Max 512GB White Titanium")
        sig_a = extract(tokens_a).signature()
        sig_b = extract(tokens_b).signature()
        assert sig_a == sig_b


# --------------------------------------------------------------------------
# Scorer tests
# --------------------------------------------------------------------------
class TestScorer:
    def test_category_mismatch_zero_score(self):
        a = extract(normalise("iPhone 17 256GB"))
        b = extract(normalise("iPad Air 256GB"))
        score, _, mismatches = attribute_match_score(a, b)
        assert score == 0.0
        assert "category" in mismatches

    def test_perfect_attribute_match(self):
        a = extract(normalise("iPhone 17 Pro Max 512GB"))
        b = extract(normalise("iPhone 17 Pro Max 512GB"))
        score, matches, _ = attribute_match_score(a, b)
        assert score >= 0.95
        assert "category" in matches
        assert "storage_gb" in matches

    def test_storage_mismatch_low_score(self):
        a = extract(normalise("iPhone 17 Pro Max 128GB"))
        b = extract(normalise("iPhone 17 Pro Max 512GB"))
        score, _, _ = attribute_match_score(a, b)
        # storage weighs heavily — mismatch should drop score substantially
        assert score < 0.75

    def test_jaccard_on_overlap(self):
        assert token_jaccard_score(["iphone", "17", "512", "gb"],
                                    ["iphone", "17", "512", "gb"]) == 1.0
        assert token_jaccard_score([], []) == 0.0
        assert token_jaccard_score(["a"], ["b"]) == 0.0

    def test_char_fuzz_reasonable(self):
        # Near-identical strings — high ratio
        r = char_fuzz_score("iphone 17 pro max 512 gb",
                            "iphone 17 pro max 512 gb")
        assert r == 1.0
        # Completely different — low ratio
        r = char_fuzz_score("iphone", "zzzzzzz")
        assert r < 0.3

    def test_combined_high_confidence(self):
        a_tokens = normalise("iP 17 PM 512GB")
        b_tokens = normalise("iPhone 17 Pro Max 512GB")
        a_attrs = extract(a_tokens)
        b_attrs = extract(b_tokens)

        breakdown = combined_score(
            a_attrs, b_attrs,
            a_tokens, b_tokens,
            " ".join(a_tokens), " ".join(b_tokens),
        )
        assert breakdown.confidence == "HIGH"
        assert breakdown.combined >= 0.85


# --------------------------------------------------------------------------
# End-to-end test (requires Product Ref.csv)
# --------------------------------------------------------------------------
def _ref_path():
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent.parent / "Product Ref.csv",    # Apple SDE/Product Ref.csv
    ]
    return next((p for p in candidates if p.exists()), None)


@pytest.mark.skipif(_ref_path() is None, reason="Product Ref.csv not found")
class TestEndToEnd:
    @pytest.fixture(scope="class")
    def harmoniser(self):
        from harmonise.harmoniser import Harmoniser
        return Harmoniser(_ref_path())

    def test_registry_not_empty(self, harmoniser):
        assert len(harmoniser) > 0

    def test_registry_dedupes_colors(self, harmoniser):
        """Multiple SKU IDs should map to a single product model (color dedup)."""
        models_with_multiple_skus = [
            e for e in harmoniser.registry if len(e.sku_ids) > 1
        ]
        assert len(models_with_multiple_skus) > 0, (
            "Expected at least some models to collapse multiple color SKUs — "
            "if this fails, signature() may be over-distinguishing."
        )

    def test_partner_a_heavy_abbr_matches(self, harmoniser):
        """Partner A's 'iP 17 PM 512GB' should land on an iPhone 17 Pro Max 512GB match."""
        results = harmoniser.match("iP 17 PM 512GB", k=3)
        assert results, "Expected at least one match"
        top = results[0]
        # Top match should be HIGH or MEDIUM confidence (structural attributes align)
        assert top.confidence in ("HIGH", "MEDIUM")
        assert "iphone" in top.canonical_name.lower()

    def test_garbage_input_low_confidence(self, harmoniser):
        results = harmoniser.match("asdf random text", k=3)
        if results:
            # If anything matched, it must be LOW confidence
            assert results[0].confidence == "LOW"

    def test_empty_input_no_results(self, harmoniser):
        assert harmoniser.match("", k=3) == []
        assert harmoniser.match("   ", k=3) == []
