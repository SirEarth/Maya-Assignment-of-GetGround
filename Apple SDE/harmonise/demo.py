"""
Demo script — runs the harmoniser against sample queries drawn from
Partner A / Partner B CSVs.

Run from project root:
    python -m harmonise.demo

Or from the Apple SDE directory:
    cd "Apple SDE"
    python -m harmonise.demo
"""

from __future__ import annotations

from pathlib import Path

from .harmoniser import Harmoniser


# Sample queries drawn from the actual Partner A / B data, plus adversarial cases.
SAMPLE_QUERIES = [
    # --- Partner A style: heavy abbreviations ---
    ("Partner A heavy abbr",  "iP 17 PM 512GB"),
    ("Partner A fused abbr",  "iP15P 128"),
    ("Partner A mixed",       "iP 17 PM 1TB"),
    ("Partner A Air",         "iPhone Air 256GB"),
    ("Partner A iPad A16",    "iPad (A16) 128GB"),
    ("Partner A iPad M3",     "iPad Air 11-inch (M3) 256GB"),
    ("Partner A iPhone",      "iPhone 15 Pro Max 512GB"),

    # --- Partner B style: verbose, colors, extra descriptors ---
    ("Partner B iPad Air",
     "Apple iPad Air 13-inch (M3) - Starlight 256GB Storage - WiFi"),
    ("Partner B iPhone",
     "Apple iPhone 16 Pro 256GB White Titanium"),
    ("Partner B iPad mini",
     "Apple iPad mini (A17 Pro chip) 8.3\" - Space Grey 256GB Storage - WiFi"),
    ("Partner B iPad Pro",
     'Apple iPad Pro 13" (M4) - Space Black 512GB Storage - Cellular + WiFi - Apple M4 Chip with Standard Glass'),

    # --- Adversarial: unseen abbreviations (should land in LOW confidence) ---
    ("Unseen abbreviation",   "iP 17 PM NEW EDITION 512GB"),
    ("Garbage",               "asdf random text"),
    ("Empty",                 ""),
]


def main() -> None:
    # Resolve Product Ref.csv — works whether CWD is repo root or Apple SDE/
    here      = Path(__file__).resolve().parent
    candidates = [
        here.parent / "Product Ref.csv",         # when run from Apple SDE/
        Path.cwd()  / "Apple SDE" / "Product Ref.csv",   # from repo root
        Path.cwd()  / "Product Ref.csv",
    ]
    ref_path = next((p for p in candidates if p.exists()), None)
    if ref_path is None:
        raise FileNotFoundError(
            "Could not locate Product Ref.csv. "
            f"Tried: {[str(p) for p in candidates]}"
        )

    print(f"Loading registry from: {ref_path}")
    h = Harmoniser(ref_path)
    s = h.summary()
    print(f"Registry built — {s['total_models']} unique models, "
          f"{s['total_skus']} underlying SKUs")
    print(f"  by category: {s['by_category']}\n")

    # --- Run demo queries --------------------------------------------------
    for label, query in SAMPLE_QUERIES:
        _print_query_result(h, label, query, k=3)


def _print_query_result(h: Harmoniser, label: str, query: str, k: int = 3) -> None:
    print("=" * 76)
    print(f"[{label}]  {query!r}")
    print("-" * 76)
    results = h.match(query, k=k)
    if not results:
        print("  (no candidates)")
        return
    for i, r in enumerate(results, 1):
        print(f"  #{i}  score={r.score:.3f}  conf={r.confidence:6s}  "
              f"{r.canonical_name!r}")
        print(f"      attr={r.breakdown.attr_match:.2f}  "
              f"jacc={r.breakdown.token_jaccard:.2f}  "
              f"fuzz={r.breakdown.char_fuzz:.2f}  "
              f"skus={r.sku_ids[:4]}{'...' if len(r.sku_ids) > 4 else ''}")
        if r.breakdown.attr_matches_on:
            print(f"      matched on: {', '.join(r.breakdown.attr_matches_on)}")
        if r.breakdown.attr_mismatches:
            print(f"      mismatched: {', '.join(r.breakdown.attr_mismatches)}")
    print()


if __name__ == "__main__":
    main()
