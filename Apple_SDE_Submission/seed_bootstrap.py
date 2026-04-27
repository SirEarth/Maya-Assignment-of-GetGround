"""
seed_bootstrap.py — populate non-transactional dimension data.

What this script does:
  1. Parse Product Ref.csv (Apple's official product registry, the
     ground-truth source) into dim_product_model + dim_product_sku.
     This is structured extraction (regex-based attribute parsing),
     NOT fuzzy harmonisation — Product Ref is authoritative, no
     similarity scoring needed.
  2. Seed dim_currency_rate_snapshot with Foreign Exchange rates for
     the date range covered by sample data (Sep–Oct 2025).

What this script does NOT do:
  • Load Partner A.csv / Partner B.csv → those go through the real
    POST /load-data Application Programming Interface endpoint, NOT
    a bypass script.

Run:
    cd "Apple SDE"
    python3 seed_bootstrap.py
"""

from __future__ import annotations

import csv
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from harmonise.normaliser import normalise
from harmonise.extractor import extract


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

DB_CONFIG = dict(
    dbname=os.getenv("PGDATABASE", "maya_assignment"),
    user=os.getenv("PGUSER", os.environ["USER"]),
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", 5432)),
)

REF_CSV = Path(__file__).resolve().parent / "Product Ref.csv"


# ---------------------------------------------------------------------------
# Step 1: dim_product_model + dim_product_sku from Product Ref.csv
# ---------------------------------------------------------------------------

# Color code → human-readable name (extracted from Product Ref long descriptions)
COLOR_CODE_MAP = {
    "PNK": "Pink", "PUR": "Purple", "STL": "Starlight", "BLK": "Black",
    "WHT": "White", "YLW": "Yellow", "BLU": "Blue", "RED": "Red",
    "SLV": "Silver", "GRY": "Grey", "GRN": "Green", "ORG": "Orange",
    "GLD": "Gold", "MDN": "Midnight", "CORANGE": "Cosmic Orange",
    "DEEPBLUE": "Deep Blue", "SKYBLUE": "Sky Blue",
}

CATEGORY_NAME_TO_CODE = {
    "iPhone":  "IPHONE",
    "iPad":    "IPAD",
    "AirPods": "AIRPODS",
    "Mac":     "MAC",
    "MacBook": "MAC",
    "Watch":   "WATCH",
}


def _extract_color(long_desc: str) -> Optional[str]:
    """Pick out a color from a long description like 'IPHONE 17 PRO MAX SILVER 512GB'."""
    if not long_desc:
        return None
    tokens = long_desc.upper().split()
    for tok in tokens:
        if tok in COLOR_CODE_MAP:
            return COLOR_CODE_MAP[tok]
    return None


def seed_products(conn) -> Tuple[int, int]:
    """
    Insert dim_product_model and dim_product_sku from Product Ref.csv.

    Returns: (model_count, sku_count)
    """
    sig_to_model_id: dict[str, int] = {}
    sku_inserts = []
    model_inserts = []

    # Pre-load category map so we can resolve category_id quickly
    with conn.cursor() as cur:
        cur.execute("SELECT category_code, category_id FROM dim_product_category")
        category_lookup = {code: cid for code, cid in cur.fetchall()}

    with REF_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat_name = (row.get("Product Category") or "").strip()
            short    = (row.get("Product Short Description") or "").strip()
            long_    = (row.get("Product Long Description") or "").strip()
            ref_id   = row.get("Product ID")
            if not cat_name or not short or not ref_id:
                continue

            tokens = normalise(f"{cat_name} {short} {long_}")
            attrs  = extract(tokens)
            sig    = attrs.signature()

            cat_code = CATEGORY_NAME_TO_CODE.get(cat_name)
            if not cat_code or cat_code not in category_lookup:
                continue
            category_id = category_lookup[cat_code]

            # Stage model inserts
            if sig not in sig_to_model_id:
                sig_to_model_id[sig] = None  # placeholder, filled after batch insert
                model_inserts.append((
                    sig,
                    category_id,
                    attrs.model_line,
                    attrs.chip,
                    attrs.storage_gb,
                    attrs.connectivity,
                ))

            # Stage sku inserts (model_id resolved after model insert below)
            sku_inserts.append((
                sig,                                    # placeholder, replaced
                int(ref_id),
                _extract_color(long_),
                short,
                long_,
            ))

    # Batch insert models (idempotent — skip if already there)
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_product_model
              (model_key, category_id, model_line, chip, storage_gb, connectivity)
            VALUES %s
            ON CONFLICT (model_key) DO NOTHING
            """,
            model_inserts,
        )

        # Re-fetch ALL model_keys → ids (covers both freshly inserted and
        # pre-existing rows; avoids the execute_values+RETURNING quirk)
        cur.execute("SELECT model_key, product_model_id FROM dim_product_model")
        for model_key, product_model_id in cur.fetchall():
            sig_to_model_id[model_key] = product_model_id

    # Resolve model_id for skus, then bulk insert
    sku_rows = []
    for sig, ref_id, color, short, long_ in sku_inserts:
        model_id = sig_to_model_id.get(sig)
        if model_id is None:
            print(f"  ⚠ skipping sku ref_id={ref_id}: sig {sig!r} not found in dim_product_model")
            continue
        sku_rows.append((model_id, ref_id, color, short, long_))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_product_sku
              (product_model_id, ref_product_id, color, short_desc, long_desc)
            VALUES %s
            """,
            sku_rows,
        )

    conn.commit()
    return len(model_inserts), len(sku_rows)


# ---------------------------------------------------------------------------
# Step 2: dim_currency_rate_snapshot — Foreign Exchange seed
# ---------------------------------------------------------------------------

# Minimal seed: simulates an external Foreign Exchange feed.
# Production path would call OpenExchangeRates / European Central Bank API.
BASE_RATES_TO_USD = {
    "AUD": 0.65,
    "NZD": 0.60,
    "USD": 1.00,
    "GBP": 1.25,
    "EUR": 1.08,
}


def seed_fx_rates(conn,
                  start: date = date(2025, 9, 1),
                  end:   date = date(2026, 2, 28)) -> int:
    """
    Insert daily Foreign Exchange rates between start and end.
    Constant rate is fine for the demo; production would vary daily.
    """
    rows = []
    d = start
    while d <= end:
        for from_cur, rate in BASE_RATES_TO_USD.items():
            rows.append((from_cur, "USD", rate, d, "seed_bootstrap"))
        d += timedelta(days=1)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_currency_rate_snapshot
              (from_currency_code, to_currency_code, rate, effective_date, source)
            VALUES %s
            ON CONFLICT (from_currency_code, to_currency_code, effective_date)
            DO NOTHING
            """,
            rows,
        )
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Connecting to {DB_CONFIG['dbname']} as {DB_CONFIG['user']}...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        print("\nStep 1: Seeding dim_product_model + dim_product_sku from Product Ref.csv ...")
        model_count, sku_count = seed_products(conn)
        print(f"  → inserted {model_count} models, {sku_count} skus")

        print("\nStep 2: Seeding dim_currency_rate_snapshot (Foreign Exchange rates) ...")
        fx_count = seed_fx_rates(conn)
        print(f"  → inserted up to {fx_count} rate rows (existing rows skipped)")

        print("\nDone. Verifying:")
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dim_product_model")
            print(f"  dim_product_model            : {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM dim_product_sku")
            print(f"  dim_product_sku              : {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM dim_currency_rate_snapshot")
            print(f"  dim_currency_rate_snapshot   : {cur.fetchone()[0]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
