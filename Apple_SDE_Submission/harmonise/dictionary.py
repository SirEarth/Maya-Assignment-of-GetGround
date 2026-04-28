"""
Abbreviation dictionaries, noise words, and pattern rules.

Three-layer strategy:
- Layer 1 (this file): manual domain knowledge — abbreviations that can't be
  auto-mined from data (iP, PM, WF/CL).
- Layer 2 (future): data-mined candidates from Product Ref Long/Short Description
  alignment.
- Layer 3 (runtime): low-confidence matches land in dq_bad_records; business
  users review and promote tokens here.
"""

# ---------------------------------------------------------------------------
# TOKEN-LEVEL abbreviation expansion (replaces whole tokens only, not substrings)
# ---------------------------------------------------------------------------
TOKEN_ABBREVIATIONS = {
    "ip":      "iphone",
    "pm":      "pro max",
    "pr":      "pro",
    "wf":      "wifi",
    "cl":      "cellular",
    "cell":    "cellular",
    "wi-fi":   "wifi",
    # Common partner B verbosity
    "gen":     "generation",
}

# ---------------------------------------------------------------------------
# REGEX-level expansions (more powerful than token replacement — handle context)
# Run BEFORE tokenization so they can see complete words.
# ---------------------------------------------------------------------------
import re

PATTERN_EXPANSIONS = [
    # "iP15PM" / "iP15P" / "iP15" — fused partner A abbreviations
    (re.compile(r"\bip(\d{1,2})pm\b",    re.I), r"iphone \1 pro max"),
    (re.compile(r"\bip(\d{1,2})p\b",     re.I), r"iphone \1 pro"),
    (re.compile(r"\bip(\d{1,2})\b",      re.I), r"iphone \1"),
    # "iPhone Air" is a real model, no change needed
    # Storage: "1TB" / "2TB" → normalize to GB for comparison
    (re.compile(r"(\d+)\s*tb\b",         re.I), lambda m: f"{int(m.group(1)) * 1024}gb"),
    # Insert space around GB for cleaner tokenization: "512GB" → "512 gb"
    (re.compile(r"(\d+)\s*gb\b",         re.I), r"\1 gb"),
    # iPad size shorthand: "11in" / "11-inch" / '11"' → "11inch"
    (re.compile(r"(\d{1,2}(?:\.\d)?)\s*(?:in|inch|-inch)\b", re.I), r"\1inch"),
    (re.compile(r"(\d{1,2}(?:\.\d)?)\s*\"",                  re.I), r"\1inch"),
    # ── AirPods / audio: collapse "active noise cancellation" → "anc"
    # Critical: keep this BEFORE noise-word stripping so the discriminating
    # ANC vs non-ANC signal survives (Reference has both `4th_generation_anc`
    # and `4th_generation_non-anc` variants).
    #
    # NOTE: do NOT add an ordinal-stripping pattern (e.g. "4th" → "4"). Ordinals
    # are part of the seeded `dim_product_model.model_key` (e.g. `airpods|
    # 4th_generation_anc_4`). Stripping them at match time would produce a
    # different signature than the registry, breaking the model_id lookup in
    # services.py:harmonise_in_stg. The existing seed contract is intentional.
    (re.compile(r"\bactive\s+noise\s+cancellation\b", re.I), r"anc"),
    (re.compile(r"\bactive\s+noise\s+cancelling\b",   re.I), r"anc"),
    (re.compile(r"\bnoise\s+cancellation\b",          re.I), r"anc"),
    (re.compile(r"\bnoise\s+cancelling\b",            re.I), r"anc"),
    (re.compile(r"\bnon[- ]?anc\b",                   re.I), r"non-anc"),
]

# ---------------------------------------------------------------------------
# NOISE WORDS — tokens dropped from both query and ref names
# They dilute matching signal without adding discrimination.
# ---------------------------------------------------------------------------
NOISE_WORDS = {
    "apple",
    "storage",
    "chip",
    "display",
    "liquid",
    "retina",
    "usb-c",
    "usbc",
    "neural",
    "engine",
    "intelligence",
    "with",
    "and",
    "the",
    "standard",
    "glass",
    "bionic",
    # Colors (not used for matching at model level — SKU-level info only)
    "starlight", "midnight", "blue", "purple", "white", "black", "silver",
    "space", "grey", "gray", "graphite", "pink", "red", "yellow",
    "ultramarine", "teal", "cloud", "titanium", "deep", "natural",
    "corange", "deserttitanium",
    # Color abbreviations (in long descriptions)
    "pnk", "pur", "stl", "blk", "ylw",
    # Descriptor words
    "brand", "new", "edition",
    # ── Audio marketing fluff (AirPods variants ship with verbose names that
    # add no model-discrimination signal — they just dilute Jaccard / fuzz
    # signals). The ANC / non-ANC distinction is preserved separately via
    # PATTERN_EXPANSIONS above; everything else here is safe to drop.
    "wireless", "headphones", "earphones", "headphone", "earphone",
    "in-ear", "true",
    "magsafe", "lightning", "case", "charging",
    "adaptive",
    "battery", "life", "hours",
    "hearing", "aid", "test", "clinical-grade",
    "ios",
}

# ---------------------------------------------------------------------------
# CATEGORY detection — first matching keyword wins
# ---------------------------------------------------------------------------
CATEGORY_KEYWORDS = [
    # Multi-word first to avoid "macbook" matching "mac" prematurely
    ("airpods",  "AIRPODS"),
    ("macbook",  "MAC"),
    ("imac",     "MAC"),
    ("ipad",     "IPAD"),
    ("iphone",   "IPHONE"),
    ("watch",    "WATCH"),
    ("mac",      "MAC"),
]

# ---------------------------------------------------------------------------
# CHIP patterns — detected by regex on normalized text
# Returns a canonical form (matching dim_product_model.chip values).
# ---------------------------------------------------------------------------
CHIP_PATTERNS = [
    (re.compile(r"\ba17\s*pro\b",   re.I), "A17 Pro"),
    (re.compile(r"\ba(\d{2})\b",    re.I), lambda m: f"A{m.group(1)}"),
    (re.compile(r"\bm(\d)\b",       re.I), lambda m: f"M{m.group(1)}"),
]

# ---------------------------------------------------------------------------
# CONNECTIVITY — cellular takes priority over wifi if both present
# ---------------------------------------------------------------------------
CONNECTIVITY_KEYWORDS = {
    "cellular": "Cellular",
    "wifi":     "WiFi",
}

# ---------------------------------------------------------------------------
# IPAD / MAC screen size (part of model_line for these categories)
# ---------------------------------------------------------------------------
SIZE_PATTERN = re.compile(r"(\d{1,2}(?:\.\d)?)\s*inch", re.I)

# ---------------------------------------------------------------------------
# STORAGE (GB)
# ---------------------------------------------------------------------------
STORAGE_PATTERN = re.compile(r"(\d+)\s*gb\b", re.I)
