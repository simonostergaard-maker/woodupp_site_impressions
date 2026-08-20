"""
Converts a Shopify "Collections and Translations" export (from the
Translate & Adapt app's Google Sheets export, or an equivalent CSV) into
the compact data/collection_url_map.json used by preprocess.py to group
collection URLs into cross-market "business areas" in the dashboard.

The raw export has one row per (collection, locale, field) — this script
keeps only the "handle" rows, since that's what determines the URL path,
and builds: canonical (default/English) handle -> per-market localized
handle.

Run this again whenever collections change meaningfully (new collections,
renamed handles, new markets). For a one-off addition of a single new
collection, it's usually faster to hand-edit collection_url_map.json
directly — see its "collections" object; each entry is
    "<canonical-handle>": {"title": "...", "markets": {"<market>": {"default": "<handle>"[, "fr": "<handle>"]}}}

Usage:
    python build_collection_map.py path/to/export.csv
"""
import csv
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_PATH = SCRIPT_DIR / "data" / "collection_url_map.json"

# Locale -> GSC market code(s) this dashboard already knows about (see
# DOMAIN_MARKET_MAP in preprocess.py). Confirmed with the site owner:
#   - Austria (at) and the German-language Switzerland storefront (ch)
#     reuse the German ("de") translations.
#   - Belgium (be) is primarily Dutch ("nl") with a French subpath.
#   - Switzerland (ch) is primarily German with a French subpath.
#   - Norway ("no" vs "nb" in the export) is unresolved — both locales
#     are present with different handles and it's unclear which is live,
#     so Norway is intentionally left unmapped rather than guessed.
# Markets not listed here (com, eu, couk, us, au, ae, coza, com_na) use
# the untranslated default/English handle directly.
LOCALE_TO_MARKETS = {
    "da": ["dk"],
    "de": ["de", "at"],
    "es": ["es"],
    "fr": ["fr"],
    "it": ["it"],
    "nl": ["nl"],
    "pl": ["pl"],
    "pt-PT": ["pt"],
    "sv": ["se"],
}

# Markets whose storefront runs the given locale as a /<locale>/ subpath
# alongside a different default-language storefront at the market root.
SUBPATH_LOCALE_MARKETS = {
    "be": ("fr", "nl"),  # (subpath locale, default locale)
    "ch": ("fr", "de"),  # (subpath locale, default locale)
}

DEFAULT_HANDLE_MARKETS = ["com", "eu", "couk", "us", "au", "ae", "coza", "com_na"]


def build_map(csv_path):
    with open(csv_path, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))

    handle_rows = [r for r in all_rows if r["Type"] == "COLLECTION" and r["Field"] == "handle"]
    title_rows = [r for r in all_rows if r["Type"] == "COLLECTION" and r["Field"] == "title"]
    titles = {r["Parent Handle"]: r["Default content"].strip() for r in title_rows if r["Default content"].strip()}

    by_collection = {}
    for r in handle_rows:
        handle = r["Parent Handle"]
        locale = r["Locale"]
        translated = r["Translated content"].strip()
        by_collection.setdefault(handle, {})[locale] = translated
        titles.setdefault(handle, handle)

    collections = {}
    for handle, locale_handles in by_collection.items():
        markets = {}
        for market in DEFAULT_HANDLE_MARKETS:
            markets[market] = {"default": handle}
        for locale, market_codes in LOCALE_TO_MARKETS.items():
            translated = locale_handles.get(locale)
            if not translated:
                continue
            for market in market_codes:
                markets[market] = {"default": translated}
        for market, (fr_locale, default_locale) in SUBPATH_LOCALE_MARKETS.items():
            entry = {}
            default_translated = locale_handles.get(default_locale)
            if default_translated:
                entry["default"] = default_translated
            fr_translated = locale_handles.get(fr_locale)
            if fr_translated:
                entry["fr"] = fr_translated
            if entry:
                markets[market] = entry
        collections[handle] = {"title": titles[handle], "markets": markets}

    return {
        "generated_from": Path(csv_path).name,
        "collections": collections,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python build_collection_map.py path/to/export.csv")
        sys.exit(1)
    csv_path = sys.argv[1]
    result = build_map(csv_path)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"Wrote {len(result['collections'])} collections to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
