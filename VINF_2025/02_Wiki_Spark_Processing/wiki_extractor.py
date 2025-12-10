#!/usr/bin/env python3
"""
Wikipedia Landmark Extractor (enhanced + robust infobox parser + accent normalization)
--------------------------------------------------------------------------------------
Streams through Wikipedia XML dump and extracts landmarks, museums,
parks, bridges, and cities for cross-linking with external datasets
(e.g. GetYourGuide).

Fixes:
- Multi-line infobox fields
- Templates like {{Start date|...}}
- Missing or malformed 'location' / 'built' values
- Non-ASCII characters normalized for matching (e.g., Královka -> kralovka)
"""

import re
import json
import unicodedata
from lxml import etree
from pathlib import Path
from collections import Counter

INPUT_FILE = "enwiki-latest-pages-articles22.xml-p44496246p44788941"
OUTPUT_FILE = "wiki_landmarks_enhanced_fixed.jsonl"
WIKI_BASE = "https://en.wikipedia.org/wiki/"

# === Infobox detection ===
INFOBOX_START_RE = re.compile(
    r'\{\{\s*Infobox\s+(building|landmark|museum|monument|park|bridge|'
    r'tourist\s*attraction|church|temple|castle|city|square|cathedral|'
    r'settlement|place|district|country|river|island|mountain|forest|gallery'
    r'geography|hotel|stadium|arena|theatre|airport|railway|station|library'
    r'university|college|market|zoo|aquarium|cemetery|palace|building_complex'
    r'mosque|synagogue|shrine|monastery|basilica'
    r'lake|waterfall|cave|valley|volcano|bay|peninsula|desert|beach|cliff|national_park'
    r'heritage_site|archaeological_site|exhibit|memorial|artwork|sculpture|museum_ship|observatory'
    r'tunnel|dam|lighthouse|port|harbor|harbour|road|highway|viaduct'
    r'fort|fortress|bunker|citadel|barracks|watchtower|gatehouse|manor|mansion|villa|chateau|hall|residence'
    r'tourist_attraction|theme_park|amusement_park|botanical_garden|structure)',
    re.I
)

CATEGORY_RE = re.compile(r'\[\[Category:([^\]|]+)', re.I)
REDIRECT_RE = re.compile(r'#REDIRECT\s*\[\[([^\]]+)\]\]', re.I)

COMMENT_RE = re.compile(r'<!--.*?-->', re.S)
REF_TAG_RE = re.compile(r'<ref\b[^>]*>.*?</ref>|<ref\b[^>]*/>', re.S | re.I)
WIKILINK_RE = re.compile(r'\[\[([^\]|]+)\|([^\]]+)\]\]|\[\[([^\]]+)\]\]')
TEMPLATE_RE = re.compile(r'\{\{[^{}]*\}\}')

CITY_IN_ABSTRACT_RE = re.compile(r'\b in ([A-Z][A-Za-z.\- ]{2,40})(?:, ([A-Z][A-Za-z.\- ]{2,40}))?\b')

# === Utility helpers ===


def strip_comments_and_refs(s: str) -> str:
    s = COMMENT_RE.sub('', s)
    s = REF_TAG_RE.sub('', s)
    return s


def clean_markup(s: str) -> str:
    """Simplify wikilinks, templates, and tags."""
    if not s:
        return ""

    # 1. HTML decoding
    import html
    s = html.unescape(s)

    # 2. Remove comments
    s = re.sub(r'<!--.*?-->', '', s, flags=re.DOTALL)

    # 3. Remove bold/italic (''' and '')
    s = re.sub(r"''+", "", s)

    # 4. Remove templates {{...}} - Iterative to handle nesting
    # Remove anything starting with {{ and ending with }}
    for _ in range(5):
        s = re.sub(r'\{\{[^{}]*\}\}', '', s)

    # 5. Remove tables {| ... |}
    s = re.sub(r'\{\|.*?\|\}', '', s, flags=re.DOTALL)

    # 6. Simplify Wikilinks [[Target|Label]] -> Label
    s = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', s)

    # 7. Remove external links [url label] -> label
    s = re.sub(r'\[http[^\s]+ ([^\]]+)\]', r'\1', s)
    s = re.sub(r'\[http[^\s]+\]', '', s)

    # 8. Remove headings == ... ==
    s = re.sub(r'=+[^=]+=+', '', s)

    # 9. Remove file/image links
    s = re.sub(r'\[\[(File|Image):.*?\]\]', '', s, flags=re.IGNORECASE)

    # 10. Cleanup whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def normalize_start_date_templates(s: str) -> str:
    """Convert {{Start date|YYYY|MM|DD}} → YYYY-MM-DD."""

    def _sub(m):
        inside = m.group(0)[2:-2]
        parts = [p.strip() for p in inside.split('|')]
        toks = [p for p in parts[1:] if p.isdigit()]
        if len(toks) >= 3 and len(toks[0]) == 4:
            return f'{toks[0]}-{toks[1].zfill(2)}-{toks[2].zfill(2)}'
        return m.group(0)

    return re.sub(r'\{\{\s*start\s*date[^}]*\}\}', _sub, s, flags=re.I)


# === Accent normalization ===
def _strip_accents(text: str) -> str:
    """Convert accented characters to plain ASCII equivalents."""
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


def normalize_title(title: str) -> str:
    """Normalize title for matching (lowercase, accents removed)."""
    title = _strip_accents(title)
    return re.sub(r'[^a-z0-9 ]', '', title.lower().replace("_", " ")).strip()


def normalize_tokens(text: str):
    """Convert text into lowercase tokens (with accent stripping)."""
    text = _strip_accents(text)
    return [t for t in re.findall(r"[a-z]{2,}", text.lower())]


# === Balanced infobox parsing ===
def extract_balanced_infobox(text: str) -> str | None:
    m = INFOBOX_START_RE.search(text)
    if not m:
        return None
    i = m.start()
    depth = 0
    j = i
    while j < len(text):
        if text[j:j + 2] == '{{':
            depth += 1
            j += 2
            continue
        if text[j:j + 2] == '}}':
            depth -= 1
            j += 2
            if depth == 0:
                return text[i:j]
            continue
        j += 1
    return None


def parse_infobox_fields(infobox: str) -> dict:
    """Parse lines inside a balanced Infobox with multi-line support."""
    s = strip_comments_and_refs(infobox)
    lines = s.splitlines()

    if lines and lines[0].lstrip().startswith('{{'):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith('}}'):
        lines = lines[:-1]

    fields = {}
    cur_key = None
    cur_val_lines = []

    def _flush():
        nonlocal cur_key, cur_val_lines
        if cur_key:
            raw = '\n'.join(cur_val_lines).strip()
            raw = normalize_start_date_templates(raw)
            for _ in range(3):
                raw2 = TEMPLATE_RE.sub('', raw)
                if raw2 == raw:
                    break
                raw = raw2
            raw = clean_markup(raw)
            fields[cur_key] = raw
        cur_key, cur_val_lines = None, []

    for ln in lines:
        if ln.lstrip().startswith('|'):
            _flush()
            body = ln.lstrip()[1:].strip()
            if '=' in body:
                k, v = body.split('=', 1)
                cur_key = k.strip().lower()
                cur_val_lines = [v.strip()]
        elif ln.strip().startswith('}}'):
            break
        else:
            if cur_key is not None:
                cur_val_lines.append(ln.rstrip())
    _flush()
    return fields


def extract_infobox_fields(text: str) -> dict:
    box = extract_balanced_infobox(text)
    if not box:
        return {}
    fields = parse_infobox_fields(box)
    built = fields.get('built') or fields.get('opened') or fields.get('established') or fields.get('completed') or fields.get('founded')
    location = fields.get('location')
    country = fields.get('country')
    coords = fields.get('coordinates') or fields.get('coord')
    architect = fields.get('architect') or fields.get('developer') or fields.get('designer')
    style = fields.get('style') or fields.get('architectural_style')
    height = fields.get('height')

    out = {}
    if location: out['location'] = location
    if country: out['country'] = country
    if coords: out['coordinates'] = coords
    if built: out['built'] = built
    if architect: out['architect'] = architect
    if style: out['style'] = style
    if height: out['height'] = height
    return out


# === Other helpers ===
def extract_categories(text: str):
    return [c.strip() for c in CATEGORY_RE.findall(text)]


def infer_type_from_categories(categories):
    joined = " ".join(c.lower() for c in categories)
    if "museum" in joined:
        return "museum"
    if "park" in joined or "garden" in joined:
        return "park"
    if "bridge" in joined:
        return "bridge"
    if "monument" in joined:
        return "monument"
    if "river" in joined:
        return "river"
    if "city" in joined or "town" in joined or "settlement" in joined:
        return "city"
    if "castle" in joined or "building" in joined or "tower" in joined or "church" in joined:
        return "landmark"
    return "place"


def infer_location_from_categories(categories):
    out = []
    for c in categories:
        m = re.search(r'\b(in|of)\s+([A-Z][A-Za-z.\- ]{2,40})', c)
        if m:
            out.append(m.group(2).strip())
    return [x for i, x in enumerate(out) if x and x not in out[:i]][:3]


def infer_location_from_abstract(abstract: str) -> str | None:
    if not abstract:
        return None
    m = CITY_IN_ABSTRACT_RE.search(abstract)
    if not m:
        return None
    if m.group(2):
        return f"{m.group(1)}, {m.group(2)}"
    return m.group(1)


def derive_geo_parent(location_field, categories):
    parents = []
    if location_field:
        parts = [p.strip() for p in re.split(r'[,;/]', location_field)]
        parents.extend(parts[:3])
    for c in categories:
        c_lower = c.lower()
        if "in " in c_lower:
            name = c.split("in ")[-1]
            parents.append(name.strip())
        elif "of " in c_lower and not c_lower.startswith("list"):
            possible = c.split("of ")[-1].strip()
            if len(possible) < 50:
                parents.append(possible)
    return list({p for p in parents if p and len(p) < 50})


def extract_abstract(text: str):
    for para in text.split("\n\n"):
        if not para.startswith("{{") and not para.startswith("|"):
            cleaned = clean_markup(para)
            if len(cleaned) > 50:
                return cleaned
    return None


# === Core page extraction ===
def extract_page(title, text):
    if REDIRECT_RE.search(text):
        return None

    categories = extract_categories(text)
    if not categories:
        return None

    fields = extract_infobox_fields(text)
    # Relaxed: Even if fields are empty, we might want the page if it has an abstract/categories
    # if not fields:
    #    return None

    abstract = extract_abstract(text)
    # add location fallback if missing
    if not fields.get('location'):
        cats_loc = infer_location_from_categories(categories)
        if cats_loc:
            fields['location'] = ', '.join(cats_loc)
        else:
            loc2 = infer_location_from_abstract(abstract)
            if loc2:
                fields['location'] = loc2

    geo_parent = derive_geo_parent(fields.get("location"), categories)
    normalized = normalize_title(title)
    tokens = normalize_tokens(title)
    geo_parent_tokens = [normalize_tokens(p) for p in geo_parent]

    return {
        "title": title,
        "normalized": normalized,
        "tokens": tokens,
        "url": WIKI_BASE + title.replace(" ", "_"),
        "categories": categories,
        "abstract": abstract,
        "type": infer_type_from_categories(categories),
        "fields": fields,
        "geo_parent": geo_parent,
        "geo_parent_tokens": geo_parent_tokens,
    }

# === Infobox statistics collection ===
def collect_infobox_stats(text: str, stats: Counter):
    """
    Collects the raw infobox type name (e.g. 'Infobox building', 'Infobox river')
    from a Wikipedia article's text for later analysis.

    Args:
        text (str): Raw wikitext of the page.
        stats (Counter): A Counter() object to accumulate occurrences.

    Example:
        stats = Counter()
        collect_infobox_stats(page_text, stats)
    """
    m = re.search(r'\{\{\s*Infobox\s*([^\n|{]+)', text, re.I)
    if m:
        infotype = m.group(1).strip().lower()
        # Keep only first word(s) until first special char
        infotype = re.split(r'[\s\|{<]', infotype)[0]
        stats[infotype] += 1



# === Main streaming loop ===
def main():
    out_path = Path(OUTPUT_FILE)
    out_path.parent.mkdir(exist_ok=True, parents=True)

    total = 0
    matched = 0
    infobox_stats = Counter()  # initialize counter

    with open(INPUT_FILE, "rb") as f, open(out_path, "w", encoding="utf-8") as out:
        for _, elem in etree.iterparse(f, events=("end",), tag="{*}page"):
            title = elem.findtext(".//{*}title")
            text = elem.findtext(".//{*}revision/{*}text")
            total += 1

            if not title or not text:
                elem.clear()
                continue

            # 👇 collect infobox statistics (regardless of filtering)
            collect_infobox_stats(text, infobox_stats)

            record = extract_page(title, text)
            if record:
                matched += 1
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                if matched <= 3:
                    print(json.dumps(record, indent=2, ensure_ascii=False))

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if total % 1000 == 0:
                print(f"Processed {total:,} pages, found {matched:,} landmarks")

    # Print top infobox types after processing
    print("\n=== Top Infobox Types Found ===")
    for name, count in infobox_stats.most_common(50):
        print(f"{name:40s} {count}")
    print(f"Total distinct infobox types: {len(infobox_stats)}")

    print(f"\nDone. {matched:,} relevant records saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
