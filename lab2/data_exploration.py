from datasets import load_dataset
from pathlib import Path
import requests
import pandas as pd
import json
import pdfplumber
import regex as re
import io


def get_csv(dataset_name):
    dataset = load_dataset(dataset_name)

    # Access a specific split
    train_data = dataset["train"]
    print(train_data.features)

    # Convert a small portion to pandas for quick inspection
    train_pd = train_data.to_pandas()
    print('First 5 rows of the CSV dataset:')
    print(train_pd.head())
    print('Total number of rows:', train_pd.shape[0])


# ASCII Texts like Forum Postings and HTML
def get_html(POST_URL):
    JSON_URL = POST_URL.rstrip("/") + "/.json"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"}

    resp = requests.get(JSON_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # ---- Post (link/self) ----
    post_data = data[0]["data"]["children"][0]["data"]
    post = {
        "id": post_data["id"],
        "subreddit": post_data.get("subreddit"),
        "title": post_data.get("title"),
        "author": post_data.get("author"),
        "selftext": post_data.get("selftext"),
        "score": post_data.get("score"),
        "upvote_ratio": post_data.get("upvote_ratio"),
        "num_comments": post_data.get("num_comments"),
        "created_utc": post_data.get("created_utc"),
        "permalink": "https://www.reddit.com" + post_data.get("permalink", ""),
        "url": post_data.get("url"),
    }
    pd.DataFrame([post]).to_csv("reddit_post.csv", index=False)
    with open("reddit_post.json", "w", encoding="utf-8") as f:
        json.dump(post, f, ensure_ascii=False, indent=2)

    # ---- Comments (recursive) ----
    comments = []

    def parse_children(children, parent_id):
        for ch in children:
            kind = ch.get("kind")
            d = ch.get("data", {})
            if kind == "t1":  # comment
                comments.append({
                    "post_id": post["id"],
                    "comment_id": d.get("id"),
                    "parent_id": parent_id,
                    "author": d.get("author"),
                    "body": d.get("body"),
                    "score": d.get("score"),
                    "created_utc": d.get("created_utc"),
                    "depth": d.get("depth", 0),
                    "permalink": "https://www.reddit.com" + d.get("permalink",""),
                })
                # get replies
                replies = d.get("replies")
                if isinstance(replies, dict):
                    parse_children(replies.get("data", {}).get("children", []), d.get("id"))

    parse_children(data[1]["data"]["children"], post["id"])

    pd.DataFrame(comments).to_csv("reddit_comments.csv", index=False)
    with open("reddit_comments.json", "w", encoding="utf-8") as f:
        json.dump(comments, f, ensure_ascii=False, indent=2)



# PDF and Word Documents that require conversion and OCR
"""
Parse UNESCO World Heritage list PDF (list00-eng.pdf) 
to extract: country, site_name, type, year
and save as CSV.
"""
# ----------- Utility functions -----------
def squash_repeats(s: str) -> str:
    """Compress OCR artifacts like 'Wwwwooorrrllddd' -> 'World' and clean spaces."""
    s = re.sub(r'([A-Za-z])\1{2,}', r'\1', s)
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()

HEADER_PATTERNS = [
    r"world\s+heritage\s+centre",
    r"convention",
    r"name\s+of\s+property",
    r"criteria\s+for\s+the\s+inclusion",
    r"notes?:",
    r"sessions\s+of\s+the\s+world\s+heritage\s+committee",
    r"n:\s*natural\s+property",
    r"c:\s*cultural\s+property",
    r"contracting\s+state\s+party",
    r"id\.?\s*no\.",
    r"year\s+of\s+inscription",
]

def is_header_footer(line: str) -> bool:
    """Check if a line is header/footer text that should be skipped."""
    return any(re.search(p, line.lower()) for p in HEADER_PATTERNS)

# Country detection
RE_COUNTRY = re.compile(r"^[A-Z][A-Z \-’'&/.]+$")
BAD_TOKENS_IN_COUNTRY = {"HERITAGE", "CONVENTION", "CENTRE", "PROPERTY", "NOTES", "CRITERIA", "SESSION", "ID", "YEAR"}

def is_country_line(line: str) -> bool:
    """Return True if a line looks like a valid country name."""
    if not RE_COUNTRY.match(line):
        return False
    return not any(tok in line for tok in BAD_TOKENS_IN_COUNTRY)

# Regex for ID/Year and criteria
RE_ID_YEAR = re.compile(r"(?P<id>\d+)\s+(?P<year>(?:rev\s*)?(?:\d{4}(?:-\d{4})*(?:\s*\(Note.*?\))?))", re.I)
RE_TYPE_BLOCK = re.compile(r"(?:(?P<t>[CN])\s*\((?P<crit>[ivx]+(?:\)\(|\)))+)", re.I)
RE_CRITS = re.compile(r"\(([ivx]+)\)", re.I)

def norm_criteria(block: str):
    """Parse type/criteria block into dict."""
    out = {"C": [], "N": []}
    for m in RE_TYPE_BLOCK.finditer(block or ""):
        t = m.group("t").upper()
        crits = RE_CRITS.findall(m.group(0))
        out[t].extend(c.lower() for c in crits)
    return out

def parse_year(year_raw: str) -> str:
    """Extract the earliest 4-digit year from a raw year string."""
    years = re.findall(r"\b((?:19|20)\d{2})\b", year_raw)
    if not years:
        return ""
    return min(years)


def get_pdf(pdf_url: str, out_csv: str):
    """
    Extract UNESCO World Heritage data (country, site_name, type, year) 
    from a PDF at a URL and save to a CSV file.
    """
    # Download PDF into memory
    resp = requests.get(pdf_url)
    resp.raise_for_status()
    pdf_file = io.BytesIO(resp.content)

    rows = []
    with pdfplumber.open(pdf_file) as pdf:
        current_country = ""
        for page in pdf.pages:
            # Split each page into two columns
            x0, y0, x1, y1 = page.bbox
            mid = (x0 + x1) / 2
            regions = [(x0, y0, mid, y1), (mid, y0, x1, y1)]
            for (rx0, ry0, rx1, ry1) in regions:
                crop = page.within_bbox((rx0, ry0, rx1, ry1))
                text = crop.extract_text(x_tolerance=2, y_tolerance=2) or ""
                lines = [squash_repeats(ln) for ln in text.splitlines()]
                lines = [ln for ln in lines if ln and not is_header_footer(ln)]

                i = 0
                while i < len(lines):
                    ln = lines[i]

                    if is_country_line(ln):
                        current_country = ln.title()
                        i += 1
                        continue

                    if "C (" in ln or "N (" in ln or ln.isupper():
                        i += 1
                        continue

                    name = ln.strip()
                    look = "\n".join(lines[i+1:i+6])
                    m_id = RE_ID_YEAR.search(look)
                    if m_id:
                        year = parse_year(m_id.group("year").strip())
                        tail = look + "\n" + "\n".join(lines[i+6:i+10])
                        crits = norm_criteria(tail)
                        if crits["C"] and crits["N"]:
                            typ = "Mixed"
                        elif crits["C"]:
                            typ = "Cultural"
                        elif crits["N"]:
                            typ = "Natural"
                        else:
                            typ = ""

                        site_name = re.sub(r"\b\d{4}\b", "", name)
                        site_name = re.sub(r"\b\d+\b", "", site_name)
                        site_name = site_name.strip(" ,;.-")

                        if re.search(r"[A-Za-z].*[A-Za-z]", site_name) and not re.search(r"[CN]\s*\([ivx]", site_name):
                            row = {
                                "country": current_country,
                                "site_name": site_name,
                                "type": typ,
                                "year": year,
                            }
                            if row["country"] and row["site_name"] and row["year"] and row["type"] in ("Cultural","Natural","Mixed"):
                                rows.append(row)

                        i += 4
                        continue
                    i += 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["country", "site_name"])
    df = df[["country", "site_name", "type", "year"]]
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} clean rows to {Path(out_csv).resolve()}")


if __name__ == "__main__":
    get_csv("pfb30/multi_woz_v22")
    get_html("https://www.reddit.com/r/travel/comments/1mc0b3n/los_angeles_advice_needed/")
    get_pdf("https://whc.unesco.org/archive/list00-eng.pdf", "unesco_world_heritage_clean.csv")