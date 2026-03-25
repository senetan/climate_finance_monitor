#!/usr/bin/env python3
"""
Weekly Climate Finance Research Monitor
========================================
Pulls recent papers from RSS feeds and academic sources,
then uses Claude to produce a structured digest.

Output: Markdown file saved to /digests/ folder.
Can run locally or via GitHub Actions.
"""

import feedparser
import requests
import json
import os
import re
from datetime import datetime, timedelta
from anthropic import Anthropic

# ── Configuration ──────────────────────────────────────────────

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "10"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "digests")

# ── Sources: RSS feeds and search endpoints ────────────────────
# Each entry: (human name, URL, type)
# type = "rss" for standard RSS, "json_nber" for NBER API

FEEDS = [
    # --- Working paper series (pre-prints) ---
    ("NBER Working Papers", 
     "https://www.nber.org/api/v1/working_page_listing/contentType/working_paper/_/_/search?page=1&perPage=30&q=climate+finance&sortBy=public_date",
     "json_nber"),
    
    ("BIS Working Papers",
     "https://www.bis.org/doclist/wppubls.rss",
     "rss"),
    
    ("ECB Working Papers",
     "https://www.ecb.europa.eu/rss/wppub.html",
     "rss"),
    
    ("CEPR / VoxEU Columns",
     "https://cepr.org/rss/columns/feed",
     "rss"),
    
    # --- Top-tier peer-reviewed journals ---
    ("Nature Climate Change",
     "https://www.nature.com/nclimate.rss",
     "rss"),
    
    ("Journal of Environmental Economics & Management",
     "https://rss.sciencedirect.com/publication/science/00950696",
     "rss"),
    
    ("Journal of Climate Finance",
     "https://rss.sciencedirect.com/publication/science/27724875",
     "rss"),
    
    ("Journal of Sustainable Finance & Investment",
     "https://www.tandfonline.com/feed/rss/tsfi20",
     "rss"),

    ("Energy Economics",
     "https://rss.sciencedirect.com/publication/science/01onal40883",
     "rss"),
    
    ("Review of Financial Studies (Oxford)",
     "https://academic.oup.com/rss/site_5504/3365.xml",
     "rss"),

    ("Journal of Financial Economics",
     "https://rss.sciencedirect.com/publication/science/0304405X",
     "rss"),
]

# Keywords to filter for climate/transition finance relevance
CLIMATE_KEYWORDS = [
    "climate", "carbon", "emission", "transition", "green bond",
    "esg", "stranded asset", "physical risk", "net zero", "net-zero",
    "temperature rise", "decarboni", "fossil fuel", "renewable",
    "sustainable finance", "climate risk", "biodiversity", "nature-related",
    "tcfd", "issb", "scope 1", "scope 2", "scope 3", "paris-aligned",
    "sbti", "science based target", "carbon market", "carbon price",
    "green finance", "transition risk", "climate disclosure",
    "implied temperature", "climate value at risk", "climate var",
    "energy transition", "low carbon", "clean energy", "msci",
]


# ── Feed Parsing ───────────────────────────────────────────────

def is_climate_relevant(text: str) -> bool:
    """Check if text contains climate finance keywords."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in CLIMATE_KEYWORDS)


def parse_rss_feed(url: str, source_name: str, cutoff: datetime) -> list[dict]:
    """Parse a standard RSS feed and return recent climate-relevant entries."""
    try:
        feed = feedparser.parse(url)
        entries = []
        for entry in feed.entries[:60]:
            # Parse publication date
            published = entry.get("published_parsed") or entry.get("updated_parsed")
            pub_date = None
            if published:
                try:
                    pub_date = datetime(*published[:6])
                    if pub_date < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass
            
            title = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            link = entry.get("link", "").strip()
            authors = entry.get("author", "")
            
            # Filter for climate relevance
            combined_text = f"{title} {summary}"
            if not is_climate_relevant(combined_text):
                continue
            
            # Clean HTML from summary
            summary_clean = re.sub(r"<[^>]+>", " ", summary)
            summary_clean = re.sub(r"\s+", " ", summary_clean).strip()[:1500]
            
            entries.append({
                "source": source_name,
                "title": title,
                "authors": authors,
                "summary": summary_clean,
                "link": link,
                "date": pub_date.strftime("%Y-%m-%d") if pub_date else "recent",
            })
        return entries
    except Exception as e:
        print(f"  Warning: Error fetching {source_name}: {e}")
        return []


def parse_nber_api(url: str, source_name: str, cutoff: datetime) -> list[dict]:
    """Parse NBER JSON API response."""
    try:
        headers = {"Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        entries = []
        results = data.get("results", [])
        for item in results[:30]:
            title = item.get("title", "").strip()
            url_paper = item.get("url", "")
            if url_paper and not url_paper.startswith("http"):
                url_paper = f"https://www.nber.org{url_paper}"
            authors_list = item.get("authors", [])
            authors = ", ".join(a.get("name", "") for a in authors_list) if authors_list else ""
            synopsis = item.get("synopsis", "").strip()
            
            combined_text = f"{title} {synopsis}"
            if not is_climate_relevant(combined_text):
                continue
            
            entries.append({
                "source": source_name,
                "title": title,
                "authors": authors,
                "summary": synopsis[:1500],
                "link": url_paper,
                "date": item.get("public_date", "recent"),
            })
        return entries
    except Exception as e:
        print(f"  Warning: Error fetching {source_name}: {e}")
        return []


def collect_all_papers() -> list[dict]:
    """Gather papers from all configured sources."""
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    all_papers = []
    
    for source_name, url, feed_type in FEEDS:
        print(f"  Fetching {source_name}...")
        if feed_type == "rss":
            entries = parse_rss_feed(url, source_name, cutoff)
        elif feed_type == "json_nber":
            entries = parse_nber_api(url, source_name, cutoff)
        else:
            entries = []
        
        all_papers.extend(entries)
        print(f"    -> {len(entries)} climate-relevant papers")
    
    # Deduplicate by title similarity
    seen_titles = set()
    unique_papers = []
    for paper in all_papers:
        title_key = paper["title"].lower().strip()[:80]
        if title_key not in seen_titles:
            seen_titles.add(title_key)
            unique_papers.append(paper)
    
    print(f"\n  Total unique papers: {len(unique_papers)}")
    return unique_papers


# ── Claude Analysis ────────────────────────────────────────────

SYSTEM_PROMPT = """You are a climate finance research analyst producing a weekly digest 
for a spokesperson at MSCI who manages the Transition Finance Tracker. 
Your summaries must be precise, actionable, and highlight anything relevant 
to MSCI's climate data products (ITR, Climate VaR, ESG Ratings, carbon metrics).

Rules:
- Be specific about datasets used (name the dataset, time period, geography)
- Flag ANY mention of MSCI data prominently with 🔴
- Flag papers by SSRN Climate Finance eJournal board members (Tufano, Starks, 
  Bolton, Caldecott, Flammer, Stroebel, Giglio, Krueger, Pedersen)
- Keep bullet points to one sentence each
- Include the direct URL for every paper
- If a paper's abstract is too sparse, say so rather than guessing
"""

USER_PROMPT_TEMPLATE = """Produce a structured weekly digest from these {n_papers} papers 
collected from academic feeds for the week of {week_date}.

Format the output as clean Markdown with this structure:

# Climate Finance Research Digest — Week of {week_date}
**Papers found: {n_papers}**

---

## 🔴 Papers Using or Citing MSCI Data
(If none found, write "None identified this week")

## 🔵 Top-Tier Journal & NBER Publications

## 🟡 Other Notable Working Papers & Policy Research

---

For EACH paper, use exactly this template:

### [Paper Title]
| Field | Detail |
|-------|--------|
| **Authors** | Names (affiliations if visible) |
| **Source** | Journal or working paper series — Date |
| **Link** | [URL](URL) |
| **Research Question** | One sentence |
| **Data Used** | Specific datasets, periods, geographies. 🔴 if MSCI. |
| **Key Findings** | See bullets below |

- Finding 1
- Finding 2
- Finding 3

**Tracker Relevance:** One sentence on how this connects to Transition Finance 
Tracker themes (emissions / targets / disclosure / financial flows / physical risk / energy transition)

---

Here are the papers:

{papers_json}
"""


def analyze_with_claude(papers: list[dict], week_date: str) -> str:
    """Send collected papers to Claude for structured analysis."""
    client = Anthropic()
    
    user_prompt = USER_PROMPT_TEMPLATE.format(
        week_date=week_date,
        n_papers=len(papers),
        papers_json=json.dumps(papers, indent=2, default=str),
    )
    
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    
    return message.content[0].text


# ── Output ─────────────────────────────────────────────────────

def save_digest(content: str, week_date: str) -> str:
    """Save the digest as a Markdown file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"digest_{week_date}.md"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n✅ Digest saved to: {filepath}")
    return filepath


# ── Main ───────────────────────────────────────────────────────

def main():
    week_date = datetime.now().strftime("%Y-%m-%d")
    print(f"🔍 Climate Finance Research Monitor — {week_date}")
    print(f"   Looking back {LOOKBACK_DAYS} days\n")
    
    # Step 1: Collect papers from feeds
    print("📡 Collecting papers from feeds...")
    papers = collect_all_papers()
    
    if not papers:
        print("⚠️  No climate-relevant papers found. Check feeds and try again.")
        # Still save an empty digest
        save_digest(
            f"# Climate Finance Research Digest — Week of {week_date}\n\n"
            "No new climate-relevant papers found this week across monitored sources.\n",
            week_date,
        )
        return
    
    # Step 2: Analyze with Claude
    print("\n🤖 Analyzing with Claude...")
    digest = analyze_with_claude(papers, week_date)
    
    # Step 3: Save
    filepath = save_digest(digest, week_date)
    
    # Step 4: Print preview
    print("\n" + "=" * 70)
    # Show first 2000 chars as preview
    preview = digest[:2000]
    if len(digest) > 2000:
        preview += "\n\n... [truncated, see full file] ..."
    print(preview)


if __name__ == "__main__":
    main()
