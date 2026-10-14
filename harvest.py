#!/usr/bin/env python3
"""
Payments news harvester — deterministic source intake.

Pulls every new item from the registered sources (RSS/Atom/RDF feeds and
WordPress REST APIs), de-duplicates by canonical URL, and appends to
data/items.jsonl. Writes a per-source health report to data/health.json.

No AI, no judgement: this is the complete, dated, correctly-linked record of
what the scanning sources published. Relevance triage happens downstream.

Usage
  python harvest.py                      # daily mode: fetch what each feed currently holds
  python harvest.py --since 2026-08-14   # window mode: for sources that support history
                                         # (WordPress REST), pull everything since this date
  python harvest.py --export 2026-08-14 2026-10-13 out.jsonl
                                         # write the stored items inside a window to a file
  python harvest.py --health             # print the last health report

Source types: feed (RSS/Atom/RDF) · wp_rest (WordPress API, full history) ·
html_list (listing page without a feed; items dated by first sighting if no date is found)

Only dependencies: requests, pyyaml (pip install -r requirements.txt).
"""
import argparse
import datetime as dt
import hashlib
import html
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
ITEMS_PATH = os.path.join(DATA_DIR, "items.jsonl")
HEALTH_PATH = os.path.join(DATA_DIR, "health.json")
FEEDS_PATH = os.path.join(HERE, "feeds.yaml")

UA = "payments-news-harvester/1.0 (polite feed reader; a few requests per source per day)"
TIMEOUT = 30
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid", "ref"}


# ----------------------------------------------------------------------------- helpers

def canonical_url(url: str) -> str:
    """Strip tracking params and fragments so the same story has one id."""
    try:
        p = urlparse(url.strip())
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACKING_PARAMS]
        path = p.path.rstrip("/") or "/"
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", urlencode(q), ""))
    except Exception:
        return url.strip()


def item_id(url: str) -> str:
    return hashlib.sha1(canonical_url(url).encode("utf-8")).hexdigest()[:16]


def clean_text(s: str, limit: int = 400) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)          # strip HTML
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("[&hellip;]", "…").replace("[…]", "…")
    return s[:limit]


def parse_date(s: str):
    """Return an ISO date (YYYY-MM-DD) from the many formats feeds use, or None."""
    if not s:
        return None
    s = s.strip()
    for fn in (parsedate_to_datetime,):
        try:
            d = fn(s)
            if d:
                return d.date().isoformat()
        except Exception:
            pass
    # ISO-ish
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y", "%d/%m/%Y",
                "%A, %B %d, %Y - %H:%M", "%A, %d %B %Y - %H:%M", "%A, %B %d, %Y"):
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    # last resort: find a "13 Oct 2026" / "October 13, 2026" anywhere in the string
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", s) or re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})", s)
    if m:
        for fmt in ("%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y"):
            try:
                return dt.datetime.strptime(" ".join(m.groups()), fmt).date().isoformat()
            except Exception:
                pass
    return None


def load_existing():
    ids = set()
    if os.path.exists(ITEMS_PATH):
        with open(ITEMS_PATH, encoding="utf-8") as f:
            for line in f:
                try:
                    ids.add(json.loads(line)["id"])
                except Exception:
                    continue
    return ids


def append_items(items):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ITEMS_PATH, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def get(url, params=None):
    r = requests.get(url, params=params, headers={"User-Agent": UA, "Accept": "*/*"}, timeout=TIMEOUT)
    r.raise_for_status()
    return r


# ----------------------------------------------------------------------------- fetchers

def fetch_feed(src):
    """RSS 2.0, Atom, or RSS 1.0 (RDF). Returns a list of raw items."""
    r = get(src["url"])
    root = ET.fromstring(r.content)
    ns = {"atom": "http://www.w3.org/2005/Atom",
          "rss1": "http://purl.org/rss/1.0/",
          "dc": "http://purl.org/dc/elements/1.1/",
          "content": "http://purl.org/rss/1.0/modules/content/"}
    out = []
    tag = root.tag.lower()

    if tag.endswith("feed"):  # Atom
        for e in root.findall("atom:entry", ns):
            link = ""
            for l in e.findall("atom:link", ns):
                if l.get("rel") in (None, "alternate"):
                    link = l.get("href", "")
                    break
            out.append({
                "title": (e.findtext("atom:title", default="", namespaces=ns) or ""),
                "url": link,
                "date": e.findtext("atom:published", default="", namespaces=ns) or e.findtext("atom:updated", default="", namespaces=ns),
                "summary": e.findtext("atom:summary", default="", namespaces=ns) or e.findtext("atom:content", default="", namespaces=ns),
            })
    elif tag.endswith("rdf"):  # RSS 1.0
        for e in root.findall("rss1:item", ns):
            out.append({
                "title": e.findtext("rss1:title", default="", namespaces=ns),
                "url": e.findtext("rss1:link", default="", namespaces=ns),
                "date": e.findtext("dc:date", default="", namespaces=ns),
                "summary": e.findtext("rss1:description", default="", namespaces=ns),
            })
    else:  # RSS 2.0
        for e in root.iter("item"):
            out.append({
                "title": e.findtext("title", default=""),
                "url": e.findtext("link", default="") or (e.find("guid").text if e.find("guid") is not None else ""),
                "date": e.findtext("pubDate", default="") or e.findtext("dc:date", default="", namespaces=ns),
                "summary": e.findtext("description", default="") or e.findtext("content:encoded", default="", namespaces=ns),
            })
    return out


def fetch_wp_rest(src, since=None):
    """WordPress REST API: full history with date filters. 100 per page."""
    base = src["url"].rstrip("/") + "/wp-json/wp/v2/posts"
    params = {"per_page": 100, "page": 1, "_fields": "id,date_gmt,link,title,excerpt",
              "orderby": "date", "order": "desc"}
    if since:
        params["after"] = f"{since}T00:00:00"
    out = []
    max_pages = src.get("max_pages", 60 if since else 2)
    while params["page"] <= max_pages:
        r = get(base, params=params)
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        for p in batch:
            out.append({
                "title": (p.get("title") or {}).get("rendered", ""),
                "url": p.get("link", ""),
                "date": p.get("date_gmt", ""),
                "summary": (p.get("excerpt") or {}).get("rendered", ""),
            })
        total_pages = int(r.headers.get("X-WP-TotalPages", "1") or 1)
        if params["page"] >= total_pages:
            break
        params["page"] += 1
        time.sleep(0.5)
    return out


def fetch_html_list(src):
    """Publisher listing pages without a feed. Finds article links by a URL regex,
    takes the anchor text as the title, and looks for a date within the next 400
    characters of HTML; if none, the item is dated by first sighting (flagged)."""
    pattern = re.compile(src["link_pattern"])
    out, seen = [], set()
    for page in src.get("pages") or [src["url"]]:
        html_text = get(page).text
        for m in re.finditer(r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text, re.S | re.I):
            href, inner = m.group(1), m.group(2)
            if href.startswith("/"):
                base = re.match(r"https?://[^/]+", page).group(0)
                href = base + href
            if not pattern.search(href) or href in seen:
                continue
            title = clean_text(inner, 300)
            if len(title) < 15:          # image/icon anchors; the text anchor comes later
                continue
            seen.add(href)
            window = html_text[m.end(): m.end() + 400]
            d = parse_date(clean_text(window, 400))
            out.append({"title": title, "url": href, "date": d or "", "summary": "",
                        "date_estimated": d is None})
        time.sleep(0.5)
    return out


# ----------------------------------------------------------------------------- main

def harvest(since=None, only=None):
    cfg = yaml.safe_load(open(FEEDS_PATH, encoding="utf-8"))
    existing = load_existing()
    now = dt.datetime.now(dt.timezone.utc)
    today = now.date().isoformat()
    tomorrow = (now.date() + dt.timedelta(days=1)).isoformat()
    health = {"run_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "since": since, "sources": {}}
    new_items = []
    for src in cfg["sources"]:
        name = src["name"]
        if only and name not in only:
            continue
        if not src.get("enabled", True):
            health["sources"][name] = {"status": "disabled"}
            continue
        t0 = time.time()
        try:
            if src["type"] == "wp_rest":
                raw = fetch_wp_rest(src, since=since)
            elif src["type"] == "feed":
                raw = fetch_feed(src)
            elif src["type"] == "html_list":
                raw = fetch_html_list(src)
            else:
                raise ValueError(f"unknown type {src['type']}")
            added = 0
            dates = []
            for it in raw:
                url = (it.get("url") or "").strip()
                if not url.startswith("http"):
                    continue
                iid = item_id(url)
                d = parse_date(it.get("date", ""))
                if d and d > tomorrow:      # events and webinars dated in the future
                    continue
                if not d and it.get("date_estimated"):
                    d = today               # dated by first sighting; flagged below
                if d:
                    dates.append(d)
                if iid in existing:
                    continue
                existing.add(iid)
                new_items.append({
                    "id": iid,
                    "source": name,
                    "tier": src.get("tier", 2),
                    "kind": src.get("kind", ""),
                    "title": clean_text(it.get("title", ""), 300),
                    "url": canonical_url(url),
                    "published": d,
                    "date_estimated": bool(it.get("date_estimated")),
                    "summary": clean_text(it.get("summary", "")),
                    "fetched_at": health["run_at"],
                })
                added += 1
            health["sources"][name] = {
                "status": "ok", "fetched": len(raw), "added": added,
                "oldest": min(dates) if dates else None, "newest": max(dates) if dates else None,
                "seconds": round(time.time() - t0, 1),
            }
            print(f"  ok   {name:<28} fetched={len(raw):<4} new={added:<4} range={health['sources'][name]['oldest']}..{health['sources'][name]['newest']}")
        except Exception as e:  # keep going — one broken feed must not kill the run
            health["sources"][name] = {"status": "error", "error": str(e)[:300]}
            print(f"  FAIL {name:<28} {str(e)[:120]}")
        time.sleep(float(cfg.get("polite_delay_seconds", 1)))

    append_items(new_items)
    os.makedirs(DATA_DIR, exist_ok=True)
    json.dump(health, open(HEALTH_PATH, "w", encoding="utf-8"), indent=2)
    print(f"\n{len(new_items)} new items appended → {os.path.relpath(ITEMS_PATH)}")
    return new_items


def export(start, end, out_path):
    n = 0
    with open(ITEMS_PATH, encoding="utf-8") as f, open(out_path, "w", encoding="utf-8") as g:
        for line in f:
            it = json.loads(line)
            d = it.get("published")
            if d and start <= d <= end:
                g.write(line)
                n += 1
    print(f"{n} items in window {start}..{end} → {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD: pull history from this date where the source supports it")
    ap.add_argument("--only", nargs="*", help="limit to these source names")
    ap.add_argument("--export", nargs=3, metavar=("START", "END", "OUT"), help="export stored items in a window")
    ap.add_argument("--health", action="store_true", help="print last health report")
    a = ap.parse_args()
    if a.health:
        print(open(HEALTH_PATH, encoding="utf-8").read() if os.path.exists(HEALTH_PATH) else "no health report yet")
    elif a.export:
        export(*a.export)
    else:
        harvest(since=a.since, only=a.only)
