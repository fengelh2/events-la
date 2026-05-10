"""Fetch and parse iCal (.ics) URLs into the canonical momEvents schema.

Used by tools/scrape_venue_events.py for venues whose `kind: ical`. May also be
imported standalone for one-off probes.

Canonical Event schema (dict, keys match projects/momEvents/CLAUDE.md):
    title, start, end, venue_id, venue_name, city, category, url, description,
    price, source

Functions:
    discover_ics_urls(listing_url, pattern, base_url=None) -> list[str]
        Fetch a venue's calendar listing page and extract all .ics URLs that
        match the configured regex pattern.

    fetch_ics_events(ics_url) -> list[dict]
        Fetch one .ics file and return its VEVENTs as raw dicts (lib-agnostic).
        Caller is responsible for assembling the canonical Event.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import icalendar
import requests

log = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "momEvents-calendar/0.1 (+private use, contact: fengelh@gmail.com)"
    ),
    "Accept": "text/calendar, text/html, */*",
}

DEFAULT_TIMEOUT = 20


def discover_ics_urls(
    listing_url: str,
    pattern: str,
    base_url: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> list[str]:
    """Fetch the listing page and return absolute .ics URLs matching `pattern`.

    The pattern is a regex applied to the raw HTML. Matches are deduplicated and
    converted to absolute URLs against `base_url` (or the listing URL).
    """
    sess = session or requests
    resp = sess.get(listing_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    raw_matches = re.findall(pattern, resp.text)
    base = base_url or listing_url
    abs_urls = sorted({urljoin(base, m) for m in raw_matches})
    log.info("discover_ics_urls(%s): %d matches", listing_url, len(abs_urls))
    return abs_urls


def fetch_ics_events(
    ics_url: str,
    session: Optional[requests.Session] = None,
) -> list[dict]:
    """Fetch one .ics file and return one dict per VEVENT.

    Each dict has these keys (raw, before canonicalization):
        title, start, end, location, url, uid, description
    """
    sess = session or requests
    resp = sess.get(ics_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    cal = icalendar.Calendar.from_ical(resp.content)
    events: list[dict] = []
    for ev in cal.walk("VEVENT"):
        events.append(
            {
                "title": _str(ev.get("summary")),
                "start": _dt(ev.get("dtstart")),
                "end": _dt(ev.get("dtend")),
                "location": _str(ev.get("location")),
                "url": _str(ev.get("url")),
                "uid": _str(ev.get("uid")),
                "description": _str(ev.get("description")),
            }
        )
    return events


def _str(field) -> str:
    if field is None:
        return ""
    s = str(field).strip()
    # icalendar sometimes wraps strings in vText; str() unwraps. Replace
    # common runaway control chars from sloppy CMSes.
    return s.replace("", "–").replace("", "—")


def _dt(field) -> Optional[datetime]:
    """Convert an icalendar DATE/DATETIME field to a tz-aware datetime.

    iCal DATE values (no time) become midnight in UTC, which is fine for our
    "all-day exhibition" use case — the renderer formats date-only when the
    time is exactly 00:00:00 UTC and end-start is a whole number of days.
    """
    if field is None:
        return None
    val = field.dt
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
    return None


if __name__ == "__main__":
    # Smoke test: probe TUP Essen, fetch first 3, print their summaries.
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    listing = "https://www.theater-essen.de/programm/kalender/"
    pattern = r"/kalender-eintrag/\d+/ical-\d{4}-\d{2}-\d{2}-\d+\.ics"
    urls = discover_ics_urls(listing, pattern)
    print(f"Found {len(urls)} .ics URLs")
    for u in urls[:3]:
        for ev in fetch_ics_events(u):
            print(f"  {ev['start']!s:30s} | {ev['location']:30.30s} | {ev['title']}")
