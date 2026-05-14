"""Parametric scraper that dispatches on `kind` field in venues.yaml.

Supported kinds (current):
    ical        — uses tools.parse_ical to discover + fetch .ics URLs
    html_list   — CSS selectors against the listing page (with optional detail-page follow)
    unknown     — skipped (pending onboarding)

Future kinds:
    json_ld     — extruct on the listing or detail pages

The scraper assembles each event into the canonical schema documented in
projects/momEvents/CLAUDE.md. A single source row may produce events under
multiple `venue_id`s when the source row declares `produces_venue_ids:` plus
a `stage_resolver:` rule list.
"""

from __future__ import annotations

import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import dateparser
import requests
import yaml
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent))
import parse_ical  # noqa: E402

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 20
DEFAULT_HEADERS = parse_ical.DEFAULT_HEADERS


@dataclass
class Event:
    """Canonical event row. Matches the schema in projects/momEvents/CLAUDE.md."""

    title: str
    start: datetime
    end: Optional[datetime]
    venue_id: str
    venue_name: str
    city: str
    category: str
    url: str
    description: Optional[str] = None
    price: Optional[str] = None
    source: str = ""
    audience: str = "general"   # general | kids | educational  (drives display dimming)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["start"] = self.start.isoformat() if self.start else None
        d["end"] = self.end.isoformat() if self.end else None
        return d


# ─── public API ──────────────────────────────────────────────────────────────


def scrape(venue_row: dict, session: Optional[requests.Session] = None) -> list[Event]:
    """Dispatch one venue row to the right scraping path.

    On any failure, logs the error and returns []. Per-venue isolation is the
    workflow's responsibility (see projects/momEvents/workflows/rebuild_calendar.md).
    """
    kind = venue_row.get("kind", "unknown")
    venue_id = venue_row.get("id", "?")
    try:
        if kind == "ical":
            events = _scrape_ical(venue_row, session=session)
        elif kind == "html_list":
            events = _scrape_html_list(venue_row, session=session)
        elif kind == "detail_pages":
            events = _scrape_detail_pages(venue_row, session=session)
        elif kind == "static":
            events = _scrape_static(venue_row)
        elif kind == "playwright_html_list":
            events = _scrape_playwright_html_list(venue_row, session=session)
        elif kind == "json_ld_aggregator":
            events = _scrape_json_ld_aggregator(venue_row, session=session)
        elif kind == "tribe_rest":
            events = _scrape_tribe_rest(venue_row, session=session)
        elif kind == "algolia_calendar":
            events = _scrape_algolia_calendar(venue_row, session=session)
        elif kind == "flat_json_feed":
            events = _scrape_flat_json_feed(venue_row, session=session)
        elif kind == "nextjs_contentful":
            events = _scrape_nextjs_contentful(venue_row, session=session)
        elif kind == "unknown":
            log.info("skip %s: kind=unknown (pending onboarding)", venue_id)
            return []
        else:
            log.warning("skip %s: unknown kind=%r", venue_id, kind)
            return []
    except Exception as exc:
        log.error("scrape failed for %s: %s: %s", venue_id, type(exc).__name__, exc)
        return []
    log.info("scrape %s (%s): %d events", venue_id, kind, len(events))
    return events


# ─── detail-pages path ──────────────────────────────────────────────────────
# For venues whose listing page has only URLs (no item-level data), and all
# title/date info lives on per-event detail pages. Lindenbrauerei Unna fits.

def _scrape_detail_pages(venue_row: dict, session=None) -> list[Event]:
    """Discover detail URLs from the listing, then fetch each detail page
    and extract title + date from selectors there.

    Config:
      detail_url_pattern: regex to find detail URLs on listing
      selectors: {title, date} for the detail page
      title_strip_suffixes: optional cleanup
      date_extract_regex: optional, applied to extracted date text
    """
    sess = session or requests
    listing_url = venue_row["calendar_url"]
    pattern = venue_row.get("detail_url_pattern")
    if not pattern:
        log.warning("%s: detail_pages kind missing detail_url_pattern", venue_row["id"])
        return []
    sel = venue_row.get("selectors") or {}

    try:
        resp = sess.get(listing_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("%s: listing fetch failed: %s", venue_row["id"], exc)
        return []

    matches = re.findall(pattern, resp.text)
    detail_urls = sorted({urljoin(listing_url, m) for m in matches})
    log.debug("%s: found %d detail URLs", venue_row["id"], len(detail_urls))

    out: list[Event] = []
    for url in detail_urls:
        try:
            r = sess.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
        except requests.RequestException as exc:
            log.debug("  %s: detail fetch failed: %s", url, exc)
            continue
        page = BeautifulSoup(r.content, "html.parser")
        title = _select_text(page, sel.get("title", "title"))
        title = _clean_title(title)
        for suffix in venue_row.get("title_strip_suffixes") or []:
            title = re.sub(re.escape(suffix), "", title, flags=re.IGNORECASE).strip()
        date_text = _select_text(page, sel.get("date"))
        extract_re = venue_row.get("date_extract_regex")
        if extract_re and date_text:
            m = re.search(extract_re, date_text)
            if m:
                date_text = m.group(0)
        if not title or not date_text:
            continue
        for pat in venue_row.get("skip_if_title_matches") or []:
            if re.search(pat, title):
                title = ""
                break
        if not title:
            continue
        start = _parse_one(date_text, venue_row.get("date_format"))
        if start is None:
            log.debug("  %s: failed to parse date %r", url, date_text)
            continue
        out.append(
            Event(
                title=title,
                start=start,
                end=None,
                venue_id=venue_row["id"],
                venue_name=venue_row.get("display_name") or venue_row["name"],
                city=venue_row.get("city", ""),
                category=venue_row.get("category", "other"),
                url=url,
                description=None,
                source=venue_row["id"],
                audience=_infer_audience(title),
            )
        )
    return out


# ─── Runtime-Playwright HTML list path ──────────────────────────────────────
# For JS-rendered LA venues without a discoverable JSON API (CTG queue-it,
# The Broad, MOCA, Soraya, CAP UCLA, etc.) — render the page in headless
# Chromium, then run html_list-style selectors against the rendered DOM.
# CI workflow installs `playwright install --with-deps chromium`.


def _scrape_playwright_html_list(venue_row: dict, session=None) -> list[Event]:
    """Render `calendar_url` in headless Chromium, then extract events using
    html_list-style selectors against the rendered DOM.

    Config (extends html_list):
      calendar_url, selectors.item/title/date/...: same as html_list
      wait_for_selector: optional CSS selector to wait for before extraction
      scroll:            bool, default true — scroll to bottom for lazy-load
      dismiss_cookies:   bool, default true — try common cookie-accept buttons
      timeout_ms:        page-load timeout (default 45000)
      extra_wait_ms:     fixed wait after load/scroll (default 3000)
      locale:            browser locale (default 'en-US')
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("%s: playwright not installed; skipping", venue_row["id"])
        return []

    url = venue_row["calendar_url"]
    wait_for = venue_row.get("wait_for_selector")
    do_scroll = bool(venue_row.get("scroll", True))
    do_cookies = bool(venue_row.get("dismiss_cookies", True))
    timeout_ms = int(venue_row.get("timeout_ms", 45000))
    extra_wait_ms = int(venue_row.get("extra_wait_ms", 3000))

    html_text: Optional[str] = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                locale=venue_row.get("locale", "en-US"),
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if do_cookies:
                for selector in [
                    "button:has-text('Accept all')",
                    "button:has-text('Allow all')",
                    "button:has-text('I agree')",
                    "button:has-text('Akzeptieren')",
                    "[data-testid='uc-accept-all-button']",
                    "#uc-btn-accept-banner",
                    "button.uc-btn-accept",
                    "#CybotCookiebotDialogBodyButtonAccept",
                ]:
                    try:
                        page.click(selector, timeout=1500)
                        break
                    except Exception:
                        pass
            if wait_for:
                try:
                    page.wait_for_selector(wait_for, timeout=timeout_ms)
                except Exception as exc:
                    log.warning("%s: wait_for_selector failed: %s", venue_row["id"], exc)
            if do_scroll:
                try:
                    for _ in range(8):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(700)
                except Exception:
                    pass
            page.wait_for_timeout(extra_wait_ms)
            html_text = page.content()
            browser.close()
    except Exception as exc:
        log.warning("%s: playwright render failed: %s", venue_row["id"], exc)
        return []

    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    sel = venue_row.get("selectors") or {}
    item_sel = sel.get("item")
    if not item_sel:
        log.warning("%s: playwright_html_list missing selectors.item", venue_row["id"])
        return []
    items = soup.select(item_sel)
    log.debug("%s: rendered DOM has %d items", venue_row["id"], len(items))

    out: list[Event] = []
    prev_day = None
    for it in items:
        ev, prev_day = _assemble_from_html_item(
            it, url, venue_row,
            month_ctx=None,
            ctx_year=None,
            prev_day=prev_day,
        )
        if ev is not None:
            out.append(ev)
    log.info("%s: %d events from playwright_html_list", venue_row["id"], len(out))
    return out


# ─── static path ─────────────────────────────────────────────────────────────


def _scrape_static(venue_row: dict) -> list[Event]:
    """Return hardcoded Event objects from the venue's `static_events` list.

    Use case: venues with no scrapeable calendar — Villa Hügel's permanent
    Krupp exhibition, Domschatz Essen's renovation closure notice. The user
    maintains these entries by hand in venues.yaml.
    """
    out: list[Event] = []
    for item in venue_row.get("static_events") or []:
        title = _clean_title(item.get("title") or "")
        start_raw = item.get("start")
        if not title or not start_raw:
            continue
        start = _coerce_to_dt(start_raw)
        end = _coerce_to_dt(item.get("end"))
        if start is None:
            log.warning("%s: static_events entry skipped (bad start=%r)", venue_row["id"], start_raw)
            continue
        category = item.get("category") or venue_row.get("category", "other")
        out.append(
            Event(
                title=title,
                start=start,
                end=end,
                venue_id=venue_row["id"],
                venue_name=venue_row.get("display_name") or venue_row["name"],
                city=venue_row.get("city", ""),
                category=category,
                url=item.get("detail_url") or venue_row.get("homepage", "#"),
                description=_clean_title(item.get("description") or "") or None,
                source=venue_row["id"],
                audience=item.get("audience", "general"),
            )
        )
    return out


# ─── json-ld aggregator path ────────────────────────────────────────────────
# For sites that publish many events on one page as JSON-LD entities (Discover
# Los Angeles being the canonical case). Two-step extraction:
#   1. Parse <script type="application/ld+json"> blocks for `ItemList`-shape
#      data with nested Event entities — gives clean title/start/end/venue/url.
#   2. Parse the HTML cards' data-* attrs for region/neighborhood/category —
#      these come from the publisher's editorial taxonomy and are far cleaner
#      than the JSON-LD `addressLocality` field (which often just says
#      "Los Angeles" with whitespace/case noise).
# Dedupe by URL — long-running exhibitions repeat in ItemList for every day
# the calendar shows them (60+ entries for a single 6-month run).
import json as _json

# Map Discover-LA neighborhood → our 4 zones. Source of truth for any
# new aggregator with the same shape: extend this map (or pass `zone_map`
# in the venue row to override).
_DISCOVER_LA_ZONE_MAP = {
    # Central LA
    "Mid-Wilshire": "Central LA",
    "Downtown": "Central LA",
    "Hollywood": "Central LA",
    "Arts District": "Central LA",
    "Chinatown": "Central LA",
    "USC": "Central LA",
    "Mid City": "Central LA",
    "La Brea": "Central LA",
    "West Hollywood": "Central LA",
    "Exposition Park": "Central LA",
    "Historic West Adams": "Central LA",
    "Koreatown": "Central LA",
    "Los Feliz": "Central LA",
    "Westlake": "Central LA",
    "Echo Park": "Central LA",
    "Highland Park": "Central LA",
    "Eagle Rock": "Central LA",
    "Silver Lake": "Central LA",
    # Westside
    "Brentwood": "Westside",
    "Pacific Palisades": "Westside",
    "Venice": "Westside",
    "Santa Monica": "Westside",
    "Westwood": "Westside",
    "Beverly Hills": "Westside",
    "Topanga": "Westside",
    "Culver City": "Westside",
    "Marina del Rey": "Westside",
    # Pasadena & East
    "Pasadena": "Pasadena & East",
    "San Marino": "Pasadena & East",
    "Glendale": "Pasadena & East",
    "Burbank": "Pasadena & East",
    "Sierra Madre": "Pasadena & East",
    "Claremont": "Pasadena & East",
    # Greater LA — anything else falls through to default
}

# Map publisher's editorial category → our schema's category.
_DISCOVER_LA_DENY_CATEGORIES = frozenset({
    "Sports",
    "Food & Drink",
    "Community Viewing Parties",
    "Official Fan Zones",
    "Official Fan Festival",
    "World Cup Matches",
    "Outdoors",
    "Miscellaneous",
})

_DISCOVER_LA_CATEGORY_MAP = {
    "Museums": "museum_exhibition",
    "Art Shows & Galleries": "museum_exhibition",
    "Arts & Culture": "museum_exhibition",
    "Cultural Heritage": "museum_exhibition",
    "Music": "concert",
    "Music & Entertainment": "concert",
    "Theatre": "theatre",
    "Arts & Theatre": "theatre",
    "Film": "film",
    "Film, TV & Radio": "film",
    "Comedy": "theatre",
    "Festivals": "other",
    "Food & Drink": "other",
    "Sports": "other",
    "Educational": "other",
    "Outdoors": "other",
}


def _scrape_json_ld_aggregator(venue_row: dict, session=None) -> list[Event]:
    """Aggregator parser: pulls many events from one page via JSON-LD ItemList
    + HTML data-attrs for editorial taxonomy.

    Config:
      calendar_url: page URL
      zone_map: optional {neighborhood: zone} override (else _DISCOVER_LA_ZONE_MAP)
      default_zone: zone for events with no recognized neighborhood (default "Greater LA")
    """
    sess = session or requests
    url = venue_row["calendar_url"]
    try:
        resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("%s: page fetch failed: %s", venue_row["id"], exc)
        return []
    text = resp.text

    zone_map = venue_row.get("zone_map") or _DISCOVER_LA_ZONE_MAP
    default_zone = venue_row.get("default_zone", "Greater LA")
    cat_map = venue_row.get("category_map") or _DISCOVER_LA_CATEGORY_MAP
    deny_cats = frozenset(venue_row.get("deny_categories") or _DISCOVER_LA_DENY_CATEGORIES)

    # Step 1: build URL → (neighborhood, region, category) lookup from HTML cards.
    # Each event-card-info anchor carries data-* attrs we trust more than the
    # JSON-LD's addressLocality.
    card_attrs: dict[str, dict] = {}
    for m in re.finditer(r'<a[^>]+data-nid=\"\d+\"[^>]*>', text):
        tag = m.group(0)
        href_m = re.search(r'href=\"([^\"]+)\"', tag)
        if not href_m:
            continue
        href = urljoin(url, href_m.group(1))
        # First card seen wins; subsequent duplicates carry identical attrs.
        if href in card_attrs:
            continue
        attrs = {}
        for k in ("neighborhood", "region", "category", "venue", "location", "start-date"):
            am = re.search(r'data-' + k + r'=\"([^\"]+)\"', tag)
            if am:
                # html-decode &amp; etc.
                v = am.group(1).replace("&amp;", "&").replace("&#39;", "'")
                attrs[k] = v
        card_attrs[href] = attrs
    log.debug("%s: %d HTML cards indexed by URL", venue_row["id"], len(card_attrs))

    # Step 2: walk JSON-LD blocks. Three shapes supported:
    #   (a) ItemList with itemListElement[] of {url, item: Event}    [Discover LA]
    #   (b) bare Event block, one per <script>                       [Live Nation]
    #   (c) array of Event blocks at top level                       [some CMS]
    seen_urls: set[str] = set()
    is_aggregator = venue_row.get("city") == "__aggregator__"
    out: list[Event] = []
    for block in re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', text, re.DOTALL):
        try:
            data = _json.loads(block)
        except _json.JSONDecodeError:
            continue
        # Normalize to a list of (event_dict, url_hint) pairs. Accept any
        # Schema.org Event subclass — MusicEvent, TheaterEvent, DanceEvent,
        # SportsEvent, ScreeningEvent, ComedyEvent, BusinessEvent, etc.
        def _is_event_type(t):
            if not isinstance(t, str):
                return False
            return t == "Event" or t.endswith("Event")

        candidates: list[tuple[dict, str | None]] = []
        if isinstance(data, list):
            for d in data:
                if isinstance(d, dict) and _is_event_type(d.get("@type")):
                    candidates.append((d, d.get("url")))
        elif isinstance(data, dict):
            if _is_event_type(data.get("@type")):
                candidates.append((data, data.get("url")))
            for li in data.get("itemListElement", []) or []:
                if isinstance(li, dict):
                    ev = li.get("item") or {}
                    if isinstance(ev, dict) and _is_event_type(ev.get("@type")):
                        candidates.append((ev, li.get("url") or ev.get("url")))
        for ev, ev_url in candidates:
            if not ev_url or ev_url in seen_urls:
                continue
            seen_urls.add(ev_url)

            title = _clean_title(ev.get("name") or "")
            start = _parse_jsonld_dt(ev.get("startDate"))
            end = _parse_jsonld_dt(ev.get("endDate"))
            if not title or start is None:
                continue

            # Drop obvious non-cultural noise (singles parties, pet adoptions,
            # summer camps, etc.). Only applied to aggregators — single-venue
            # sources are pre-curated.
            if is_aggregator:
                tlow = title.lower()
                if any(d in tlow for d in _TITLE_DENY_KEYWORDS):
                    continue

            loc = ev.get("location") or {}
            venue_name_jsonld = (loc.get("name") if isinstance(loc, dict) else None) or ""
            venue_name = _clean_title(venue_name_jsonld) or venue_row.get("display_name") or venue_row["name"]

            if is_aggregator:
                # Per-event zoning + category from companion HTML data-attrs.
                attrs = card_attrs.get(ev_url, {})
                neighborhood = attrs.get("neighborhood", "")
                zone = zone_map.get(neighborhood, default_zone)
                raw_cat = attrs.get("category", "")
                if raw_cat in deny_cats:
                    continue
                base_cat = cat_map.get(raw_cat) or venue_row.get("category", "other")
                if base_cat == "mixed":
                    base_cat = "other"
                # Aggregator → venue unknown; trust title/venue keywords to refine.
                category = _infer_category(title, venue_row, stage_default=base_cat, venue_name=venue_name)
            else:
                # Single-venue page (Live Nation, etc.) — trust the venue's
                # declared category. Title keyword overlay is risky here: it
                # false-positives on marketing copy ("cosmic opera" tour names).
                zone = venue_row.get("city") or default_zone
                category = venue_row.get("category", "other")
                if category == "mixed":
                    category = "other"
                venue_name = venue_row.get("display_name") or venue_row["name"]

            out.append(
                Event(
                    title=title,
                    start=start,
                    end=end,
                    venue_id=venue_row["id"],
                    venue_name=venue_name or venue_row.get("display_name") or venue_row["name"],
                    city=zone,
                    category=category,
                    url=ev_url,
                    description=None,
                    source=venue_row["id"],
                    audience=_infer_audience(title),
                )
            )
    log.info("%s: %d unique events from json_ld_aggregator", venue_row["id"], len(out))
    return out


def _parse_jsonld_dt(s) -> Optional[datetime]:
    """Parse a JSON-LD ISO-8601 datetime; return tz-aware or None."""
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        # JSON-LD is supposed to carry TZ; fall back to America/Los_Angeles
        # (the page is LA-local) by treating as UTC and shifting.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ─── tribe events REST API path ─────────────────────────────────────────────
# WordPress + The Events Calendar (Tribe) plugin exposes a JSON API at
# /wp-json/tribe/events/v1/events with paginated event records. Pasadena
# Playhouse, AceHotel (Theatre at Ace), Pasadena Symphony, Piano Spheres all
# expose this endpoint. Cleanest single-venue source available in LA.

import html as _html


def _scrape_tribe_rest(venue_row: dict, session=None) -> list[Event]:
    """Walk a Tribe Events REST API, paginating until exhausted.

    Config:
      calendar_url: REST endpoint (typically `.../wp-json/tribe/events/v1/events?per_page=100`)
      filter_venue_substring: optional — keep events whose `venue.venue` contains this string
        (used by aggregator hosts like AceHotel that mix multiple programs).
      max_pages: safety cap (default 30)
    """
    sess = session or requests
    base_url = venue_row["calendar_url"]
    sep = "&" if "?" in base_url else "?"
    if "per_page=" not in base_url:
        base_url = f"{base_url}{sep}per_page=100"
        sep = "&"

    venue_filter = venue_row.get("filter_venue_substring")
    max_pages = int(venue_row.get("max_pages", 30))
    out: list[Event] = []
    seen_urls: set[str] = set()

    for page in range(1, max_pages + 1):
        page_url = f"{base_url}{sep}page={page}"
        try:
            r = sess.get(page_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("%s: tribe_rest fetch failed page=%d: %s", venue_row["id"], page, exc)
            break
        events = data.get("events") or []
        if not events:
            break
        for raw in events:
            ev = _tribe_to_event(raw, venue_row, venue_filter=venue_filter)
            if ev is None:
                continue
            if ev.url in seen_urls:
                continue
            seen_urls.add(ev.url)
            out.append(ev)
        if page >= int(data.get("total_pages") or 1):
            break

    log.info("%s: %d events from tribe_rest", venue_row["id"], len(out))
    return out


def _tribe_to_event(raw: dict, venue_row: dict, venue_filter: str | None = None) -> Optional[Event]:
    """Map one Tribe REST record to a canonical Event."""
    title = raw.get("title") or ""
    if not title:
        return None
    title = _clean_title(_html_decode(title))
    if not title:
        return None

    # Optional venue scoping for aggregator hosts.
    if venue_filter:
        venue_obj = raw.get("venue")
        if isinstance(venue_obj, dict):
            vname = venue_obj.get("venue") or ""
        elif isinstance(venue_obj, list) and venue_obj:
            vname = venue_obj[0].get("venue", "") if isinstance(venue_obj[0], dict) else ""
        else:
            vname = ""
        if venue_filter.lower() not in vname.lower():
            return None

    start = _parse_tribe_dt(raw.get("utc_start_date") or raw.get("start_date"))
    end = _parse_tribe_dt(raw.get("utc_end_date") or raw.get("end_date"))
    if start is None:
        return None

    url = raw.get("url") or venue_row.get("homepage", "#")
    venue_obj = raw.get("venue")
    venue_name = ""
    if isinstance(venue_obj, dict):
        venue_name = _html_decode(venue_obj.get("venue") or "")
    venue_name = venue_name or venue_row.get("display_name") or venue_row["name"]

    # Single-venue source: trust the venue's declared category. Title overlay
    # is risky for the same reason as the json_ld single-venue path.
    category = venue_row.get("category", "other")
    if category == "mixed":
        category = "other"
    return Event(
        title=title,
        start=start,
        end=end,
        venue_id=venue_row["id"],
        venue_name=venue_name,
        city=venue_row.get("city", ""),
        category=category,
        url=url,
        description=None,
        source=venue_row["id"],
        audience=_infer_audience(title),
    )


# ─── Algolia calendar API path ──────────────────────────────────────────────
# Tessitura-on-Kentico venues (LA Opera, LA Phil, Hollywood Bowl, ...) expose
# their performance schedule via an Algolia search index. The React app on
# /whats-on (or /events/performances) sends a POST to:
#   https://<APP_ID>-dsn.algolia.net/1/indexes/*/queries
#   ?x-algolia-api-key=<KEY>&x-algolia-application-id=<APP_ID>
# Body: {"requests":[{"indexName":"<INDEX>","params":"<urlencoded params>"}]}
# Response hit shape: {Title, SubTitle, Venue, StartDate (ms),
# PerformanceDates [ms], PrimaryCategory, KenticoUrl, ...}
# The API key is public (used by the page's own JS); no auth required.

from urllib.parse import urlencode


def _scrape_algolia_calendar(venue_row: dict, session=None) -> list[Event]:
    """Fetch performances from an Algolia-indexed Tessitura calendar.

    Config:
      algolia_app_id       — Algolia application ID
      algolia_api_key      — public search-only API key (from the page's JS)
      algolia_index        — index name (e.g. prod_laopera_calendar)
      algolia_filters      — Algolia `filters` param (default: ItemType:Performance
                             AND ExcludeFromCalendar:false)
      algolia_hits_per_page — int (default 200; max usually 1000)
      origin               — for the Origin header (default: venue homepage)
      url_prefix           — prefix for KenticoUrl when it's a relative path
                             (default: venue homepage)
    """
    sess = session or requests
    app_id = venue_row.get("algolia_app_id")
    api_key = venue_row.get("algolia_api_key")
    index = venue_row.get("algolia_index")
    if not (app_id and api_key and index):
        log.warning("%s: algolia_calendar missing app_id/api_key/index", venue_row["id"])
        return []

    filters = venue_row.get(
        "algolia_filters",
        "ExcludeFromCalendar:false AND ItemType:Performance",
    )
    hits_per_page = int(venue_row.get("algolia_hits_per_page", 200))
    origin = venue_row.get("origin") or venue_row.get("homepage", "")
    url_prefix = venue_row.get("url_prefix") or venue_row.get("homepage", "")

    endpoint = f"https://{app_id.lower()}-dsn.algolia.net/1/indexes/*/queries"
    out: list[Event] = []
    seen_keys: set[tuple] = set()

    for page in range(0, 20):  # safety cap
        params_str = urlencode({
            "filters": filters,
            "hitsPerPage": hits_per_page,
            "page": page,
        })
        body = {"requests": [{"indexName": index, "params": params_str}]}
        try:
            r = sess.post(
                endpoint,
                params={
                    "x-algolia-api-key": api_key,
                    "x-algolia-application-id": app_id,
                },
                json=body,
                headers={
                    **DEFAULT_HEADERS,
                    "Content-Type": "application/json",
                    "Origin": origin,
                    "Referer": origin + "/",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("%s: algolia query page=%d failed: %s", venue_row["id"], page, exc)
            break
        results = data.get("results") or []
        if not results:
            break
        hits = results[0].get("hits") or []
        nb_pages = int(results[0].get("nbPages") or 1)
        if not hits:
            break
        for h in hits:
            ev = _algolia_hit_to_event(h, venue_row, url_prefix=url_prefix)
            if ev is None:
                continue
            key = (ev.title, ev.start, ev.venue_name)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(ev)
        if page + 1 >= nb_pages:
            break

    log.info("%s: %d events from algolia_calendar", venue_row["id"], len(out))
    return out


_ALGOLIA_PRIMARY_CATEGORY_MAP = {
    "Operas": "opera",
    "Opera": "opera",
    "Concerts & Recitals": "concert",
    "Concerts": "concert",
    "Symphony": "concert",
    "Chamber Music": "concert",
    "Ballet": "ballet",
    "Dance": "ballet",
    "Theatre": "theatre",
    "Theater": "theatre",
    "Plays": "theatre",
    "Musicals": "theatre",
    "Film": "film",
    "Screenings": "film",
    "Exhibition": "museum_exhibition",
    "Exhibitions": "museum_exhibition",
    "Talks & Lectures": "other",
    "Educational": "other",
    "Family": "other",
}


def _algolia_hit_to_event(h: dict, venue_row: dict, url_prefix: str = "") -> Optional[Event]:
    """Map one Algolia calendar hit to a canonical Event."""
    title = _clean_title(_html_decode(h.get("Title") or ""))
    if not title:
        return None
    start_ms = h.get("StartDate")
    end_ms = h.get("EndDate")
    if not isinstance(start_ms, (int, float)) or start_ms <= 0:
        # Some hits carry only PerformanceDates[] instead of StartDate.
        dates = h.get("PerformanceDates") or []
        if dates:
            start_ms = dates[0]
        else:
            return None
    start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc) if isinstance(end_ms, (int, float)) and end_ms > 0 else None

    venue_name = _clean_title(_html_decode(h.get("Venue") or "")) or (
        venue_row.get("display_name") or venue_row["name"]
    )
    rel_url = h.get("KenticoUrl") or ""
    if rel_url.startswith("/") and url_prefix:
        url = url_prefix.rstrip("/") + rel_url
    elif rel_url.startswith("http"):
        url = rel_url
    else:
        url = venue_row.get("homepage", "#")

    # Category resolution for Algolia hits:
    #   1. Algolia's PrimaryCategory (most reliable — the venue's own taxonomy)
    #   2. Title-keyword overlay (for opera/ballet productions)
    #   3. Venue-name override
    #   4. Row default
    primary_cat = h.get("PrimaryCategory") or ""
    algolia_cat = _ALGOLIA_PRIMARY_CATEGORY_MAP.get(primary_cat.strip())
    if algolia_cat:
        category = algolia_cat
    else:
        base_cat = venue_row.get("category", "other")
        if base_cat == "mixed":
            base_cat = "other"
        category = _infer_category(title, venue_row, stage_default=base_cat, venue_name=venue_name)

    return Event(
        title=title,
        start=start,
        end=end,
        venue_id=venue_row["id"],
        venue_name=venue_name,
        city=venue_row.get("city", ""),
        category=category,
        url=url,
        description=None,
        source=venue_row["id"],
        audience=_infer_audience(title),
    )


# ─── flat JSON feed path ────────────────────────────────────────────────────
# Many venues expose a single JSON endpoint that returns a flat array of
# event objects. LA Phil's /events/feed/live is the canonical example:
# 580 events spanning Hollywood Bowl + Walt Disney Concert Hall + The Ford,
# each with start_time / venue.name / site.name / program.name / absolute_url.
# This parser supports field-path mapping + optional filtering by a sub-venue
# field (so one URL can be split into 3 venue rows for the calendar's
# venue-chip filter).


def _dig(obj, path: str):
    """Read a dotted path from a nested dict (e.g. 'venue.name'). Returns None
    on missing key. List indexing not supported (not needed yet)."""
    if not path:
        return None
    cur = obj
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


# ─── Next.js + Contentful path ──────────────────────────────────────────────
# Sites built on Next.js + Contentful (Academy Museum of Motion Pictures is
# the canonical case) expose `_next/data/<buildId>/<route>.json` endpoints
# that mirror the page's getStaticProps payload. We:
#   1. Fetch the page HTML to discover the rotating buildId.
#   2. Hit the data endpoint, walk the JSON tree for the configured Contentful
#      __typename, and extract events from each match.
#   3. Flatten Contentful rich-text fields (programTitle is wrapped in nested
#      {json: {content: [...]}} arrays) to plain strings.


def _rich_text_to_plain(node) -> str:
    """Flatten a Contentful rich-text node tree to a plain string."""
    if not node:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        if node.get("nodeType") == "text":
            return node.get("value", "")
        if "json" in node:
            return _rich_text_to_plain(node["json"])
        if "content" in node:
            return "".join(_rich_text_to_plain(c) for c in node["content"])
    if isinstance(node, list):
        return "".join(_rich_text_to_plain(x) for x in node)
    return ""


def _scrape_nextjs_contentful(venue_row: dict, session=None) -> list[Event]:
    """Scrape a Next.js + Contentful events page via the _next/data/<buildId>/...
    JSON endpoint.

    Config:
      calendar_url: the HTML page that embeds the buildId (e.g. /en/calendar)
      data_path:    path suffix appended to /_next/data/<buildId>/ (e.g. /en/calendar.json)
      typename:     Contentful __typename to extract (default 'ProgramEvent')
      title_field:  Contentful rich-text field for the title (default 'programTitle')
      start_field:  ISO datetime field (default 'activeStartDate')
      end_field:    ISO datetime field (default 'activeEndDate'; falsy → no end)
      slug_field:   slug field for URL construction (default 'slug')
      url_pattern:  detail URL template, {slug} substituted (default homepage)
    """
    sess = session or requests
    page_url = venue_row["calendar_url"]
    try:
        r = sess.get(page_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as exc:
        log.warning("%s: nextjs page fetch failed: %s", venue_row["id"], exc)
        return []
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', r.text)
    if not m:
        log.warning("%s: nextjs buildId not found", venue_row["id"])
        return []
    build_id = m.group(1)

    data_path = venue_row.get("data_path") or "/en/calendar.json"
    host = "https://" + page_url.split("/")[2]
    data_url = f"{host}/_next/data/{build_id}{data_path}"
    try:
        r2 = sess.get(data_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT + 10)
        r2.raise_for_status()
        data = r2.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("%s: nextjs data fetch failed (%s): %s", venue_row["id"], data_url, exc)
        return []

    typename = venue_row.get("typename", "ProgramEvent")
    matches: list[dict] = []

    def _walk(obj, depth=0):
        if depth > 14 or obj is None:
            return
        if isinstance(obj, dict):
            if obj.get("__typename") == typename:
                matches.append(obj)
            for v in obj.values():
                _walk(v, depth + 1)
        elif isinstance(obj, list):
            for x in obj:
                _walk(x, depth + 1)

    _walk(data)

    title_field = venue_row.get("title_field", "programTitle")
    start_field = venue_row.get("start_field", "activeStartDate")
    end_field = venue_row.get("end_field", "activeEndDate")
    slug_field = venue_row.get("slug_field", "slug")
    url_pattern = venue_row.get("url_pattern") or venue_row.get("homepage", "#")

    out: list[Event] = []
    seen: set[tuple] = set()
    for m_item in matches:
        title = _clean_title(_rich_text_to_plain(m_item.get(title_field)))
        if not title:
            continue
        start = _parse_jsonld_dt(m_item.get(start_field))
        end = _parse_jsonld_dt(m_item.get(end_field)) if end_field else None
        if start is None:
            continue
        slug = m_item.get(slug_field) or ""
        if "{slug}" in url_pattern and slug:
            url = url_pattern.replace("{slug}", slug)
        else:
            url = venue_row.get("homepage", "#")
        category = venue_row.get("category", "other")
        if category == "mixed":
            category = "other"
        venue_name = venue_row.get("display_name") or venue_row["name"]
        key = (title, start)
        if key in seen:
            continue
        seen.add(key)
        out.append(Event(
            title=title,
            start=start,
            end=end,
            venue_id=venue_row["id"],
            venue_name=venue_name,
            city=venue_row.get("city", ""),
            category=category,
            url=url,
            description=None,
            source=venue_row["id"],
            audience=_infer_audience(title),
        ))

    log.info("%s: %d events from nextjs_contentful", venue_row["id"], len(out))
    return out


def _scrape_flat_json_feed(venue_row: dict, session=None) -> list[Event]:
    """Fetch + parse a flat JSON-array event feed.

    Config:
      calendar_url: URL returning a JSON array of event records
      title_path: dot-path to title (default 'title')
      start_path: dot-path to start time (ISO 8601 string; default 'start_time')
      end_path: dot-path to end time (optional)
      url_path: dot-path to detail URL (optional)
      venue_path: dot-path to venue name (optional)
      filter_field: dot-path to filter on (e.g. 'venue.name')
      filter_value: substring match against filter_field (case-insensitive)
      categories_path: dot-path to list of category objects (each {name: ...});
                       names get joined for category-keyword matching
    """
    sess = session or requests
    url = venue_row["calendar_url"]
    try:
        r = sess.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("%s: feed fetch failed: %s", venue_row["id"], exc)
        return []
    if not isinstance(data, list):
        # Sometimes the feed nests the array under a key.
        for key in ("events", "data", "results", "items"):
            if isinstance(data, dict) and isinstance(data.get(key), list):
                data = data[key]
                break
        else:
            log.warning("%s: feed response is not a list", venue_row["id"])
            return []

    title_path = venue_row.get("title_path", "title")
    start_path = venue_row.get("start_path", "start_time")
    end_path = venue_row.get("end_path")
    url_path = venue_row.get("url_path")
    venue_path = venue_row.get("venue_path")
    cats_path = venue_row.get("categories_path")
    filter_field = venue_row.get("filter_field")
    filter_value = (venue_row.get("filter_value") or "").lower()

    out: list[Event] = []
    seen_urls: set[str] = set()
    for raw in data:
        if not isinstance(raw, dict):
            continue
        if filter_field and filter_value:
            field_val = (_dig(raw, filter_field) or "")
            if filter_value not in str(field_val).lower():
                continue
        title = _clean_title(_html_decode(_dig(raw, title_path) or ""))
        if not title:
            continue
        start = _parse_jsonld_dt(_dig(raw, start_path))
        if start is None:
            continue
        end = _parse_jsonld_dt(_dig(raw, end_path)) if end_path else None
        url_val = _dig(raw, url_path) if url_path else None
        url_str = url_val if isinstance(url_val, str) else venue_row.get("homepage", "#")
        if url_str in seen_urls:
            continue
        seen_urls.add(url_str)

        venue_name = _clean_title(_html_decode(_dig(raw, venue_path) or "")) if venue_path else ""
        venue_name = venue_name or venue_row.get("display_name") or venue_row["name"]

        # Category — concatenate any tag/category names + title for keyword pass.
        category_hints = title
        if cats_path:
            cats = _dig(raw, cats_path) or []
            if isinstance(cats, list):
                names = [c.get("name") for c in cats if isinstance(c, dict) and c.get("name")]
                category_hints = title + " " + " ".join(names)
        base_cat = venue_row.get("category", "other")
        if base_cat == "mixed":
            base_cat = "other"
        category = _infer_category(category_hints, venue_row, stage_default=base_cat, venue_name=venue_name)

        out.append(Event(
            title=title,
            start=start,
            end=end,
            venue_id=venue_row["id"],
            venue_name=venue_name,
            city=venue_row.get("city", ""),
            category=category,
            url=url_str,
            description=None,
            source=venue_row["id"],
            audience=_infer_audience(title),
        ))

    log.info("%s: %d events from flat_json_feed", venue_row["id"], len(out))
    return out


def _parse_tribe_dt(s) -> Optional[datetime]:
    """Parse Tribe REST datetime ('YYYY-MM-DD HH:MM:SS', UTC if from utc_*)."""
    if not s or not isinstance(s, str):
        return None
    try:
        # Tribe utc_* fields are UTC; non-utc are local. We treat utc_* as UTC.
        dt = datetime.fromisoformat(s.replace(" ", "T"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# Marketing-ribbon suffixes Tribe sites bake into the title via
# <span class="orange-sm-caps">. Strip these — the title field should be
# the show name, not a status badge.
_TITLE_RIBBONS = re.compile(
    r"\s*(?:SELLING\s+FAST|SOLD\s+OUT|FEW\s+TICKETS\s+LEFT|"
    r"ON\s+SALE\s+NOW|JUST\s+ANNOUNCED|FINAL\s+WEEK|EXTENDED|NEW\s+DATE)\s*$",
    re.IGNORECASE,
)


def _html_decode(s: str) -> str:
    """Decode HTML entities + strip embedded tags + trailing marketing ribbons."""
    if not s:
        return ""
    s = _html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for _ in range(3):  # several ribbons can stack
        new = _TITLE_RIBBONS.sub("", s)
        if new == s:
            break
        s = new.strip()
    return s


def _coerce_to_dt(v) -> Optional[datetime]:
    """Coerce a YAML date/datetime/ISO-string into a tz-aware datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    from datetime import date as _date
    if isinstance(v, _date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


# ─── ical path ───────────────────────────────────────────────────────────────


def _scrape_ical(venue_row: dict, session=None) -> list[Event]:
    listing = venue_row["calendar_url"]
    pattern = venue_row.get("ical_pattern")
    # If no pattern is configured, treat calendar_url as a single .ics endpoint
    # (used by venues that publish ONE .ics with all events, e.g. Tribe Events
    # WordPress sites with `?ical=1` query). Skips the discovery step.
    if not pattern:
        ics_urls = [listing]
    else:
        ics_urls = parse_ical.discover_ics_urls(listing, pattern, session=session)

    # Build id → detail-URL map from the listing if a detail_pattern is configured.
    # This is how TUP Essen exposes its real event pages (the .ics URL points only
    # to the iCal endpoint; the human-readable page lives at a different path).
    detail_map = _build_detail_url_map(listing, venue_row, session=session)

    out: list[Event] = []
    for ics_url in ics_urls:
        try:
            raw_events = parse_ical.fetch_ics_events(ics_url, session=session)
        except requests.RequestException as exc:
            log.warning("  fetch %s failed: %s", ics_url, exc)
            continue
        for raw in raw_events:
            ev = _assemble_from_ical(raw, ics_url, venue_row, detail_map=detail_map)
            if ev is not None:
                out.append(ev)
    return out


def _build_detail_url_map(listing_url: str, venue_row: dict, session=None) -> dict[str, str]:
    """Fetch the listing page once and extract {event_id: detail_url} pairs."""
    detail_pattern = venue_row.get("detail_pattern")
    if not detail_pattern:
        return {}
    try:
        sess = session or requests
        resp = sess.get(listing_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("could not fetch listing for detail map: %s", exc)
        return {}
    base = listing_url
    out: dict[str, str] = {}
    for m in re.finditer(detail_pattern, resp.text):
        event_id = m.group("id") if "id" in m.groupdict() else m.group(1)
        if event_id and event_id not in out:
            out[event_id] = urljoin(base, m.group(0))
    log.debug("detail map: %d entries from %s", len(out), listing_url)
    return out


def _assemble_from_ical(raw: dict, ics_url: str, venue_row: dict, detail_map: dict | None = None) -> Optional[Event]:
    """Map one .ics VEVENT to a canonical Event, applying stage routing."""
    if not raw.get("title") or raw.get("start") is None:
        return None

    # Drop tour engagements outside the city (e.g. "Gastspiel" or
    # "Recklinghausen") — they're real productions but mum can't attend them
    # at this venue. Configured per-venue via skip_if_location_matches.
    location_raw = raw.get("location") or ""
    for pat in venue_row.get("skip_if_location_matches") or []:
        if re.search(pat, location_raw):
            return None

    title = _clean_title(raw["title"])

    venue_id, venue_name, city, stage_default_category = _resolve_stage(
        location=raw.get("location") or "",
        title=title,
        venue_row=venue_row,
    )

    # URL resolution priority:
    #   1. .ics URL field (rarely populated)
    #   2. detail_map lookup by event_id (the right answer for TUP)
    #   3. fall back to the calendar listing
    detail_url = raw.get("url") or _detail_url_from_map(ics_url, detail_map) \
        or venue_row["calendar_url"]

    return Event(
        title=title,
        start=raw["start"],
        end=raw.get("end"),
        venue_id=venue_id,
        venue_name=venue_name,
        city=city,
        category=_infer_category(title, venue_row, stage_default=stage_default_category),
        url=detail_url,
        description=_clean_title(raw.get("description") or "") or None,
        price=None,
        source=venue_row["id"],
        audience=_infer_audience(title),
    )


def _clean_title(s: str) -> str:
    """Strip soft hyphens, collapse newlines and runs of whitespace."""
    if not s:
        return s
    # Remove SHY (soft hyphen) and other invisibles that break display
    s = s.replace("­", "").replace("​", "").replace("﻿", "")
    # iCal SUMMARY can have embedded newlines (e.g. conductor / orchestra); join with bullet
    s = re.sub(r"[\r\n]+", " · ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _detail_url_from_map(ics_url: str, detail_map: dict | None) -> str:
    """Look up the human-readable detail URL by extracting the event ID from the .ics URL."""
    if not detail_map:
        return ""
    m = re.search(r"/(\d+)/ical-", ics_url)
    if not m:
        return ""
    return detail_map.get(m.group(1), "")


# ─── stage routing ───────────────────────────────────────────────────────────


def _resolve_stage(location: str, title: str, venue_row: dict) -> tuple[str, str, str, Optional[str]]:
    """Map an event's location string to (venue_id, venue_name, city, default_category).

    `default_category` may be None — caller falls back to keyword inference or
    the venue's base category.

    Each resolver rule:
        - match: "Aalto"           # str OR list[str]; substring, case-insensitive
          venue_id: aalto-essen
          venue_name: "Aalto-Theater"
          city: Essen
          default_category: opera  # optional fallback when title keywords miss
    """
    resolvers = venue_row.get("stage_resolver") or []
    haystack = f"{location} {title}".lower()
    for rule in resolvers:
        patterns = rule["match"]
        if isinstance(patterns, str):
            patterns = [patterns]
        if any(p.lower() in haystack for p in patterns):
            # Prefer the actual LOCATION string for display — it's more specific
            # ("NATIONAL-BANK Pavillon" beats the rule-name "Philharmonie Essen").
            # The rule's venue_name is only a fallback when location is empty.
            display_name = location if location else rule.get("venue_name", venue_row["name"])
            return (
                rule["venue_id"],
                display_name,
                rule.get("city", venue_row["city"]),
                rule.get("default_category"),
            )

    # Use the location string as the display name when present (even short
    # ones like "Box" are informative). Else fall back to the venue's short
    # display name from `display_name`, or `name` if no short form is set.
    fallback_name = location if location else (venue_row.get("display_name") or venue_row["name"])
    return (venue_row["id"], fallback_name, venue_row["city"], None)


# ─── html_list path ──────────────────────────────────────────────────────────


def _scrape_html_list(venue_row: dict, session=None) -> list[Event]:
    """Fetch the listing page(s) and assemble events from each item.

    Supports multi-month pagination (`paginate_months: N` + `paginate_url_param`)
    for venues that show one month at a time and need explicit URL stepping.
    Each fetched page may carry its own month context (for venues like Theater
    Münster where the date day-number per item is paired with a page-level
    month name).
    """
    sess = session or requests
    sel = venue_row.get("selectors") or {}
    listing_url = venue_row["calendar_url"]

    item_sel = sel.get("item")
    if not item_sel:
        log.warning("%s: html_list missing selectors.item", venue_row["id"])
        return []

    # Build the list of URLs to fetch. Defaults to single calendar_url; if
    # paginate_months is set, append `?date=YYYY-MM` for the current month
    # plus N-1 future months.
    urls = _paginated_urls(venue_row)

    out: list[Event] = []
    for url, ctx_year in urls:
        try:
            resp = sess.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("  %s: page fetch failed (%s): %s", venue_row["id"], url, exc)
            continue
        # Pass bytes (not resp.text) so BS4 detects the charset from the document
        soup = BeautifulSoup(resp.content, "html.parser")

        # Page-level month context (Theater Münster, Wolfgang-Borchert-Theater).
        # Combined with per-item day-number to produce a full date.
        month_ctx = None
        mc_sel = venue_row.get("month_context_selector")
        if mc_sel:
            mc_el = soup.select_one(mc_sel)
            if mc_el:
                month_ctx = mc_el.get_text(" ", strip=True)

        items = soup.select(item_sel)
        log.debug("%s [%s]: matched %d items", venue_row["id"], url[-30:], len(items))

        prev_day = None  # for date_day_carry_forward
        for it in items:
            ev, prev_day = _assemble_from_html_item(
                it, url, venue_row,
                month_ctx=month_ctx,
                ctx_year=ctx_year,
                prev_day=prev_day,
            )
            if ev is not None:
                out.append(ev)
    return out


def _paginated_urls(venue_row: dict) -> list[tuple[str, Optional[int]]]:
    """Return [(url, year_for_that_url), ...] — one entry per month for
    paginated venues, or just [(calendar_url, None)] for non-paginated."""
    base = venue_row["calendar_url"]
    months = venue_row.get("paginate_months")
    if not months:
        return [(base, None)]
    param = venue_row.get("paginate_url_param", "date")
    out: list[tuple[str, Optional[int]]] = []
    today = datetime.now(timezone.utc).date()
    y, m = today.year, today.month
    sep = "&" if "?" in base else "?"
    for _ in range(int(months)):
        url = f"{base}{sep}{param}={y:04d}-{m:02d}"
        out.append((url, y))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _assemble_from_html_item(
    item, listing_url: str, venue_row: dict,
    month_ctx: Optional[str] = None,
    ctx_year: Optional[int] = None,
    prev_day: Optional[str] = None,
) -> tuple[Optional[Event], Optional[str]]:
    """Build a single Event from one selector match.

    Returns (event_or_None, day_carry_forward_value) — the second value is
    used by the caller to remember the last seen day-number for venues where
    continuation rows have an empty day cell (Theater Münster).
    """
    sel = venue_row["selectors"]
    new_prev_day = prev_day  # default: pass through unchanged

    # When the date is embedded in the title block, preserve line breaks so we can
    # split off venue/location lines after extracting the date.
    title_separator = "\n" if venue_row.get("date_from_title") else " "
    raw_title = _select_text(item, sel.get("title"), separator=title_separator)
    if not raw_title:
        return None, new_prev_day

    # Optional skip filter — drop non-event entries like "Museum closed" cards
    for pat in venue_row.get("skip_if_title_matches") or []:
        if re.search(pat, raw_title):
            return None, new_prev_day

    # Optional title cleanup: strip suffixes (literal) and regex patterns
    title = _clean_title(raw_title)
    for suffix in venue_row.get("title_strip_suffixes") or []:
        title = re.sub(re.escape(suffix), "", title, flags=re.IGNORECASE).strip()
    for pattern in venue_row.get("title_strip_regex") or []:
        title = re.sub(pattern, "", title).strip(" -–—,.\n\t")

    # Resolve dates. Modes (first matching wins):
    #   0. month_context: page-level month + per-item day (Theater Münster, WBT)
    #   1. date_start + date_end selectors (Folkwang-style)
    #   2. date selector (Red Dot / Ruhr Museum extracts)
    #   3. date_from_title regex (Ruhr Museum exhibitions)
    start = end = None
    date_text = ""
    if month_ctx and sel.get("date_day"):
        # Construct date from day-number + page-level month + inferred year
        day_t = _select_text(item, sel.get("date_day")).strip(" .,")
        if not day_t and venue_row.get("date_day_carry_forward"):
            day_t = prev_day or ""
        if day_t:
            new_prev_day = day_t
            time_t = _select_text(item, sel.get("date_time")) if sel.get("date_time") else ""
            yr = ctx_year or datetime.now(timezone.utc).year
            date_text = f"{day_t}. {month_ctx} {yr} {time_t}".strip()
            start = _parse_one(date_text, venue_row.get("date_format"))
    if start is not None:
        pass  # month_context mode already produced a start
    elif sel.get("date_start") or sel.get("date_end"):
        start_t = _select_text(item, sel.get("date_start"))
        end_t = _select_text(item, sel.get("date_end"))
        if start_t:
            start = _parse_one(start_t, venue_row.get("date_format"))
        if end_t:
            end = _parse_one(end_t, venue_row.get("date_format"))
        date_text = f"{start_t} – {end_t}".strip(" –")
    elif venue_row.get("date_from_title"):
        mode = venue_row.get("date_from_title_mode", "single")
        # Operate on the already-cleaned title (SHY chars + collapsed whitespace
        # gone) so the strip leaves a clean display string.
        haystack = title
        if mode == "range":
            # Two capture groups: (start, end). Year on start may be missing —
            # inferred from end. Used by Kunstpalast: "DIE GROSSE 5.7.–9.8.2026".
            pattern = venue_row.get(
                "date_from_title_pattern",
                r"(\d{1,2}\.\d{1,2}\.(?:\d{4})?)\s*[–—-]\s*(\d{1,2}\.\d{1,2}\.\d{4})",
            )
            m = re.search(pattern, haystack)
            if m and m.lastindex and m.lastindex >= 2:
                date_text = m.group(0)
                title = re.sub(re.escape(date_text), "", haystack).strip(" -–—,.\n\t")
                start_str, end_str = m.group(1), m.group(2)
                if not re.search(r"\d{4}", start_str):
                    yr = re.search(r"(\d{4})", end_str)
                    if yr:
                        start_str = start_str + yr.group(1)
                start = _parse_one(start_str, venue_row.get("date_format"))
                end = _parse_one(end_str, venue_row.get("date_format"))
        else:
            date_pattern = venue_row.get(
                "date_from_title_pattern",
                # Default: "Bis 10. Januar 2027" / "ab 10. Mai 2026" / "10.01.2027"
                r"(?:Bis|bis|Ab|ab|Vom|vom|Noch bis|noch bis)\s*\d{1,2}\.\s*\w+\s*\d{4}|\d{1,2}\.\d{1,2}\.\d{4}",
            )
            m = re.search(date_pattern, haystack)
            if m:
                date_text = m.group(0)
                title = re.sub(re.escape(date_text), "", haystack).strip(" -–—,.\n\t")
            if date_text:
                if re.match(r"^(?:Bis|bis|Noch bis|noch bis)\b", date_text):
                    end = _parse_one(date_text, venue_row.get("date_format"))
                    today = datetime.now(timezone.utc)
                    start = today.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    start = _parse_one(date_text, venue_row.get("date_format"))
    else:
        date_text = _select_text(item, sel.get("date"))
        # Optional pre-extract: pull a clean date substring out of a noisy
        # text blob. Used by Ruhr Museum where the date selector returns
        # "Margarethenhöhe... Sonntag 10.5. 11:00 - 13:00" — regex pulls just
        # the day+time portion before dateparser sees it.
        extract_re = venue_row.get("date_extract_regex")
        if extract_re and date_text:
            if venue_row.get("date_find_all"):
                # Find every match; first = start, last = end. Used for venues
                # that show "DATE_A bis DATE_B - verlängert bis DATE_C" — we
                # want DATE_A → DATE_C, not DATE_A → DATE_B.
                matches = re.findall(extract_re, date_text)
                if matches:
                    date_text = matches[0] if len(matches) == 1 else f"{matches[0]} – {matches[-1]}"
            else:
                m = re.search(extract_re, date_text)
                if m:
                    date_text = m.group(0)
        start, end = _parse_date_range(date_text, venue_row.get("date_format"))

    if start is None:
        log.debug("%s: failed to parse date %r (raw_title=%r)", venue_row["id"], date_text, raw_title)
        return None, new_prev_day

    # Title cleanup pass 2: take first non-empty line, in case selector pulled a multiline blob
    if "\n" in title or "  " in title:
        first_line = next((ln.strip() for ln in re.split(r"\n|\s{2,}", title) if ln.strip()), title)
        if first_line:
            title = first_line

    detail_link = _select_attr(item, sel.get("detail_link"), "href")
    detail_url = urljoin(listing_url, detail_link) if detail_link else listing_url

    description = _select_text(item, sel.get("description")) or None

    venue_id, venue_name, city, stage_default_category = _resolve_stage(
        location="", title=title, venue_row=venue_row
    )

    category = _infer_category(title, venue_row, stage_default=stage_default_category)

    return Event(
        title=title,
        start=start,
        end=end,
        venue_id=venue_id,
        venue_name=venue_name,
        city=city,
        category=category,
        url=detail_url,
        description=description,
        price=None,
        source=venue_row["id"],
        audience=_infer_audience(title),
    ), new_prev_day


def _select_text(node, selector: Optional[str], separator: str = " ") -> str:
    """Read text (or an attribute) from a CSS-selected element.

    The `@attr` suffix on a selector reads the attribute instead of the text,
    e.g. `meta[itemprop='startDate']@content` returns the meta tag's content.
    A selector starting with `@` (no element selector before it) reads the
    attribute from the *item* element itself — used by Ruhrfestspiele where
    the date list is on `<article ... data-days='["2026-05-04",...]'>`.
    """
    if not selector:
        return ""
    if selector.startswith("@"):
        return (node.get(selector[1:].strip()) or "")
    if "@" in selector:
        sel, attr = selector.rsplit("@", 1)
        el = node.select_one(sel.strip())
        return (el.get(attr.strip()) or "") if el is not None else ""
    el = node.select_one(selector)
    if el is None:
        return ""
    text = el.get_text(separator, strip=True)
    if separator == "\n":
        lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
        return "\n".join(ln for ln in lines if ln)
    return " ".join(text.split())


def _select_attr(node, selector: Optional[str], attr: str) -> str:
    """Read an attribute from a CSS-selected element.

    Supports the same `@attr` and `@attr-on-item` shortcuts as _select_text:
        - "h2 a"          → select h2 a, read attr `attr`
        - "h2 a@href"     → select h2 a, read attr `href` (overrides default)
        - "@href"         → read attr `href` from the item element itself
    """
    if not selector:
        return ""
    if selector.startswith("@"):
        return (node.get(selector[1:].strip()) or "").strip()
    if "@" in selector:
        sel, override_attr = selector.rsplit("@", 1)
        el = node.select_one(sel.strip())
        return (el.get(override_attr.strip()) or "").strip() if el is not None else ""
    el = node.select_one(selector)
    if el is None:
        return ""
    return (el.get(attr) or "").strip()


# ─── date parsing ────────────────────────────────────────────────────────────


_DATE_PARSER_KW = dict(
    languages=["en"],
    settings={
        "PREFER_DATES_FROM": "future",
        # US convention is MM/DD/YYYY. Opposite of the German project's DMY.
        # Note: dateparser tries to detect format from input first; explicit
        # MDY just disambiguates "5/12/2026" → May 12, not Dec 5.
        "DATE_ORDER": "MDY",
    },
)


def _parse_date_range(text: str, explicit_format: Optional[str] = None) -> tuple[Optional[datetime], Optional[datetime]]:
    """Parse a German date string, possibly a range, into (start, end).

    Handles patterns like:
        "21. Juni 2026"
        "Sa, 14.05.2026 19:30"
        "14.05.–30.06.2026"      (range — exhibition run)
        "14.05.2026 — 30.06.2026"
        "ab 18.05.2026"
        "noch bis 17.08.2026"

    Returns tz-aware datetimes (Europe/Berlin → UTC). end is None for single-day events.
    """
    if not text:
        return (None, None)
    text = text.strip()

    # Range patterns — try a few separators
    for sep in [" – ", " — ", " - ", "–", "—", " bis ", " – bis "]:
        if sep in text:
            left, right = text.split(sep, 1)
            l = _parse_one(left.strip(), explicit_format)
            r = _parse_one(right.strip(), explicit_format)
            if l and r:
                return (l, r)

    # Compact range "14.05.–30.06.2026" (no spaces, en-dash with year only on right)
    m = re.match(r"^(\d{1,2}\.\d{1,2}\.)[–—-](\d{1,2}\.\d{1,2}\.\d{4})$", text)
    if m:
        l_str = m.group(1) + m.group(2).split(".")[-1]   # tack year on
        l = _parse_one(l_str, explicit_format)
        r = _parse_one(m.group(2), explicit_format)
        if l and r:
            return (l, r)

    single = _parse_one(text, explicit_format)
    return (single, None)


def _parse_one(text: str, explicit_format: Optional[str] = None) -> Optional[datetime]:
    text = text.strip().rstrip(".")
    if not text:
        return None

    if explicit_format:
        try:
            dt = datetime.strptime(text, explicit_format)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    # ISO 8601 first. dateparser with DATE_ORDER='DMY' (set globally for German
    # numeric dates) actively rejects ISO format, so any "2026-05-23" coming
    # from a <time datetime="..."> attribute would silently fail otherwise.
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass

    # Strip leading English prepositions that confuse dateparser
    for prefix in ("through ", "thru ", "until ", "starting ", "from ", "ends "):
        if text.lower().startswith(prefix):
            text = text[len(prefix):]

    parsed = dateparser.parse(text, **_DATE_PARSER_KW)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


# ─── category inference ──────────────────────────────────────────────────────


_CATEGORY_KEYWORDS = [
    ("vernissage", ["opening reception", "opening night", "vernissage"]),
    # Opera rep — extended 2026-05-11 to match momEvents' German + rep-heavy
    # list. Word-boundary match enforced in _infer_category so e.g. "Aida"
    # doesn't accidentally trigger inside "Saida" or similar.
    ("opera", [
        "opera", "operetta",
        # Verdi
        "falstaff", "aida", "rigoletto", "otello", "la traviata",
        "il trovatore", "nabucco", "don carlos", "macbeth",
        "simon boccanegra", "un ballo in maschera", "la forza del destino",
        # Puccini
        "tosca", "la bohème", "la boheme", "madama butterfly", "turandot",
        "gianni schicchi", "manon lescaut",
        # Mozart
        "magic flute", "zauberflöte",
        "don giovanni", "cosi fan tutte", "così fan tutte",
        "marriage of figaro", "le nozze di figaro", "le nozze",
        "idomeneo", "la clemenza di tito",
        # Wagner — full Ring cycle + others
        "die walküre", "rheingold", "siegfried",
        "götterdämmerung", "ring des nibelungen",
        "tannhäuser", "lohengrin", "parsifal",
        "tristan und isolde", "fliegender holländer", "die meistersinger",
        # R. Strauss
        "der rosenkavalier", "rosenkavalier", "salome", "elektra",
        "ariadne auf naxos", "capriccio", "arabella",
        "die frau ohne schatten",
        # J. Strauss / Lehár (operettas)
        "die fledermaus", "fledermaus", "eine nacht in venedig",
        "der zigeunerbaron", "wiener blut",
        "lustige witwe", "land des lächelns",
        # Beethoven / Weber / Humperdinck
        "fidelio", "der freischütz",
        "hänsel und gretel", "königskinder",
        # Bizet / Rossini / Donizetti / Bellini
        "carmen", "les pêcheurs de perles",
        "barbiere di siviglia", "barber of seville",
        "elisir d'amore", "elixir of love", "lucia di lammermoor",
        "don pasquale", "fille du régiment", "norma",
        # Tchaikovsky / Mussorgsky / Borodin
        "eugen onegin", "eugene onegin",
        "pique dame", "pikowaja dama",
        "boris godunov", "prince igor",
        # Massenet / Gounod / Offenbach
        "werther", "manon", "faust",
        "contes d'hoffmann", "hoffmanns erzählungen",
        "orpheus in der unterwelt", "orphée aux enfers",
        "belle hélène", "schöne helena",
        # Janáček / Berg / Britten / Bartók
        "jenufa", "jenůfa", "katja kabanowa",
        "sache makropulos", "wozzeck", "lulu",
        "peter grimes", "billy budd", "death in venice",
        "midsummer night's dream",
        "herzog blaubarts burg", "bluebeard",
        # Misc rep
        "die verkaufte braut", "bartered bride",
        "rake's progress",
        "cavalleria rusticana", "pagliacci",
    ]),
    ("ballet", [
        "ballet", "ballett", "tanztheater",
        "swan lake", "schwanensee",
        "nutcracker", "nussknacker",
        "sleeping beauty", "dornröschen",
        "giselle", "coppelia", "coppélia",
        "don quixote", "don quichotte",
        "la sylphide", "la bayadère", "la bayadere",
        "petrushka", "petruschka",
        "rite of spring", "le sacre du printemps", "frühlingsopfer",
        "raymonda", "sylvia", "spartacus", "spartakus",
        "feuervogel", "firebird",
        "daphnis und chloé", "daphnis et chloé",
        "boléro", "bolero",
        "tanzhommage",
    ]),
    ("concert", [
        "symphony", "concerto", "philharmonic", "chamber music",
        "recital", "lieder", "orchestra", "string quartet",
        "jazz", "trio", "quintet",
    ]),
    ("theatre", ["play", "drama", "monologue", "premiere"]),
    ("film", ["screening", "film festival", "70mm", "imax", "double feature"]),
    ("museum_exhibition", ["exhibition", "exhibit", "retrospective"]),
]


# Famous musicals — titles literally contain "opera"/"ballet" but they're
# musical theatre, not opera/ballet. Tag them as theatre.
_MUSICAL_THEATRE_TITLES = (
    "phantom of the opera", "phantom der oper",
    "les misérables", "les miserables",
    "miss saigon",
    "evita",
    "jesus christ superstar",
    "tanz der vampire",
    "rebecca",
    "the lion king", "der könig der löwen",
    "wicked",
    "hamilton",
    "hadestown",
    "cabaret",
    "chicago",
    "rent",
    "company",  # Sondheim
    "into the woods",
    "sweeney todd",
    "starlight express",
    "anatevka", "fiddler on the roof",
    "mamma mia",
    "beauty and the beast", "die schöne und das biest",
    "tarzan",
    "rocky horror",
    "we will rock you",
)


# Venue-name → category overrides. Catches LA Opera productions at Dorothy
# Chandler Pavilion (venue name doesn't say "opera"), American Contemporary
# Ballet, etc. Matched case-insensitively as substring.
_VENUE_NAME_CATEGORY: list[tuple[str, str]] = [
    # opera
    ("dorothy chandler pavilion", "opera"),  # LA Opera's home
    ("la opera", "opera"),
    ("los angeles opera", "opera"),
    ("pacific opera project", "opera"),
    ("long beach opera", "opera"),
    # ballet
    ("american contemporary ballet", "ballet"),
    ("los angeles ballet", "ballet"),
    # famous LA theatres — Discover LA tags these as "Music" or "Other";
    # the venue itself tells us they're theatre programming.
    ("pantages theatre", "theatre"),
    ("ahmanson theatre", "theatre"),
    ("mark taper forum", "theatre"),
    ("kirk douglas theatre", "theatre"),
    ("geffen playhouse", "theatre"),
    ("pasadena playhouse", "theatre"),
    ("boston court", "theatre"),
    ("a noise within", "theatre"),
    ("latino theater company", "theatre"),
    ("los angeles theatre center", "theatre"),
    ("santa monica playhouse", "theatre"),
    ("east west players", "theatre"),
    ("rogue machine", "theatre"),
    ("antaeus theatre", "theatre"),
    ("skylight theatre", "theatre"),
    # famous LA concert venues
    ("hollywood bowl", "concert"),
    ("walt disney concert hall", "concert"),  # default; opera/ballet titles still overlay first
    ("greek theatre", "concert"),
    ("the wiltern", "concert"),
    ("wiltern theatre", "concert"),
    ("hollywood palladium", "concert"),
    # museums
    ("lacma", "museum_exhibition"),
    ("the broad", "museum_exhibition"),
    ("hammer museum", "museum_exhibition"),
    ("getty center", "museum_exhibition"),
    ("getty villa", "museum_exhibition"),
    ("norton simon", "museum_exhibition"),
    ("huntington library", "museum_exhibition"),
    ("academy museum", "museum_exhibition"),
    ("autry museum", "museum_exhibition"),
    ("japanese american national museum", "museum_exhibition"),
    ("natural history museum", "museum_exhibition"),
    ("la brea tar pits", "museum_exhibition"),
    # cinemas
    ("american cinematheque", "film"),
    ("egyptian theatre", "film"),
    ("aero theatre", "film"),
    ("vidiots", "film"),
    ("new beverly cinema", "film"),
]


# Title-substring deny list. Discover LA includes singles parties, pet
# adoptions, summer camps, brunch cruises, etc. — not cultural events.
# Matched case-insensitively as substring on the title.
_TITLE_DENY_KEYWORDS = (
    "singles party",
    "singles night",
    "singles social",
    "swipe right",
    "speed dating",
    "speed-dating",
    "pet adoption",
    "summer camp",
    "summer theatre camp",
    "spring camp",
    "winter camp",
    "kids camp",
    "brunch cruise",
    "dinner cruise",
    "happy hour",
    "wine tasting",
    "beer tasting",
    "yoga class",
    "boot camp",
    "trivia night",
    "trivia ",
    "bingo night",
    "karaoke",
    "open mic",
    "free workshop",
    "career fair",
    "job fair",
    "book signing",  # not strictly junk; high-volume + low cultural signal
    "story time",
    "story hour",
    "storytime",
)


# ─── audience inference ──────────────────────────────────────────────────────
# Title keyword markers for events that should be visually de-emphasized
# or hidden behind the "Also show classes & family programmes" toggle.

_AUDIENCE_KEYWORDS = {
    # ORDER MATTERS — first match wins. Hide-by-default classes (kids, active)
    # are listed before the dim-only "educational" class so a "Family Workshop"
    # gets classified as kids (hidden) rather than educational (dimmed).

    # Hidden by default; revealed by the toggle.
    "kids": [
        "kids",
        "children",
        "for children",
        "for kids",
        "family workshop",
        "family event",
        "family-friendly",
        "story time",
        "story-time",
        "youth",
        "teen",
        "school program",
        "spring break camp",
        "summer camp",
        "winter camp",
        "preschool",
        "babies",
        "toddler",
        "ages 5",
        "ages 6",
        "ages 7",
        "ages 8",
        "kinder",
    ],
    # Hidden by default; revealed by the same toggle. Hands-on / participatory.
    "active": [
        "workshop",
        "class:",
        "course:",
        "drop-in art",
        "drop-in studio",
        "art making",
        "make-and-take",
        "yoga",
        "meditation session",
        "open studio",
        "drawing session",
        "painting session",
        "kids workshop",
    ],
    # Visible but dimmed — passive things the user might enjoy (tours, lectures, behind-the-scenes).
    "educational": [
        "guided tour",
        "docent tour",
        "members' tour",
        "behind the scenes",
        "behind-the-scenes",
        "introduction to",
        "lecture:",
        "in conversation",
        "panel discussion",
        "artist talk",
        "curator talk",
        "open rehearsal",
        "preview",
    ],
}


def _infer_audience(title: str) -> str:
    t = title.lower()
    for cls, kws in _AUDIENCE_KEYWORDS.items():
        for kw in kws:
            if kw in t:
                return cls
    return "general"


def _infer_category(title: str, venue_row: dict, stage_default: Optional[str] = None,
                    venue_name: Optional[str] = None) -> str:
    """Resolve event category. Priority:

    1. per-venue category_keyword_overrides
    2. famous-musical override (Phantom of the Opera → theatre, not opera)
    3. global title keyword match — word-boundary on opera/ballet so
       "Operation" / "operational" don't false-positive
    4. venue-name override (e.g. Dorothy Chandler Pavilion → opera) — only
       fires when title gave us nothing more specific; this matters because
       "New York City Ballet AT Dorothy Chandler" must stay ballet, not opera
    5. stage_default from stage_resolver rule
    6. venue's base category if not 'mixed'
    7. 'other'
    """
    t = title.lower()
    overrides = venue_row.get("category_keyword_overrides") or {}
    for cat, kws in overrides.items():
        if any(kw.lower() in t for kw in (kws or [])):
            return cat
    # 2. Famous musicals — title-contains check that beats opera keyword.
    if any(m in t for m in _MUSICAL_THEATRE_TITLES):
        return "theatre"
    # 3. Title keyword match.
    for cat, kws in _CATEGORY_KEYWORDS:
        for kw in kws:
            # opera/ballet → word-boundary regex; everything else stays substring
            # (multi-word phrases like "swan lake" handle their own boundaries).
            if cat in ("opera", "ballet") and " " not in kw:
                if re.search(rf"\b{re.escape(kw)}\b", t):
                    return cat
            elif kw in t:
                return cat
    # 4. Venue-name override.
    if venue_name:
        vn = venue_name.lower()
        for needle, cat in _VENUE_NAME_CATEGORY:
            if needle in vn:
                return cat
    if stage_default:
        return stage_default
    base = (venue_row.get("category") or "").lower()
    if base and base != "mixed":
        return base
    return "other"


# ─── helpers for orchestrator ────────────────────────────────────────────────


def load_venues(path: str | Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─── CLI smoke test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--venue-id", required=True)
    p.add_argument("--venues-path", default="projects/momEvents/config/venues.yaml")
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args()

    venues = load_venues(args.venues_path)
    row = next((v for v in venues if v["id"] == args.venue_id), None)
    if row is None:
        print(f"venue id {args.venue_id!r} not in {args.venues_path}")
        sys.exit(1)

    events = scrape(row)
    print(f"\n{len(events)} events for {row['id']}\n")
    for ev in events[:args.limit]:
        print(f"  {ev.start!s:30s} | {ev.venue_id:22s} | {ev.category:18s} | {ev.title[:60]}")
