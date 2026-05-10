"""Orchestrator: scrape every venue, mark highlights, render events.html.

Run from events-la repo root:
    python tools/rebuild_calendar.py
        --venues       config/venues.yaml
        --highlights   config/highlights.yaml
        --out          .tmp/events.html

Per-venue scrape failure is logged and counted, NOT raised. The run fails (exit 2)
only if more than 5 venues fail.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from rapidfuzz import fuzz

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

import render_events_html  # noqa: E402
import scrape_venue_events  # noqa: E402

log = logging.getLogger("rebuild_calendar")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--venues", default=str(REPO_ROOT / "config/venues.yaml"))
    p.add_argument("--highlights", default=str(REPO_ROOT / "config/highlights.yaml"))
    p.add_argument("--out", default=str(REPO_ROOT / ".tmp/events.html"))
    p.add_argument("--only-city", default=None, help="Limit to one city/neighborhood (e.g. 'Pasadena')")
    p.add_argument("--horizon-days", type=int, default=270)
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )
    log.setLevel(logging.INFO)

    venues = scrape_venue_events.load_venues(args.venues)
    highlights = _load_highlights(args.highlights)

    if args.only_city:
        venues = [v for v in venues if v.get("city") == args.only_city]

    # Reuse one HTTP session across all venues for connection pooling
    session = requests.Session()
    session.headers.update(scrape_venue_events.DEFAULT_HEADERS)

    all_events = []
    failed: list[tuple[str, str]] = []
    ok = 0
    skipped = 0
    for v in venues:
        if v.get("kind") == "unknown":
            skipped += 1
            continue
        try:
            evs = scrape_venue_events.scrape(v, session=session)
        except Exception as exc:  # belt-and-braces; scrape() already isolates
            log.error("scrape() raised for %s: %s", v["id"], exc)
            failed.append((v["id"], str(exc)))
            continue
        if not evs:
            failed.append((v["id"], "no events returned"))
        else:
            all_events.extend(evs)
            ok += 1

    log.info("scraped %d venues OK, %d failed, %d skipped (kind=unknown)", ok, len(failed), skipped)
    log.info("raw events: %d", len(all_events))

    # Mark featured events using highlights config
    featured = _compute_featured_set(all_events, highlights)
    log.info("flagged %d events as featured", len(featured))

    # Render
    n = render_events_html.render(
        events=all_events,
        out_path=args.out,
        featured=featured,
        title="What's On in LA",
        horizon_days=args.horizon_days,
        now=datetime.now(timezone.utc),
    )

    # Summary report
    print()
    print(f"Run {datetime.now(timezone.utc).isoformat(timespec='seconds')} — "
          f"{ok+len(failed)} venues, {ok} OK, {len(failed)} failed, {skipped} skipped")
    if failed:
        print("  Failed venues:")
        for vid, reason in failed:
            print(f"    - {vid}: {reason}")
    print(f"  Events: {len(all_events)} raw → {n} after future-only/horizon filter")
    print(f"  Featured: {len(featured)}")
    print(f"  Output: {args.out}")

    if len(failed) > 5:
        log.error("too many failed venues: %d", len(failed))
        return 2
    return 0


# ─── highlights logic ────────────────────────────────────────────────────────


def _load_highlights(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {"featured_keywords": [], "featured_events": []}
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {
        "featured_keywords": data.get("featured_keywords") or [],
        "featured_events": data.get("featured_events") or [],
    }


def _compute_featured_set(events: list, highlights: dict) -> set:
    """Return a set of (venue_id, normalized_title) for events that match.

    Two paths:
        1. title contains any string from featured_keywords (case-insensitive substring)
        2. matches a featured_events entry: same venue_id + token_set_ratio >= 85
    """
    keywords = [kw.lower() for kw in highlights.get("featured_keywords", [])]
    manual = highlights.get("featured_events", [])

    today = datetime.now(timezone.utc).date()
    out: set = set()

    for ev in events:
        if isinstance(ev, dict):
            title = ev.get("title", "")
            venue_id = ev.get("venue_id", "")
            description = ev.get("description") or ""
            audience = ev.get("audience") or "general"
        else:
            title = getattr(ev, "title", "") or ""
            venue_id = getattr(ev, "venue_id", "") or ""
            description = getattr(ev, "description", "") or ""
            audience = getattr(ev, "audience", "general") or "general"

        # Don't pin kids/educational events as highlights, even if they keyword-match
        if audience != "general":
            continue

        haystack = f"{title} {description}".lower()

        matched = False
        for kw in keywords:
            if kw in haystack:
                matched = True
                break

        if not matched:
            for entry in manual:
                if entry.get("venue_id") != venue_id:
                    continue
                until = entry.get("until")
                if until and _coerce_date(until) < today:
                    continue
                if fuzz.token_set_ratio(title, entry.get("title_match", "")) >= 85:
                    matched = True
                    break

        if matched:
            out.add(render_events_html._featured_key(ev))

    return out


def _coerce_date(v):
    if isinstance(v, datetime):
        return v.date()
    return v


if __name__ == "__main__":
    sys.exit(main())
