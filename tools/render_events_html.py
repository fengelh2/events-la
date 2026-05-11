"""Render a list of canonical events to a clean, mobile-friendly HTML page.

Layout philosophy (see projects/momEvents/CLAUDE.md and the agreed mockup):
    - Pinned "Nicht verpassen" section at the top (max 4 cards, sorted by
      closing-date-soonest-first — creates urgency).
    - Below that, a chronological agenda grouped by ISO week.
    - Each event row shows: date stamp on the left rail; title + venue · city +
      category · relative countdown on the right.
    - Featured events also keep a ★ inline so they aren't lost in the agenda.

No JavaScript. System fonts. Designed to read well on a phone for a 60+ user.
"""

from __future__ import annotations

import html
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
WEEKDAYS_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

CATEGORY_LABELS = {
    "museum_exhibition": "Exhibition",
    "opera": "Opera",
    "ballet": "Ballet",
    "concert": "Concert",
    "theatre": "Theatre",
    "film": "Film",
    "vernissage": "Opening",
    "other": "Event",
    "mixed": "Event",
}

# CSS-class suffix per category — used for the colored left stripe and icon tint.
CATEGORY_SLUGS = {
    "museum_exhibition": "exh",
    "opera": "opera",
    "ballet": "ballet",
    "concert": "concert",
    "theatre": "theatre",
    "film": "film",
    "vernissage": "vern",
    "other": "other",
    "mixed": "other",
}

# Emoji icons. Render consistently across browsers / phones / OSes; zero
# fragility vs. inline SVG. Combined with the colored pill background, each
# category is unambiguous at a glance.
_CATEGORY_ICONS = {
    "museum_exhibition": "🎨",
    "opera": "🎭",
    "ballet": "🩰",
    "concert": "🎵",
    "theatre": "🎟️",
    "film": "🎬",
    "vernissage": "🥂",
    "other": "✨",
    "mixed": "✨",
}


def _icon(category: str) -> str:
    return _CATEGORY_ICONS.get(category, "")


def _slug(category: str) -> str:
    return CATEGORY_SLUGS.get(category, "other")


def _city_slug(city: str) -> str:
    """Slugify a German city name for use in CSS class names."""
    if not city:
        return "unknown"
    s = city.lower().strip()
    for src, dst in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        s = s.replace(src, dst)
    # Drop common suffixes that hurt readability
    s = s.replace(" a.d. ruhr", "")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"


def _css_safe(s: str) -> str:
    """Sanitize a string for use as a CSS id selector (e.g. venue_id → CSS id)."""
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", s)


# Sub-stage names that should collapse to their parent venue for chip grouping.
# The agenda row still displays the specific room (mum sees "Aalto-Foyer ·
# Essen") — only the *chip* is grouped, so unticking "Aalto-Theater" hides
# every Aalto sub-room in one go.
_CHIP_NAME_NORMALIZE = {
    # Aalto sub-spaces (incl. Aalto Ballett's ADA studio) — collapse to one chip
    "Aalto-Foyer": "Aalto-Theater",
    "Aalto-Cafeteria": "Aalto-Theater",
    "ADA": "Aalto-Theater",
    "Treffpunkt: Haupteingang Aalto-Theater": "Aalto-Theater",
    # Grillo sub-spaces — collapse
    "Treffpunkt: Haupteingang Grillo-Theater": "Grillo-Theater",
    "Café Central": "Grillo-Theater",
    # Philharmonie Essen complex — main hall + chamber pavilions + courtyard,
    # all collapsed into one chip. Specific room still shown on each row.
    "Alfried Krupp Saal": "Philharmonie Essen",
    "NATIONAL-BANK Pavillon": "Philharmonie Essen",
    "RWE Pavillon": "Philharmonie Essen",
    "Vorplatz Huyssenallee": "Philharmonie Essen",
}

_CHIP_NAME_REGEX = [
    (r"\s*[–\-]\s*(Foyer|Probensaal|Probebühne|Bühne|Studio)\b.*$", ""),
    (r"\s+Studio\s+\d+$", ""),
]


def _normalize_chip_name(name: str) -> str:
    if name in _CHIP_NAME_NORMALIZE:
        return _CHIP_NAME_NORMALIZE[name]
    for pat, repl in _CHIP_NAME_REGEX:
        name = re.sub(pat, repl, name)
    return name.strip()


WHEN_OPTIONS = [
    ("this-week",     "This Week"),
    ("next-week",     "Next Week"),
    ("this-weekend",  "This Weekend"),
    ("next-weekend",  "Next Weekend"),
    ("this-month",    "This Month"),
    ("next-month",    "Next Month"),
]
ALL_WHEN_TAGS = [slot for slot, _ in WHEN_OPTIONS]


def _when_tags(start: Optional[datetime], end: Optional[datetime], now: datetime) -> list[str]:
    """Compute Wann filter tags for an event. Returns a subset of ALL_WHEN_TAGS.

    An ongoing exhibition (end > now, start <= now) qualifies for ALL tags
    since it's open across every relevant window.
    """
    if start is None:
        return []
    is_ongoing = end is not None and end > now and start <= now
    if is_ongoing:
        return list(ALL_WHEN_TAGS)

    today = now.date()
    sd = start.date()

    # Week boundaries (Mon-Sun, Python weekday: 0=Mon, 5=Sat, 6=Sun)
    this_monday = today - timedelta(days=today.weekday())
    this_sunday = this_monday + timedelta(days=6)
    next_monday = this_monday + timedelta(days=7)
    next_sunday = next_monday + timedelta(days=6)
    # Month boundaries (use day-32 trick to roll forward)
    this_m_start = today.replace(day=1)
    next_m_start = (this_m_start + timedelta(days=32)).replace(day=1)
    after_next_m = (next_m_start + timedelta(days=32)).replace(day=1)

    tags: list[str] = []
    if this_monday <= sd <= this_sunday:
        tags.append("this-week")
        if sd.weekday() in (5, 6):
            tags.append("this-weekend")
    elif next_monday <= sd <= next_sunday:
        tags.append("next-week")
        if sd.weekday() in (5, 6):
            tags.append("next-weekend")

    if this_m_start <= sd < next_m_start:
        tags.append("this-month")
    elif next_m_start <= sd < after_next_m:
        tags.append("next-month")

    return tags

# ─── public API ──────────────────────────────────────────────────────────────


def render(
    events: list,
    out_path: str | Path,
    featured: Optional[set] = None,
    title: str = "What's On",
    subtitle: Optional[str] = None,
    now: Optional[datetime] = None,
    horizon_days: int = 90,
    venue_meta: Optional[dict] = None,
) -> int:
    """Render `events` (list of Event dataclasses or dicts) to one HTML file.

    Args:
        events: scraped events (Event dataclass or dict — both supported via attribute lookup).
        out_path: where to write events.html.
        featured: set of (venue_id, normalized_title) tuples flagged as must-see.
            (Computed by the orchestrator from highlights.yaml.)
        title, subtitle: page header.
        now: override for relative-date calculation (testing).
        horizon_days: drop events starting more than this many days in the future,
            UNLESS they're ongoing exhibitions (end > now).

    Returns the number of events rendered.
    """
    now = now or datetime.now(timezone.utc)
    horizon = now + timedelta(days=horizon_days)
    featured = featured or set()

    # 1. Filter — drop past events; drop far-future single-day events; keep ongoing exhibitions
    visible = [e for e in events if _is_visible(e, now, horizon)]
    visible.sort(key=lambda e: _start(e))

    # 2. Collapse recurring same-title events at the same venue. After this, a 5-night
    #    Carmen run is one card showing the next date plus "und 4 weitere bis 11. Juni".
    #    De-emphasized (kids/edu) events are also collapsed by title.
    visible = _collapse_recurrences(visible)

    # 3. Featured pin list — sorted by closing date (soonest first), capped at 4.
    #    Skip de-emphasized: don't pin kids/educational shows at the top.
    feat_events = [
        e for e in visible
        if _featured_key(e) in featured and (_attr(e, "audience") or "general") == "general"
    ]
    feat_events.sort(key=lambda e: (_end(e) or _start(e)))
    feat_events = feat_events[:6]

    # 4. Group the rest by ISO week
    week_groups: list[tuple[str, list]] = _group_by_week(visible, now)

    # 5. Emit HTML
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    html_text = _render_html(
        title=title,
        subtitle=subtitle or _long_date(now),
        now=now,
        feat_events=feat_events,
        week_groups=week_groups,
        featured=featured,
        visible_events=visible,
        venue_meta=venue_meta or {},
    )
    out_path.write_text(html_text, encoding="utf-8")
    return len(visible)


def _collect_venues_by_city(
    events: list,
    venue_meta: Optional[dict] = None,
) -> dict[str, list[tuple[str, str, set, set]]]:
    """Build {city_slug: [(chip_id, display_name, {venue_ids}, {category_slugs}), ...]}

    Venues are grouped by **display name** within each city so multiple sources
    targeting the same physical venue (e.g. `kunstpalast` + `kunstpalast-current`)
    collapse into ONE chip that controls both. chip_id is `<city>-<name-slug>`.

    Per-venue category set tags the chip with `has-cat-X` classes for the
    "hide chips not relevant to the selected category" CSS rules.
    Aggregator events skipped.
    """
    from collections import Counter as _Counter
    venue_meta = venue_meta or {}
    # First pass — per venue_id collect venue_name distribution + cats + count.
    by_vid: dict[str, dict] = {}
    for e in events:
        vid = _attr(e, "venue_id") or ""
        vname = _attr(e, "venue_name") or ""
        city = _attr(e, "city") or ""
        if not vid or not city or city == "__aggregator__":
            continue
        bucket = by_vid.setdefault(vid, {"city": city, "names": _Counter(), "cats": set(), "n": 0})
        bucket["names"][vname] += 1
        bucket["cats"].add(_slug(_attr(e, "category") or "other"))
        bucket["n"] += 1

    # Second pass — assign a canonical chip label per venue_id.
    seen: dict[str, dict[str, dict]] = {}
    for vid, bucket in by_vid.items():
        cslug = _city_slug(bucket["city"])
        canonical = venue_meta.get(vid) or (bucket["names"].most_common(1)[0][0] if bucket["names"] else vid)
        chip_label = _normalize_chip_name(canonical)
        chip_id = f"{cslug}-{_css_safe(chip_label.lower())}"
        group = seen.setdefault(cslug, {}).setdefault(
            chip_id, {"name": chip_label, "venue_ids": set(), "cats": set(), "n": 0}
        )
        group["venue_ids"].add(vid)
        group["cats"].update(bucket["cats"])
        group["n"] += bucket["n"]

    # Suppress chips with <2 events — phantom venues from aggregator feeds
    # (random street locations, one-off school auditoriums, etc.) shouldn't
    # clutter the venues filter row. The events still render in the agenda.
    MIN_EVENTS_PER_CHIP = 2

    out: dict[str, list[tuple[str, str, set, set]]] = {}
    for cslug, groups in seen.items():
        kept = [
            (cid, g["name"], g["venue_ids"], g["cats"])
            for cid, g in groups.items()
            if g["n"] >= MIN_EVENTS_PER_CHIP
        ]
        out[cslug] = sorted(kept, key=lambda t: t[1].lower())
    return out


# ─── filtering ───────────────────────────────────────────────────────────────


def _collapse_recurrences(events: list) -> list:
    """Collapse same-title events at the same venue into a single representative.

    After collapse, the representative event carries `extra_occurrences` (a list
    of the other start datetimes) on it as a dynamic attribute, plus
    `occurrence_last` (the last occurrence's start). The representative is the
    next future occurrence (so the agenda shows it at the right week).

    Single-occurrence events are unchanged.
    """
    from collections import defaultdict
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for e in events:
        key = (_attr(e, "venue_id") or "", _normalize_title(_attr(e, "title") or ""))
        groups[key].append(e)

    out: list = []
    for evs in groups.values():
        evs.sort(key=lambda e: _start(e))
        if len(evs) == 1:
            out.append(evs[0])
            continue
        rep = evs[0]
        rest = evs[1:]
        try:
            rep.extra_occurrences = [_start(e) for e in rest]
            rep.occurrence_last = _start(evs[-1])
        except AttributeError:
            # dict events: write into the dict
            rep["extra_occurrences"] = [_start(e) for e in rest]
            rep["occurrence_last"] = _start(evs[-1])
        out.append(rep)
    return out


def _is_visible(e, now: datetime, horizon: datetime) -> bool:
    s = _start(e)
    en = _end(e)
    if s is None:
        return False
    # Ongoing exhibitions: end is in the future even if start is in the past
    if en is not None and en >= now:
        return en <= horizon + timedelta(days=365)  # keep multi-month shows visible
    # Single-day or short events: must be future-ish (one day buffer for tonight)
    return now - timedelta(days=1) <= s <= horizon


def _start(e) -> Optional[datetime]:
    return _dt_attr(e, "start")


def _end(e) -> Optional[datetime]:
    return _dt_attr(e, "end")


def _attr(e, name):
    """Read a field from either a dataclass-like object or a dict. No type coercion."""
    if isinstance(e, dict):
        return e.get(name)
    return getattr(e, name, None)


def _dt_attr(e, name) -> Optional[datetime]:
    """Read a datetime field; if it's an ISO string (from a dict round-trip), parse it."""
    v = _attr(e, name)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return v


# ─── grouping ────────────────────────────────────────────────────────────────


def _group_by_week(events: list, now: datetime) -> list[tuple[str, list]]:
    """Bucket events by ISO week. Each bucket gets a human label like
    'Diese Woche · 12.–18. Mai' or '15.–21. September'.

    Ongoing exhibitions (start in the past, end in the future) are bucketed
    into TODAY's week — otherwise NEUE WELTEN (started 2019) would land in a
    historical 'Juni 2019' bucket. They sort to the top of this week's bucket
    because their start dates are oldest.
    """
    if not events:
        return []
    this_year, this_week, _ = now.isocalendar()
    buckets: dict[tuple[int, int], list] = {}
    for e in events:
        s = _start(e)
        en = _end(e)
        if en is not None and en > now and s <= now:
            iso_year, iso_week = this_year, this_week
        else:
            iso_year, iso_week, _ = s.isocalendar()
        buckets.setdefault((iso_year, iso_week), []).append(e)

    today = now.date()
    this_year, this_week, _ = now.isocalendar()
    out: list[tuple[str, list]] = []
    for key in sorted(buckets):
        evs = buckets[key]
        evs.sort(key=lambda e: _start(e))
        label = _week_label(key, this_year, this_week, evs[0])
        out.append((label, evs))
    return out


def _week_label(key: tuple[int, int], this_year: int, this_week: int, first_event) -> str:
    iso_year, iso_week = key
    monday = datetime.fromisocalendar(iso_year, iso_week, 1).date()
    sunday = monday + timedelta(days=6)
    # US-style date span: "May 4–10" (single month) or "May 28 – Jun 3" (cross-month)
    if monday.month == sunday.month:
        span = f"{MONTHS[monday.month - 1]} {monday.day}–{sunday.day}"
    else:
        span = f"{MONTHS_SHORT[monday.month - 1]} {monday.day} – {MONTHS_SHORT[sunday.month - 1]} {sunday.day}"
    if iso_year == this_year and iso_week == this_week:
        return f"This Week · {span}"
    next_year, next_week, _ = (
        datetime.fromisocalendar(this_year, this_week, 1) + timedelta(days=7)
    ).isocalendar()
    if iso_year == next_year and iso_week == next_week:
        return f"Next Week · {span}"
    return span


# ─── relative time helpers ───────────────────────────────────────────────────


def _relative_phrase(start: datetime, end: Optional[datetime], now: datetime) -> str:
    """English countdown / duration phrase for the row.

    Single-day events:    'today' / 'tomorrow' / 'in 3 days' / 'in 2 weeks'
    Ongoing exhibitions:  'N days left' (closing date is already in the time
                          column, so we don't repeat it).
    """
    if end is not None and end > now and start <= now:
        days_left = (end.date() - now.date()).days
        return f"{days_left} day{'s' if days_left != 1 else ''} left"

    days = (start.date() - now.date()).days
    if days < 0:
        return ""
    if days == 0:
        return "today"
    if days == 1:
        return "tomorrow"
    if days < 7:
        return f"in {days} days"
    if days < 14:
        return "in 1 week"
    if days < 31:
        return f"in {days // 7} weeks"
    months = days // 30
    return f"in {months} month{'s' if months != 1 else ''}"


def _long_date(dt: datetime) -> str:
    """US-style long date: 'May 9, 2026'."""
    return f"{MONTHS[dt.month - 1]} {dt.day}, {dt.year}"


def _us_time(dt: datetime) -> str:
    """US 12-hour time without leading zero: '7:30 PM'. Cross-platform."""
    return dt.strftime("%I:%M %p").lstrip("0")


# ─── featured-event matching helper ──────────────────────────────────────────


_ADDRESS_RE = re.compile(
    r"\b\d{5}\b"                                            # zip code
    r"|"
    r"\b(allee|stra(ß|ss)e|platz|weg|ring|markt|gasse)\b",  # street-type words
    re.IGNORECASE,
)


def _clean_description(desc: str, venue_name: str) -> str:
    """Drop venue-address noise from event descriptions.

    TUP Essen's .ics DESCRIPTION fields are sometimes just the venue's
    postal address ("NATIONAL-BANK Pavillon · Philharmonie Essen ·
    Huyssenallee 53 · 45128 Essen"), which adds nothing on a card that
    already shows venue + city. Detect and drop those.
    """
    desc = (desc or "").strip()
    if not desc:
        return ""

    # If the description contains a postal code or street-type word AND
    # contains the venue name (or a fragment of it), it's almost certainly
    # the address — drop it.
    if _ADDRESS_RE.search(desc):
        venue_words = [w for w in re.split(r"\W+", venue_name) if len(w) >= 4]
        if any(w.lower() in desc.lower() for w in venue_words):
            return ""
    return desc


def _featured_key(e) -> tuple[str, str]:
    """The key used by the orchestrator to mark events as featured."""
    title = _attr(e, "title") or ""
    venue_id = _attr(e, "venue_id") or ""
    return (venue_id, _normalize_title(title))


def _normalize_title(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s


# ─── HTML emission ───────────────────────────────────────────────────────────


def _render_html(
    title: str,
    subtitle: str,
    now: datetime,
    feat_events: list,
    week_groups: list[tuple[str, list]],
    featured: set,
    visible_events: list,
    venue_meta: Optional[dict] = None,
) -> str:
    venues_by_city = _collect_venues_by_city(visible_events, venue_meta=venue_meta)
    # Flatten to {chip_id: {venue_ids: set}} — one entry per chip, even if the chip
    # controls multiple sources (Kunstpalast + Kunstpalast-current → one chip).
    chips_meta: dict[str, set] = {}
    for chip_list in venues_by_city.values():
        for cid, _name, venue_ids, _cats in chip_list:
            chips_meta.setdefault(cid, set()).update(venue_ids)

    # Discover cities present in the data → drives the dynamic Wo filter chips
    # and per-city CSS rules. Sorted by event count desc so the city with the
    # most events lists first, then alphabetical.
    city_counts: dict[str, tuple[str, int]] = {}
    for e in visible_events:
        city = _attr(e, "city") or ""
        if not city or city == "__aggregator__":
            continue
        slug = _city_slug(city)
        name, n = city_counts.get(slug, (city, 0))
        city_counts[slug] = (name, n + 1)
    # Alphabetical by display name (matches user expectation A→Z).
    cities = sorted(city_counts.items(), key=lambda kv: kv[1][0].lower())
    # cities = [(slug, (display_name, count)), ...]

    # Per-chip "is checked" visual state (for the chip itself). Filtering of
    # the actual rows is JS-driven (see inline script): on chip change, JS
    # collects checked venue_ids and tags non-matching rows with .venue-hidden.
    venue_css_rules = []
    for cid in sorted(chips_meta):
        venue_css_rules.append(
            f'#v-{cid}:checked ~ .filter-panel .venue-chips .venue-chip[for="v-{cid}"] '
            f'{{ background: var(--ink); color: var(--bg); border-color: var(--ink); }}'
        )
        venue_css_rules.append(
            f'#v-{cid}:checked ~ .filter-panel .venue-chips .venue-chip[for="v-{cid}"] .venue-checkbox '
            f'{{ background: var(--bg); border: 1px solid var(--bg); }}'
        )
    venue_css = "\n    ".join(venue_css_rules)

    # Per-Wann-slot CSS — hide rows/cards/days/weeks not matching the active
    # tag, audience-aware so slots that contain only kids/active rows still
    # collapse cleanly. Plus chip-active highlight per slot.
    when_css_rules = []
    for slot, _label in WHEN_OPTIONS:
        when_css_rules.append(
            f'#w-{slot}:checked ~ .agenda .row:not([data-when~="{slot}"]), '
            f'#w-{slot}:checked ~ .agenda .day:not(:has(.row[data-when~="{slot}"]:not(.audience-kids):not(.audience-active):not(.venue-hidden))), '
            f'#w-{slot}:checked ~ .agenda .week:not(:has(.row[data-when~="{slot}"]:not(.audience-kids):not(.audience-active):not(.venue-hidden))), '
            f'#w-{slot}:checked ~ .featured .featured-card:not([data-when~="{slot}"]), '
            f'#w-{slot}:checked ~ .featured:not(:has(.featured-card[data-when~="{slot}"])) '
            f'{{ display: none; }}'
        )
        when_css_rules.append(
            f'#w-{slot}:checked ~ .filter-panel .filter-bar .filter-chip[for="w-{slot}"] '
            f'{{ background: var(--ink); color: var(--bg); border-color: var(--ink); }}'
        )
    when_css_rules.append(
        '#w-all:checked ~ .filter-panel .filter-bar .filter-chip[for="w-all"] '
        '{ background: var(--ink); color: var(--bg); border-color: var(--ink); }'
    )
    when_css = "\n    ".join(when_css_rules)

    # Per-city CSS: hide rows/cards/days/weeks not matching, show empty-state,
    # show that city's venue-chip row, mark chip-row as active.
    # Plus city × category combined rules — when both filters are active, hide
    # days/weeks that have no row matching BOTH filters. CSS `:has()` checks
    # DOM presence not visibility, so single-dimension rules don't catch this.
    CATS_FOR_COMBO = ["opera", "concert", "ballet", "theatre", "film", "exh"]
    city_css_rules = []
    for cslug, (_name, _n) in cities:
        city_css_rules.append(
            f'#c-{cslug}:checked ~ .featured .featured-card:not(.city-{cslug}), '
            f'#c-{cslug}:checked ~ .agenda .row:not(.city-{cslug}), '
            f'#c-{cslug}:checked ~ .agenda .day:not(:has(.row.city-{cslug}:not(.audience-kids):not(.audience-active):not(.venue-hidden))), '
            f'#c-{cslug}:checked ~ .agenda .week:not(:has(.row.city-{cslug}:not(.audience-kids):not(.audience-active):not(.venue-hidden))), '
            f'#c-{cslug}:checked ~ .featured:not(:has(.featured-card.city-{cslug})) '
            f'{{ display: none; }}'
        )
        city_css_rules.append(
            f'#c-{cslug}:checked ~ .agenda:not(:has(.row.city-{cslug}:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}'
        )
        # Override single-city: re-show days/weeks when extras toggle reveals kids/active
        city_css_rules.append(
            f'#show-extras:checked ~ #c-{cslug}:checked ~ .agenda .day:has(.row.city-{cslug}:not(.venue-hidden)), '
            f'#show-extras:checked ~ #c-{cslug}:checked ~ .agenda .week:has(.row.city-{cslug}:not(.venue-hidden)) '
            f'{{ display: block !important; }}'
        )
        city_css_rules.append(
            f'#c-{cslug}:checked ~ .filter-panel .venue-chips.city-{cslug} {{ display: flex; }}'
        )
        city_css_rules.append(
            f'#c-{cslug}:checked ~ .filter-panel .filter-bar .filter-chip[for="c-{cslug}"] '
            f'{{ background: var(--ink); color: var(--bg); border-color: var(--ink); }}'
        )
        # Combined city × category empty-day/week hiders, audience-aware.
        # Order matters: f-* inputs emit before c-* inputs in DOM.
        for cat in CATS_FOR_COMBO:
            city_css_rules.append(
                f'#f-{cat}:checked ~ #c-{cslug}:checked ~ .agenda .day:not(:has(.row.city-{cslug}.cat-{cat}:not(.audience-kids):not(.audience-active):not(.venue-hidden))), '
                f'#f-{cat}:checked ~ #c-{cslug}:checked ~ .agenda .week:not(:has(.row.city-{cslug}.cat-{cat}:not(.audience-kids):not(.audience-active):not(.venue-hidden))) '
                f'{{ display: none; }}'
            )
            city_css_rules.append(
                f'#show-extras:checked ~ #f-{cat}:checked ~ #c-{cslug}:checked ~ .agenda .day:has(.row.city-{cslug}.cat-{cat}:not(.venue-hidden)), '
                f'#show-extras:checked ~ #f-{cat}:checked ~ #c-{cslug}:checked ~ .agenda .week:has(.row.city-{cslug}.cat-{cat}:not(.venue-hidden)) '
                f'{{ display: block !important; }}'
            )
    city_css = "\n    ".join(city_css_rules)

    parts: list[str] = []
    parts.append(_PAGE_HEAD.format(title=html.escape(title), venue_css=venue_css, city_css=city_css, when_css=when_css))
    parts.append('<div class="page">')

    # Hidden inputs — must be siblings of .featured/.agenda for the `~` CSS
    # selectors to reach them. ORDER MATTERS: any chained rule like
    # `#A:checked ~ #B:checked ~ .agenda` requires #B to come AFTER #A in DOM.
    # We emit `show-extras` first so override rules `#show-extras:checked ~ #f-X`
    # and `#show-extras:checked ~ #c-X` actually match. Then f-* before c-*
    # before w-* so cross-dimension combos chain correctly.
    parts.append('  <input type="checkbox" id="show-extras" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-all" class="filter-input" checked>')
    parts.append('  <input type="radio" name="catfilter" id="f-opera" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-concert" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-ballet" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-theatre" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-film" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-exh" class="filter-input">')
    parts.append('  <input type="radio" name="catfilter" id="f-fav" class="filter-input">')
    parts.append('  <input type="radio" name="cityfilter" id="c-all" class="filter-input" checked>')
    for cslug, _ in cities:
        parts.append(f'  <input type="radio" name="cityfilter" id="c-{cslug}" class="filter-input">')
    # When filter (Wann) — single-select like Wo / Was. Slots come from WHEN_OPTIONS.
    parts.append('  <input type="radio" name="whenfilter" id="w-all" class="filter-input" checked>')
    for slot, _label in WHEN_OPTIONS:
        parts.append(f'  <input type="radio" name="whenfilter" id="w-{slot}" class="filter-input">')
    # Per-chip checkboxes — default UNchecked. Click-to-add interaction:
    # when zero chips checked, all venues visible; when 1+ checked, only those.
    # Filtering logic lives in inline JS at bottom (sets .venue-hidden class).
    for cid in sorted(chips_meta):
        parts.append(f'  <input type="checkbox" id="v-{cid}" class="filter-input">')

    # Header
    parts.append('  <header class="masthead">')
    parts.append(f'    <h1>{html.escape(title)}</h1>')
    parts.append(f'    <p class="subtitle">Coming weeks · As of {html.escape(subtitle)}, {now.strftime("%H:%M")}</p>')
    parts.append('  </header>')

    # All filters wrapped in one panel for visual rhythm
    parts.append('  <div class="filter-panel">')

    # Search box — type to filter the agenda by free text (title, venue, city)
    parts.append('  <div class="search-row">')
    parts.append('    <input type="search" id="search-box" class="search-input" '
                 'placeholder="Search — e.g. Beethoven, LACMA, ballet…" '
                 'autocomplete="off" spellcheck="false">')
    parts.append('  </div>')

    # Filter bars — Wo (city) first, then Was (category). When a specific city
    # is picked, a third row of venue chips appears below allowing per-venue
    # opt-in within that city.
    parts.append('  <nav class="filter-bar filter-bar-city" aria-label="Filter by neighborhood">')
    parts.append('    <span class="filter-label">Where</span>')
    parts.append('    <label for="c-all" class="filter-chip filter-all">All</label>')
    for cslug, (cname, ccount) in cities:
        parts.append(
            f'    <label for="c-{cslug}" class="filter-chip">'
            f'{html.escape(cname)} <span class="chip-count">{ccount}</span>'
            f'</label>'
        )
    parts.append('  </nav>')
    parts.append('  <nav class="filter-bar filter-bar-when" aria-label="Filter by time">')
    parts.append('    <span class="filter-label">When</span>')
    parts.append('    <label for="w-all" class="filter-chip filter-all">Anytime</label>')
    for slot, label in WHEN_OPTIONS:
        parts.append(f'    <label for="w-{slot}" class="filter-chip">{html.escape(label)}</label>')
    parts.append('  </nav>')
    parts.append('  <nav class="filter-bar" aria-label="Filter by category">')
    parts.append('    <span class="filter-label">What</span>')
    parts.append('    <label for="f-all"     class="filter-chip filter-all">All</label>')
    parts.append('    <label for="f-opera"   class="filter-chip cat-opera"><span class="cat-icon">🎭</span><span>Opera</span></label>')
    parts.append('    <label for="f-concert" class="filter-chip cat-concert"><span class="cat-icon">🎵</span><span>Concert</span></label>')
    parts.append('    <label for="f-ballet"  class="filter-chip cat-ballet"><span class="cat-icon">🩰</span><span>Ballet</span></label>')
    parts.append('    <label for="f-theatre" class="filter-chip cat-theatre"><span class="cat-icon">🎟️</span><span>Theatre</span></label>')
    parts.append('    <label for="f-film"    class="filter-chip cat-film"><span class="cat-icon">🎬</span><span>Film</span></label>')
    parts.append('    <label for="f-exh"     class="filter-chip cat-exh"><span class="cat-icon">🎨</span><span>Exhibition</span></label>')
    # Spacer pushes the Favoriten chip to the right edge of the Was row
    parts.append('    <span class="filter-bar-spacer" aria-hidden="true"></span>')
    parts.append('    <label for="f-fav"     class="filter-chip filter-chip-fav"><span class="cat-icon">❤️</span><span>Favorites</span></label>')
    parts.append('  </nav>')
    # Per-venue chips — built from the actual venues in the data, grouped by city.
    # Each chip is tagged with `has-cat-X` classes for every category the venue
    # actually has events in. CSS hides chips that don't match the active
    # category filter, so picking "Konzert" only shows venues that have concerts.
    # The "Alle" button at the start of each row is JS-driven (re-tick all chips).
    for city_slug, venues in venues_by_city.items():
        parts.append(f'  <nav class="venue-chips city-{city_slug}" aria-label="Filter by venue">')
        parts.append('    <span class="filter-label">Venues</span>')
        parts.append('    <button type="button" class="venue-chip-all" data-city="{}">All</button>'.format(city_slug))
        for chip_id, vname, _venue_ids, cats in venues:
            cat_classes = " ".join(f"has-cat-{c}" for c in sorted(cats))
            parts.append(
                f'    <label for="v-{chip_id}" class="venue-chip {cat_classes}">'
                f'<span class="venue-checkbox" aria-hidden="true"></span>'
                f'{html.escape(vname)}'
                f'</label>'
            )
        parts.append('  </nav>')
    parts.append('  <div class="extras-toggle-wrapper">')
    parts.append('    <label for="show-extras" class="extras-toggle">'
                 '<span class="extras-checkbox" aria-hidden="true"></span>'
                 'Also show classes, workshops, and family programmes'
                 '</label>')
    parts.append('  </div>')
    parts.append('  </div>')  # close .filter-panel

    # Featured / "Nicht verpassen"
    if feat_events:
        parts.append('  <section class="featured">')
        parts.append('    <h2 class="featured-heading"><span class="star">★</span> Don\'t Miss</h2>')
        parts.append('    <div class="featured-grid">')
        for ev in feat_events:
            parts.append(_render_featured_card(ev, now))
        parts.append('    </div>')
        parts.append('  </section>')

    # Weekly agenda — each week wrapped in .week for clean filter targeting.
    # Ongoing exhibitions inside a week bucket get a special "Aktuell zu sehen"
    # day-block at the top, instead of being scattered under their original
    # start dates (which are often years ago).
    parts.append('  <section class="agenda">')
    if not week_groups:
        parts.append('    <p class="empty">No events found.</p>')
    for week_label, evs in week_groups:
        parts.append('    <div class="week">')
        parts.append(f'      <h2 class="week-heading">{html.escape(week_label)}</h2>')
        ongoing: list = []
        by_day: dict[date, list] = {}
        for e in evs:
            s = _start(e)
            en = _end(e)
            if en is not None and en > now and s <= now:
                ongoing.append(e)
            else:
                by_day.setdefault(s.date(), []).append(e)
        if ongoing:
            ongoing.sort(key=lambda x: (_end(x) or _start(x)))
            parts.append(_render_ongoing_block(ongoing, now, featured))
        for d in sorted(by_day):
            parts.append(_render_day_block(d, by_day[d], now, featured))
        parts.append('    </div>')
    parts.append('    <p class="filter-empty">No events in this category.</p>')
    parts.append('  </section>')

    # Footer
    parts.append('  <footer>')
    parts.append(f'    <p>Updated {html.escape(_long_date(now))} · {now.strftime("%H:%M")}</p>')
    parts.append('  </footer>')

    parts.append('</div>')
    # Tiny "Alle" toggle script — re-ticks every per-chip checkbox in one click.
    # Only behaviour the page can't do in pure CSS (CSS reads input state, can't
    # write it). If JS is disabled the rest of the page still works.
    # Inline JS: Alle-toggle (venue chips) + favorites + NEU badges.
    # All persistence via localStorage. Degrades gracefully if JS is disabled —
    # filter chips still work (CSS), the page is still readable, just no
    # favoriting and no "Neu since last visit" badges.
    # JS: serialize the chip→venue_ids map so the venue-filter logic knows which
    # rows each chip controls (a chip can control multiple venue_ids in the
    # multi-source-merged-venue case, e.g. Kunstpalast + Kunstpalast-current).
    import json as _json
    chip_map_json = _json.dumps({cid: sorted(vids) for cid, vids in chips_meta.items()})
    parts.append('<script>')
    parts.append('(function(){')
    parts.append(f'  var chipMap={chip_map_json};')
    parts.append('  // Apply venue filter: tag non-matching rows with .venue-hidden.')
    parts.append('  // When NO chips checked, no filter applied (everything visible).')
    parts.append('  function applyVenueFilter(){')
    parts.append('    var checked=Object.create(null);')
    parts.append('    document.querySelectorAll(\'input.filter-input[id^="v-"]:checked\').forEach(function(c){')
    parts.append('      var cid=c.id.slice(2);')
    parts.append('      (chipMap[cid]||[]).forEach(function(v){checked[v]=true;});')
    parts.append('    });')
    parts.append('    var anyChecked=Object.keys(checked).length>0;')
    parts.append('    document.querySelectorAll("a.row[data-venue], a.featured-card[data-venue]").forEach(function(el){')
    parts.append('      var vid=el.getAttribute("data-venue");')
    parts.append('      if(!anyChecked||checked[vid]) el.classList.remove("venue-hidden");')
    parts.append('      else el.classList.add("venue-hidden");')
    parts.append('    });')
    parts.append('    // "Reset" button is the visual default — highlight it when no')
    parts.append('    // venue chips are checked, dim it when the user has picked specific venues.')
    parts.append('    document.querySelectorAll(".venue-chip-all").forEach(function(b){')
    parts.append('      b.classList.toggle("active",!anyChecked);')
    parts.append('    });')
    parts.append('  }')
    parts.append('  document.querySelectorAll(\'input.filter-input[id^="v-"]\').forEach(function(c){')
    parts.append('    c.addEventListener("change",applyVenueFilter);')
    parts.append('  });')
    parts.append('  // Initialize "Reset" highlight on page load (default = active).')
    parts.append('  applyVenueFilter();')
    parts.append('  // "Alle" button = clear venue selection (uncheck all chips, show everything).')
    parts.append('  document.querySelectorAll(".venue-chip-all").forEach(function(b){')
    parts.append('    b.addEventListener("click",function(){')
    parts.append('      document.querySelectorAll(\'input.filter-input[id^="v-"]\').forEach(function(c){c.checked=false;});')
    parts.append('      applyVenueFilter();')
    parts.append('    });')
    parts.append('  });')
    parts.append('  // Favorites + NEU badges (state in localStorage)')
    parts.append('  var FAV="momev:favs",SEEN="momev:seen",TS="momev:seen-ts";')
    parts.append('  var FOUR_HOURS=4*60*60*1000, SEEN_DELAY=30*1000;')
    parts.append('  function load(k){try{return JSON.parse(localStorage.getItem(k)||"[]");}catch(e){return [];}}')
    parts.append('  function save(k,arr){try{localStorage.setItem(k,JSON.stringify(arr));}catch(e){}}')
    parts.append('  var favs={},seen={};')
    parts.append('  load(FAV).forEach(function(id){favs[id]=true;});')
    parts.append('  load(SEEN).forEach(function(id){seen[id]=true;});')
    parts.append('  var lastTs=parseInt(localStorage.getItem(TS)||"0",10);')
    parts.append('  var now=Date.now();')
    parts.append('  var firstEverVisit=lastTs===0;')
    parts.append('  var freshSession=(now-lastTs)>FOUR_HOURS;')
    parts.append('  var els=document.querySelectorAll("a.row, a.featured-card");')
    parts.append('  var currentIds={};')
    parts.append('  els.forEach(function(el){')
    parts.append('    var id=el.getAttribute("href");')
    parts.append('    currentIds[id]=true;')
    parts.append('    if(favs[id]){')
    parts.append('      el.classList.add("is-fav");')
    parts.append('      var btn=el.querySelector(".fav-btn");')
    parts.append('      if(btn) btn.textContent="♥";')
    parts.append('    }')
    parts.append('    if(freshSession && !firstEverVisit && !seen[id]){')
    parts.append('      el.classList.add("is-new");')
    parts.append('      var b=el.querySelector(".new-badge");')
    parts.append('      if(b) b.removeAttribute("hidden");')
    parts.append('    }')
    parts.append('    var fav=el.querySelector(".fav-btn");')
    parts.append('    if(fav){')
    parts.append('      fav.addEventListener("click",function(e){')
    parts.append('        e.preventDefault(); e.stopPropagation();')
    parts.append('        if(favs[id]){ delete favs[id]; el.classList.remove("is-fav"); fav.textContent="♡"; }')
    parts.append('        else        { favs[id]=true; el.classList.add("is-fav");     fav.textContent="♥"; }')
    parts.append('        save(FAV,Object.keys(favs));')
    parts.append('      });')
    parts.append('    }')
    parts.append('  });')
    parts.append('  // First visit: persist seen-state SYNCHRONOUSLY so a fast tab-close')
    parts.append('  // doesn\'t leave us stuck in "first ever visit" mode forever.')
    parts.append('  if(firstEverVisit){')
    parts.append('    Object.keys(currentIds).forEach(function(id){seen[id]=true;});')
    parts.append('    save(SEEN,Object.keys(seen));')
    parts.append('    localStorage.setItem(TS,String(now));')
    parts.append('  } else {')
    parts.append('    // Normal: delay merge so user has time to see NEU badges')
    parts.append('    setTimeout(function(){')
    parts.append('      Object.keys(currentIds).forEach(function(id){seen[id]=true;});')
    parts.append('      save(SEEN,Object.keys(seen));')
    parts.append('      localStorage.setItem(TS,String(now));')
    parts.append('    }, SEEN_DELAY);')
    parts.append('  }')
    parts.append('  // Search box — debounced free-text filter. Tags rows with')
    parts.append('  // .search-match when title/venue/city contains the typed text.')
    parts.append('  var box=document.getElementById("search-box");')
    parts.append('  if(box){')
    parts.append('    var allEls=document.querySelectorAll("a.row, a.featured-card");')
    parts.append('    var idx=Array.prototype.map.call(allEls,function(el){')
    parts.append('      var t=(el.querySelector(".row-title,.fc-title")||{}).textContent||"";')
    parts.append('      var v=(el.querySelector(".row-venue,.fc-venue")||{}).textContent||"";')
    parts.append('      return {el:el, hay:(t+" "+v).toLowerCase()};')
    parts.append('    });')
    parts.append('    var deb;')
    parts.append('    box.addEventListener("input",function(){')
    parts.append('      clearTimeout(deb);')
    parts.append('      deb=setTimeout(function(){')
    parts.append('        var q=box.value.trim().toLowerCase();')
    parts.append('        if(!q){')
    parts.append('          document.body.classList.remove("has-search-query");')
    parts.append('          idx.forEach(function(o){o.el.classList.remove("search-match");});')
    parts.append('          return;')
    parts.append('        }')
    parts.append('        document.body.classList.add("has-search-query");')
    parts.append('        idx.forEach(function(o){')
    parts.append('          if(o.hay.indexOf(q)!==-1) o.el.classList.add("search-match");')
    parts.append('          else                      o.el.classList.remove("search-match");')
    parts.append('        });')
    parts.append('      },120);')
    parts.append('    });')
    parts.append('  }')
    parts.append('})();')
    parts.append('</script>')
    parts.append('</body></html>')
    return "\n".join(parts)


def _render_featured_card(ev, now: datetime) -> str:
    title = html.escape(_attr(ev, "title") or "")
    venue_name_raw = _attr(ev, "venue_name") or ""
    venue = html.escape(venue_name_raw)
    city = html.escape(_attr(ev, "city") or "")
    url = html.escape(_attr(ev, "url") or "#")
    raw_desc = _attr(ev, "description") or ""
    description = html.escape(_clean_description(raw_desc, venue_name_raw))[:240]
    category_raw = _attr(ev, "category") or "other"
    category = CATEGORY_LABELS.get(category_raw, "")
    cat_slug = _slug(category_raw)
    icon = _icon(category_raw)

    s = _start(ev)
    e = _end(ev)
    if e is not None and e > now:
        days_left = (e.date() - now.date()).days
        date_line = f"through {MONTHS[e.month - 1]} {e.day} · {days_left} day{'s' if days_left != 1 else ''} left"
    else:
        date_line = (
            f'{WEEKDAYS[s.weekday()]}, {MONTHS[s.month - 1]} {s.day}'
            f' · {_us_time(s)}'
        )

    extras = _attr(ev, "extra_occurrences") or []
    if extras:
        last = _attr(ev, "occurrence_last")
        last_str = f"{MONTHS_SHORT[last.month - 1]} {last.day}" if last else ""
        date_line += f" · + {len(extras)} more {'date' if len(extras) == 1 else 'dates'} through {last_str}"

    pill_html = (
        f'<div class="cat-pill cat-{cat_slug} fc-pill">'
        f'<span class="cat-icon">{icon}</span>'
        f'<span class="cat-label">{html.escape(category).upper()}</span>'
        f'</div>'
        if category else ""
    )

    city_slug = _city_slug(_attr(ev, 'city') or '')
    venue_id_attr = html.escape(_attr(ev, "venue_id") or "")
    when_attr = " ".join(_when_tags(s, e, now))
    return f"""    <a class="featured-card cat-{cat_slug} city-{city_slug}" href="{url}" target="_blank" rel="noopener noreferrer" data-venue="{venue_id_attr}" data-when="{when_attr}">
      <div class="fc-band"></div>
      <div class="fc-inner">
        {pill_html}
        <button type="button" class="fav-btn fc-fav" aria-label="Mark as favorite">♡</button>
        <div class="fc-title"><span class="new-badge" hidden>NEW</span>{title}</div>
        <div class="fc-venue">{venue} · {city}</div>
        <div class="fc-date">{date_line}</div>
        {f'<div class="fc-desc">{description}</div>' if description else ''}
        <div class="fc-meta"><span class="arrow">→</span></div>
      </div>
    </a>"""


def _render_day_block(d: date, evs: list, now: datetime, featured: set) -> str:
    weekday = WEEKDAYS[d.weekday()]
    month = MONTHS_SHORT[d.month - 1]
    label = f"{weekday}, {month} {d.day}"
    rows = "\n".join(_render_row(e, now, featured) for e in evs)
    return f"""    <div class="day">
      <h3 class="day-heading">{html.escape(label)}</h3>
      <div class="rows">
{rows}
      </div>
    </div>"""


def _render_ongoing_block(evs: list, now: datetime, featured: set) -> str:
    """Render ongoing exhibitions under a 'Aktuell zu sehen' heading at the top
    of their containing week. Avoids the per-day stretching of multi-month shows."""
    rows = "\n".join(_render_row(e, now, featured) for e in evs)
    return f"""    <div class="day day-ongoing">
      <h3 class="day-heading">Now Showing</h3>
      <div class="rows">
{rows}
      </div>
    </div>"""


def _render_row(ev, now: datetime, featured: set) -> str:
    s = _start(ev)
    e = _end(ev)
    title = _attr(ev, "title") or ""
    venue = html.escape(_attr(ev, "venue_name") or "")
    city = html.escape(_attr(ev, "city") or "")
    url = html.escape(_attr(ev, "url") or "#")
    category_raw = _attr(ev, "category") or "other"
    category = CATEGORY_LABELS.get(category_raw, "")
    cat_slug = _slug(category_raw)
    icon = _icon(category_raw)
    audience = _attr(ev, "audience") or "general"
    relative = _relative_phrase(s, e, now)
    is_featured = _featured_key(ev) in featured and audience == "general"

    if e is not None and e > now and s <= now:
        time_display = f"thru {MONTHS_SHORT[e.month - 1]} {e.day}"
    elif s.hour == 0 and s.minute == 0:
        time_display = "all day"
    else:
        time_display = s.strftime("%H:%M")

    star = '<span class="star inline-star">★</span> ' if is_featured else ""
    title_html = f"{star}{html.escape(title)}"

    extras = _attr(ev, "extra_occurrences") or []
    extras_text = ""
    if extras:
        last = _attr(ev, "occurrence_last")
        last_str = f"{MONTHS_SHORT[last.month - 1]} {last.day}" if last else ""
        extras_text = f'<div class="row-recurring">+ {len(extras)} more {"date" if len(extras) == 1 else "dates"} through {last_str}</div>'

    pill_html = ""
    if category:
        pill_html = (
            f'<div class="cat-pill cat-{cat_slug}">'
            f'<span class="cat-icon">{icon}</span>'
            f'<span class="cat-label">{html.escape(category).upper()}</span>'
            f'</div>'
        )

    relative_html = (
        f'<span class="relative">{html.escape(relative)}</span>' if relative else ""
    )

    classes = ["row", f"cat-{cat_slug}", f"city-{_city_slug(_attr(ev, 'city') or '')}"]
    if is_featured:
        classes.append("featured-row")
    if audience != "general":
        classes.append(f"audience-{audience}")
        classes.append("de-emphasized")

    venue_id_attr = html.escape(_attr(ev, "venue_id") or "")
    when_attr = " ".join(_when_tags(s, e, now))
    return f"""        <a class="{' '.join(classes)}" href="{url}" target="_blank" rel="noopener noreferrer" data-venue="{venue_id_attr}" data-when="{when_attr}">
          <div class="row-time">{html.escape(time_display)}</div>
          <div class="row-body">
            <div class="row-title"><span class="new-badge" hidden>NEW</span>{title_html}</div>
            <div class="row-venue">{venue} · {city}</div>
            <div class="row-meta">{relative_html}</div>
            {extras_text}
          </div>
          {pill_html}
          <button type="button" class="fav-btn" aria-label="Mark as favorite">♡</button>
          <div class="row-arrow">→</div>
        </a>"""


# ─── stylesheet ──────────────────────────────────────────────────────────────


_PAGE_HEAD = """<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #fafaf6;
      --paper: #ffffff;
      --ink: #1a1a1a;
      --muted: #6f6a5e;
      --accent: #b8860b;
      --rule: #e7e3d6;
      --hover: #f4f1e6;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #15140f;
        --paper: #1c1b15;
        --ink: #f3efe2;
        --muted: #9b9482;
        --accent: #d9a93b;
        --rule: #2a2820;
        --hover: #232118;
      }}
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; padding: 0; background: var(--bg); color: var(--ink); -webkit-text-size-adjust: 100%; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Inter", sans-serif;
      font-size: 17px;
      line-height: 1.5;
    }}
    .serif {{ font-family: Georgia, "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif; }}
    .page {{
      max-width: 720px;
      margin: 0 auto;
      padding: 32px 20px 80px;
    }}
    /* Header */
    .masthead {{
      padding-bottom: 24px;
      border-bottom: 1px solid var(--rule);
      margin-bottom: 24px;
    }}
    .masthead h1 {{
      font-size: 36px;
      font-weight: 700;
      letter-spacing: -0.02em;
      margin: 0 0 4px;
      line-height: 1.15;
    }}
    .masthead .subtitle {{
      color: var(--muted);
      font-size: 14px;
      margin: 0;
      letter-spacing: 0.02em;
    }}
    /* Filter panel — wraps Wo/Was/Wann/Häuser/search in one calm container */
    .filter-panel {{
      background: var(--paper);
      border: 1px solid var(--rule);
      border-radius: 10px;
      padding: 16px 18px 14px;
      margin: 0 0 32px;
    }}
    @media (prefers-color-scheme: dark) {{
      .filter-panel {{ background: rgba(255,255,255,0.02); }}
    }}
    /* Search box — sits at top of filter panel, debounced via JS */
    .search-row {{ margin: 0 0 12px; }}
    .search-input {{
      width: 100%;
      box-sizing: border-box;
      padding: 10px 14px;
      font: inherit;
      font-size: 15px;
      color: var(--ink);
      background: var(--paper);
      border: 1px solid var(--rule);
      border-radius: 8px;
      transition: border-color 100ms ease, background 100ms ease;
    }}
    .search-input:focus {{
      outline: none;
      border-color: var(--accent);
      background: var(--bg);
    }}
    .search-input::placeholder {{ color: var(--muted); }}
    /* When body has the .has-search-query class, hide rows/cards not flagged as matching.
       Empty days/weeks/featured collapse via :has, mirroring the other filters. */
    body.has-search-query a.row:not(.search-match),
    body.has-search-query .agenda .day:not(:has(a.row.search-match:not(.venue-hidden))),
    body.has-search-query .agenda .week:not(:has(a.row.search-match:not(.venue-hidden))),
    body.has-search-query .featured .featured-card:not(.search-match),
    body.has-search-query .featured:not(:has(.featured-card.search-match)) {{ display: none; }}
    /* Filter bar — Wo / Was / Wann / Häuser share consistent spacing & chip styles */
    .filter-input {{ position: absolute; opacity: 0; pointer-events: none; }}
    .filter-bar,
    .venue-chips {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 5px;
      margin: 0 0 8px;
    }}
    .filter-bar:last-child {{ margin-bottom: 0; }}
    /* "Auch Kurse..." toggle hidden; underlying audience filter still applies */
    .extras-toggle-wrapper {{ display: none; }}
    /* Spacer that grows to fill remaining width — pushes Favoriten chip right. */
    .filter-bar-spacer {{ flex: 1; min-width: 12px; }}
    .filter-chip-fav .cat-icon {{ font-size: 11px; }}
    .filter-label {{
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: var(--muted);
      margin-right: 6px;
      min-width: 32px;
    }}
    /* Single base style for ALL chips (filter-chip, venue-chip, venue-chip-all) */
    .filter-chip,
    .venue-chip,
    .venue-chip-all {{
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid var(--rule);
      background: var(--bg);
      font: inherit;
      font-size: 12px;
      font-weight: 500;
      letter-spacing: 0.01em;
      color: var(--ink);
      cursor: pointer;
      user-select: none;
      transition: background 100ms ease, border-color 100ms ease, color 100ms ease;
      line-height: 1.25;
    }}
    .filter-chip:hover,
    .venue-chip:hover,
    .venue-chip-all:hover {{ background: var(--hover); }}
    .cat-icon {{ font-size: 11px; }}
    /* Active state — highlight the chip whose radio is checked.
       (Per-city chip-active rules emitted dynamically in the city_css block.) */
    #f-all:checked      ~ .filter-panel .filter-bar .filter-chip[for="f-all"],
    #f-opera:checked    ~ .filter-panel .filter-bar .filter-chip[for="f-opera"],
    #f-concert:checked  ~ .filter-panel .filter-bar .filter-chip[for="f-concert"],
    #f-ballet:checked   ~ .filter-panel .filter-bar .filter-chip[for="f-ballet"],
    #f-theatre:checked  ~ .filter-panel .filter-bar .filter-chip[for="f-theatre"],
    #f-film:checked     ~ .filter-panel .filter-bar .filter-chip[for="f-film"],
    #f-exh:checked      ~ .filter-panel .filter-bar .filter-chip[for="f-exh"],
    #c-all:checked      ~ .filter-panel .filter-bar .filter-chip[for="c-all"] {{
      background: var(--ink);
      color: var(--bg);
      border-color: var(--ink);
    }}
    .chip-count {{
      font-size: 11px;
      color: var(--muted);
      margin-left: 2px;
    }}
    .filter-chip:hover .chip-count,
    .filter-input:checked + .filter-chip .chip-count {{ color: inherit; opacity: 0.7; }}
    /* Filter logic — when a category is checked, hide rows + cards + days + weeks
       that don't match. The :has(...:visible-row) checks deliberately exclude
       audience-kids/active rows so days containing only hidden kids events
       collapse too. The show-extras override re-shows them when the toggle is on. */
    .filter-empty {{ display: none; color: var(--muted); padding: 16px 8px; }}
    /* Opera */
    #f-opera:checked   ~ .featured .featured-card:not(.cat-opera),
    #f-opera:checked   ~ .agenda .row:not(.cat-opera),
    #f-opera:checked   ~ .agenda .day:not(:has(.row.cat-opera:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-opera:checked   ~ .agenda .week:not(:has(.row.cat-opera:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-opera:checked   ~ .featured:not(:has(.featured-card.cat-opera)) {{ display: none; }}
    #f-opera:checked   ~ .agenda:not(:has(.row.cat-opera:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-opera:checked ~ .agenda .day:has(.row.cat-opera:not(.venue-hidden)),
    #show-extras:checked ~ #f-opera:checked ~ .agenda .week:has(.row.cat-opera:not(.venue-hidden)) {{ display: block !important; }}
    /* Concert */
    #f-concert:checked ~ .featured .featured-card:not(.cat-concert),
    #f-concert:checked ~ .agenda .row:not(.cat-concert),
    #f-concert:checked ~ .agenda .day:not(:has(.row.cat-concert:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-concert:checked ~ .agenda .week:not(:has(.row.cat-concert:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-concert:checked ~ .featured:not(:has(.featured-card.cat-concert)) {{ display: none; }}
    #f-concert:checked ~ .agenda:not(:has(.row.cat-concert:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-concert:checked ~ .agenda .day:has(.row.cat-concert:not(.venue-hidden)),
    #show-extras:checked ~ #f-concert:checked ~ .agenda .week:has(.row.cat-concert:not(.venue-hidden)) {{ display: block !important; }}
    /* Ballet */
    #f-ballet:checked  ~ .featured .featured-card:not(.cat-ballet),
    #f-ballet:checked  ~ .agenda .row:not(.cat-ballet),
    #f-ballet:checked  ~ .agenda .day:not(:has(.row.cat-ballet:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-ballet:checked  ~ .agenda .week:not(:has(.row.cat-ballet:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-ballet:checked  ~ .featured:not(:has(.featured-card.cat-ballet)) {{ display: none; }}
    #f-ballet:checked  ~ .agenda:not(:has(.row.cat-ballet:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-ballet:checked ~ .agenda .day:has(.row.cat-ballet:not(.venue-hidden)),
    #show-extras:checked ~ #f-ballet:checked ~ .agenda .week:has(.row.cat-ballet:not(.venue-hidden)) {{ display: block !important; }}
    /* Theatre */
    #f-theatre:checked ~ .featured .featured-card:not(.cat-theatre),
    #f-theatre:checked ~ .agenda .row:not(.cat-theatre),
    #f-theatre:checked ~ .agenda .day:not(:has(.row.cat-theatre:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-theatre:checked ~ .agenda .week:not(:has(.row.cat-theatre:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-theatre:checked ~ .featured:not(:has(.featured-card.cat-theatre)) {{ display: none; }}
    #f-theatre:checked ~ .agenda:not(:has(.row.cat-theatre:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-theatre:checked ~ .agenda .day:has(.row.cat-theatre:not(.venue-hidden)),
    #show-extras:checked ~ #f-theatre:checked ~ .agenda .week:has(.row.cat-theatre:not(.venue-hidden)) {{ display: block !important; }}
    /* Film */
    #f-film:checked    ~ .featured .featured-card:not(.cat-film),
    #f-film:checked    ~ .agenda .row:not(.cat-film),
    #f-film:checked    ~ .agenda .day:not(:has(.row.cat-film:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-film:checked    ~ .agenda .week:not(:has(.row.cat-film:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-film:checked    ~ .featured:not(:has(.featured-card.cat-film)) {{ display: none; }}
    #f-film:checked    ~ .agenda:not(:has(.row.cat-film:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-film:checked ~ .agenda .day:has(.row.cat-film:not(.venue-hidden)),
    #show-extras:checked ~ #f-film:checked ~ .agenda .week:has(.row.cat-film:not(.venue-hidden)) {{ display: block !important; }}
    /* Exhibition */
    #f-exh:checked     ~ .featured .featured-card:not(.cat-exh),
    #f-exh:checked     ~ .agenda .row:not(.cat-exh),
    #f-exh:checked     ~ .agenda .day:not(:has(.row.cat-exh:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-exh:checked     ~ .agenda .week:not(:has(.row.cat-exh:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    #f-exh:checked     ~ .featured:not(:has(.featured-card.cat-exh)) {{ display: none; }}
    #f-exh:checked     ~ .agenda:not(:has(.row.cat-exh:not(.audience-kids):not(.audience-active):not(.venue-hidden))) .filter-empty {{ display: block; }}
    #show-extras:checked ~ #f-exh:checked ~ .agenda .day:has(.row.cat-exh:not(.venue-hidden)),
    #show-extras:checked ~ #f-exh:checked ~ .agenda .week:has(.row.cat-exh:not(.venue-hidden)) {{ display: block !important; }}
    /* When NO category filter is active, still hide days/weeks that contain only
       audience-hidden rows (e.g. a school-concert-only day in default view). */
    .agenda .day:not(:has(.row:not(.audience-kids):not(.audience-active):not(.venue-hidden))),
    .agenda .week:not(:has(.row:not(.audience-kids):not(.audience-active):not(.venue-hidden))) {{ display: none; }}
    #show-extras:checked ~ .agenda .day:has(.row:not(.venue-hidden)),
    #show-extras:checked ~ .agenda .week:has(.row:not(.venue-hidden)) {{ display: block !important; }}
    /* City filters — emitted dynamically per city present in data */
    {city_css}

    /* Featured */
    .featured {{ margin-bottom: 48px; }}
    .featured-heading {{
      font-size: 13px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      color: var(--accent);
      margin: 0 0 18px;
    }}
    .featured-heading .star {{ font-size: 16px; margin-right: 6px; }}
    .featured-grid {{
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 14px;
    }}
    .featured-card {{
      display: block;
      position: relative;
      background: var(--paper);
      border: 1px solid var(--rule);
      border-radius: 10px;
      overflow: hidden;
      text-decoration: none;
      color: inherit;
      transition: transform 140ms ease, box-shadow 140ms ease;
    }}
    .featured-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 8px 24px rgba(0,0,0,0.08);
    }}
    .fc-band {{ height: 6px; width: 100%; }}
    .featured-card.cat-exh     .fc-band {{ background: var(--c-exh); }}
    .featured-card.cat-film    .fc-band {{ background: var(--c-film); }}
    .featured-card.cat-opera   .fc-band {{ background: var(--c-opera); }}
    .featured-card.cat-ballet  .fc-band {{ background: var(--c-ballet); }}
    .featured-card.cat-concert .fc-band {{ background: var(--c-concert); }}
    .featured-card.cat-theatre .fc-band {{ background: var(--c-theatre); }}
    .featured-card.cat-vern    .fc-band {{ background: var(--c-vern); }}
    .featured-card.cat-other   .fc-band {{ background: var(--c-other); }}
    .fc-inner {{ padding: 18px 20px 20px; }}
    .fc-pill {{ margin-bottom: 12px; }}
    .fc-title {{
      font-size: 22px;
      font-weight: 700;
      line-height: 1.25;
      letter-spacing: -0.01em;
      margin-bottom: 8px;
    }}
    .fc-venue {{ font-size: 14px; color: var(--muted); margin-bottom: 4px; }}
    .fc-date {{ font-size: 14px; color: var(--accent); font-weight: 600; margin-bottom: 8px; }}
    .fc-desc {{ font-size: 14px; color: var(--muted); margin: 10px 0 12px; line-height: 1.45; }}
    .fc-meta {{ display: flex; justify-content: flex-end; align-items: center; font-size: 14px; color: var(--muted); }}
    .fc-meta .arrow {{ font-size: 18px; }}
    /* Agenda */
    .agenda {{ }}
    .week-heading {{
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      color: var(--muted);
      margin: 32px 0 16px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--rule);
    }}
    .day {{ margin-bottom: 24px; }}
    .day-heading {{
      font-size: 14px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--ink);
      margin: 0 0 8px;
    }}
    .row {{
      display: grid;
      grid-template-columns: 70px 1fr auto auto 18px;
      gap: 14px;
      align-items: center;
      padding: 14px 10px;
      text-decoration: none;
      color: inherit;
      border-radius: 6px;
      transition: background 100ms ease;
    }}
    .row:hover {{ background: var(--hover); }}
    .row + .row {{ border-top: 1px solid var(--rule); }}
    .row-time {{
      font-variant-numeric: tabular-nums;
      font-weight: 600;
      font-size: 17px;
      color: var(--ink);
      align-self: start;
      padding-top: 2px;
    }}
    .row-body {{ min-width: 0; align-self: start; }}
    .row-title {{
      font-size: 17px;
      font-weight: 600;
      line-height: 1.3;
      margin-bottom: 4px;
      overflow-wrap: anywhere;
    }}
    .row-venue {{
      font-size: 14px;
      color: var(--muted);
      margin-bottom: 2px;
    }}
    .row-meta {{
      font-size: 13px;
      color: var(--muted);
    }}
    .row-meta .relative {{ color: var(--accent); font-weight: 500; }}
    .row-arrow {{ color: var(--muted); font-size: 18px; padding-top: 2px; }}
    .featured-row .row-title {{ }}
    .inline-star {{ color: var(--accent); }}
    /* Footer */
    footer {{
      margin-top: 48px;
      padding-top: 16px;
      border-top: 1px solid var(--rule);
      text-align: center;
      font-size: 12px;
      color: var(--muted);
    }}
    .empty {{ color: var(--muted); }}
    /* Per-category color accents.
       Each category gets a colored 3px left stripe on the row + tinted icon.
       Colors are mid-saturation hues that read on both light and dark themes. */
    :root {{
      --c-exh: #b8860b;       /* amber */
      --c-film: #1f6f8b;      /* film teal */
      --c-opera: #962d3e;     /* deep red */
      --c-ballet: #b8527c;    /* rose */
      --c-concert: #2c4a6b;   /* deep blue */
      --c-theatre: #3d5a3a;   /* forest green */
      --c-vern: #6b3a87;      /* purple */
      --c-other: #9b9482;     /* muted gray */
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --c-exh: #d9a93b;
        --c-film: #5fa9c4;
        --c-opera: #c97e8c;
        --c-ballet: #d893b3;
        --c-concert: #7da3c8;
        --c-theatre: #88a585;
        --c-vern: #b596d4;
        --c-other: #9b9482;
      }}
    }}
    /* Category pill — colored chip with icon + caps label, sits on top-right of each row.
       Background = soft tint of category color (low alpha), text/icon = full saturation. */
    .cat-pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 10px 5px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      white-space: nowrap;
      border: 1px solid transparent;
      align-self: start;
    }}
    .cat-pill.cat-exh     {{ background: rgba(184,134,11,0.12);  color: var(--c-exh);     border-color: rgba(184,134,11,0.25); }}
    .cat-pill.cat-film    {{ background: rgba(31,111,139,0.12);  color: var(--c-film);    border-color: rgba(31,111,139,0.25); }}
    .cat-pill.cat-opera   {{ background: rgba(150,45,62,0.12);   color: var(--c-opera);   border-color: rgba(150,45,62,0.25); }}
    .cat-pill.cat-ballet  {{ background: rgba(184,82,124,0.12);  color: var(--c-ballet);  border-color: rgba(184,82,124,0.25); }}
    .cat-pill.cat-concert {{ background: rgba(44,74,107,0.12);   color: var(--c-concert); border-color: rgba(44,74,107,0.25); }}
    .cat-pill.cat-theatre {{ background: rgba(61,90,58,0.12);    color: var(--c-theatre); border-color: rgba(61,90,58,0.25); }}
    .cat-pill.cat-vern    {{ background: rgba(107,58,135,0.12);  color: var(--c-vern);    border-color: rgba(107,58,135,0.25); }}
    .cat-pill.cat-other   {{ background: rgba(155,148,130,0.12); color: var(--c-other);   border-color: rgba(155,148,130,0.25); }}
    @media (prefers-color-scheme: dark) {{
      .cat-pill.cat-exh     {{ background: rgba(217,169,59,0.18); }}
      .cat-pill.cat-opera   {{ background: rgba(201,126,140,0.18); }}
      .cat-pill.cat-ballet  {{ background: rgba(216,147,179,0.18); }}
      .cat-pill.cat-concert {{ background: rgba(125,163,200,0.18); }}
      .cat-pill.cat-theatre {{ background: rgba(136,165,133,0.18); }}
      .cat-pill.cat-vern    {{ background: rgba(181,150,212,0.18); }}
    }}
    .cat-icon {{ display: inline-flex; align-items: center; font-size: 14px; line-height: 1; }}
    .cat-pill .cat-label {{ display: inline-block; }}
    /* Favorite heart button — sits between category pill and arrow */
    .fav-btn {{
      background: transparent;
      border: 0;
      padding: 4px 8px;
      margin: 0;
      font: inherit;
      font-size: 18px;
      line-height: 1;
      color: var(--muted);
      cursor: pointer;
      border-radius: 50%;
      transition: color 100ms ease, background 100ms ease, transform 80ms ease;
      align-self: start;
    }}
    .fav-btn:hover {{ background: var(--hover); color: #d63a3a; }}
    .fav-btn:active {{ transform: scale(0.85); }}
    .fav-btn.is-fav,
    a.row.is-fav .fav-btn,
    a.featured-card.is-fav .fav-btn {{ color: #d63a3a; }}
    /* "NEU" badge — small red label inline before the title */
    .new-badge {{
      display: inline-block;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.06em;
      padding: 1px 6px 2px;
      background: #d63a3a;
      color: white;
      border-radius: 3px;
      margin-right: 8px;
      vertical-align: 2px;
    }}
    .new-badge[hidden] {{ display: none; }}
    /* Featured-card heart sits in the corner */
    .fc-fav {{
      position: absolute;
      top: 12px;
      right: 12px;
      z-index: 2;
    }}
    /* Favoriten filter chip — slightly emphasized */
    .filter-chip-fav .cat-icon {{ color: #d63a3a; }}
    #f-fav:checked ~ .filter-panel .filter-bar .filter-chip[for="f-fav"] {{
      background: #d63a3a !important;
      color: white !important;
      border-color: #d63a3a !important;
    }}
    /* Wann filter — emitted dynamically per WHEN_OPTIONS slot */
    {when_css}
    /* Favoriten filter — hide rows + cards + days/weeks without is-fav */
    #f-fav:checked ~ .agenda .row:not(.is-fav),
    #f-fav:checked ~ .agenda .day:not(:has(.row.is-fav:not(.venue-hidden))),
    #f-fav:checked ~ .agenda .week:not(:has(.row.is-fav:not(.venue-hidden))),
    #f-fav:checked ~ .featured .featured-card:not(.is-fav),
    #f-fav:checked ~ .featured:not(:has(.featured-card.is-fav)) {{ display: none; }}
    #f-fav:checked ~ .agenda:not(:has(.row.is-fav:not(.venue-hidden))) .filter-empty {{ display: block; }}
    /* Recurring-event bonus line under main meta */
    .row-recurring {{
      font-size: 12px;
      color: var(--muted);
      margin-top: 4px;
      font-style: italic;
    }}
    /* De-emphasized rows (educational + kids/active when revealed): dimmer, smaller */
    .row.de-emphasized {{
      opacity: 0.55;
      border-left-color: transparent !important;
    }}
    .row.de-emphasized .row-title {{ font-size: 15px; font-weight: 400; }}
    .row.de-emphasized .row-time {{ font-weight: 400; }}
    .row.de-emphasized:hover {{ opacity: 0.85; }}
    /* Kids + active (workshops, classes, family programme) — hidden by default;
       toggled visible by the "Auch Kurse..." checkbox. When visible, they're dimmed. */
    .row.audience-kids,
    .row.audience-active,
    .featured-card.audience-kids,
    .featured-card.audience-active {{
      display: none;
    }}
    #show-extras:checked ~ .agenda .row.audience-kids,
    #show-extras:checked ~ .agenda .row.audience-active {{
      display: grid;
    }}
    #show-extras:checked ~ .featured .featured-card.audience-kids,
    #show-extras:checked ~ .featured .featured-card.audience-active {{
      display: block;
    }}
    /* Per-venue chips — hidden by default, revealed when a specific city is picked */
    .venue-chips {{
      display: none;
      flex-wrap: wrap;
      align-items: center;
      gap: 5px;
      margin: -2px 0 24px;
    }}
    /* `display: flex` show rule for each venue-chips block emitted dynamically below */
    /* When a category filter is active, hide chips for venues that don't have
       events in that category — keeps the Häuser row focused on relevant houses. */
    #f-opera:checked   ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-opera)   {{ display: none; }}
    #f-concert:checked ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-concert) {{ display: none; }}
    #f-ballet:checked  ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-ballet)  {{ display: none; }}
    #f-theatre:checked ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-theatre) {{ display: none; }}
    #f-exh:checked     ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-exh)     {{ display: none; }}
    #f-film:checked    ~ .filter-panel .venue-chips .venue-chip:not(.has-cat-film)    {{ display: none; }}
    /* "Alle" reset button — same chip shape, slightly emphasized on hover */
    /* "Reset" button — `.active` is toggled by JS when no venue chips are
       checked, so it visually reads as the current selection. */
    .venue-chip-all:hover,
    .venue-chip-all.active {{ background: var(--ink); color: var(--bg); border-color: var(--ink); }}
    .venue-checkbox {{
      width: 10px;
      height: 10px;
      border-radius: 2px;
      background: transparent;
      border: 1px solid var(--rule);
      flex-shrink: 0;
      box-sizing: border-box;
      transition: background 100ms ease, border-color 100ms ease;
    }}
    /* JS sets .venue-hidden on rows/cards whose venue isn't in the active selection */
    .venue-hidden {{ display: none !important; }}
    /* Per-chip "is checked" visual styles emitted dynamically below */
    {venue_css}
    /* Extras toggle styling */
    .extras-toggle-wrapper {{
      margin: -20px 0 28px;
    }}
    .extras-toggle {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      cursor: pointer;
      user-select: none;
      font-size: 13px;
      color: var(--muted);
      padding: 6px 10px;
      border-radius: 6px;
      transition: background 100ms ease, color 100ms ease;
    }}
    .extras-toggle:hover {{ background: var(--hover); color: var(--ink); }}
    .extras-checkbox {{
      width: 16px;
      height: 16px;
      border: 1.5px solid var(--rule);
      border-radius: 3px;
      background: var(--paper);
      position: relative;
      flex-shrink: 0;
      transition: background 100ms ease, border-color 100ms ease;
    }}
    #show-extras:checked ~ .filter-panel .extras-toggle-wrapper .extras-checkbox,
    #show-extras:checked ~ .filter-panel .extras-toggle-wrapper .extras-toggle:hover .extras-checkbox {{
      background: var(--ink);
      border-color: var(--ink);
    }}
    #show-extras:checked ~ .filter-panel .extras-toggle-wrapper .extras-checkbox::after {{
      content: "";
      position: absolute;
      left: 4px;
      top: 0;
      width: 5px;
      height: 10px;
      border: solid var(--bg);
      border-width: 0 2px 2px 0;
      transform: rotate(45deg);
    }}
    #show-extras:checked ~ .filter-panel .extras-toggle-wrapper .extras-toggle {{
      color: var(--ink);
    }}
    /* Mobile tweaks */
    @media (max-width: 640px) {{
      body {{ font-size: 16px; }}
      .page {{ padding: 20px 16px 60px; }}
      .masthead h1 {{ font-size: 32px; }}
      .featured-grid {{ grid-template-columns: 1fr; }}
      /* On mobile, drop the pill column from the grid and let it wrap to a new row */
      .row {{ grid-template-columns: 60px 1fr auto 18px; grid-template-rows: auto auto; gap: 10px 12px; padding: 12px 8px; }}
      .row-time {{ grid-column: 1; grid-row: 1; font-size: 16px; }}
      .row-body {{ grid-column: 2 / span 2; grid-row: 1; }}
      .cat-pill {{ grid-column: 2; grid-row: 2; justify-self: start; }}
      .fav-btn {{ grid-column: 3; grid-row: 2; justify-self: end; }}
      .row-arrow {{ grid-column: 4; grid-row: 1; }}
      .row-title {{ font-size: 18px; }}
      .fc-title {{ font-size: 22px; }}
    }}
  </style>
</head>
<body>
"""
