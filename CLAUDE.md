# Project: events-la

A weekly cultural-events calendar covering LA proper + Greater LA County. Aggregates upcoming events from major museums, opera, ballet, classical concerts, theatre, plus a few specialized venues (jazz clubs, arthouse cinemas).

Sister project of [momEvents](https://github.com/fengelh2/momevents) (German equivalent for Essen + Düsseldorf). Codebases are independent; the LA project has its own self-contained `tools/` so improvements don't ripple between locales.

## Goal

A simple, easy-to-read web page rebuilt nightly that lists upcoming cultural events. Filters: **Where** (neighborhood) · **What** (category) · **When** (this week / weekend / month / next) · **Venues** (multi-select, click to add). Free-text search + favorites + "NEW since last visit" badges via localStorage. Mobile-first, no JS frameworks, deploys to GitHub Pages.

## Inputs

- `config/venues.yaml` — venue master list. Each entry declares `kind` (`ical` | `html_list` | `detail_pages` | `static`) plus selectors / regex / pagination helpers.
- `config/highlights.yaml` — featured-keyword list (US-relevant: artists, composers, conductors, blockbuster show names) + manual featured_events for specific runs.

## Outputs

- `.tmp/events.html` — single static page, rebuilt nightly. Deployed to `gh-pages` branch by GitHub Actions; served at `https://<user>.github.io/events-la/`.

## Architecture (WAT)

Self-contained inside this project — no shared dependencies on the workspace `tools/` (those serve momEvents):

- `tools/scrape_venue_events.py` — parametric scraper, dispatches on `kind`. Handles iCal (single .ics or per-event), HTML selectors with attribute-syntax + multi-month pagination + day-carry-forward + month-context, detail-page following, static fixtures.
- `tools/parse_ical.py` — `.ics` discovery + parse.
- `tools/render_events_html.py` — Apple-agenda layout + filter panel + favorites JS. **English UI, US date format (MM/DD/YYYY).**
- `tools/rebuild_calendar.py` — orchestrator + highlights logic.

## Canonical Event schema

```yaml
title: str            # English
start: datetime       # ISO 8601, with timezone (America/Los_Angeles)
end: datetime | null
venue_id: str
venue_name: str
city: str             # neighborhood: Hollywood / DTLA / Westside / Pasadena / Santa Monica / Long Beach / Beverly Hills / etc.
category: str         # museum_exhibition | opera | ballet | concert | theatre | film | vernissage | other
url: str
description: str | null
audience: str         # general | educational | active | kids
```

## Scrape landscape (per LA reconnaissance, 2026-05-10)

- **iCal exposure: rare.** 0/8 LA tier-1 venues publish `.ics` URLs. Different cultural norm than German venues — don't waste time grepping for it.
- **JSON-LD `@type: Event`: rare on individual venue sites** but **abundant on aggregators**. Discover Los Angeles publishes ~600 JSON-LD Event entities on its `/events` page — single best aggregator find.
- **Tessitura ticketing platform** is the dominant LA pattern (LA Phil, LA Opera, CTG, Wallis, Geffen, Pasadena Playhouse). TNEW v15+ subdomains (`my.<venue>.org` or `tickets.<venue>.org`) render structured calendars. Building one TNEW adapter unlocks ~5-8 venues.
- **WordPress + The Events Calendar plugin (Tribe REST API)** is exposed at `/wp-json/tribe/events/v1/events` on at least Pasadena Playhouse — cleanest single-venue source.
- **JS-only / Playwright-required:** Center Theatre Group (queue-it bot wall), KCRW events.
- **Server-rendered cards:** LACMA, Getty, Hammer.

## Date conventions (DIFFERS from momEvents)

- US format: MM/DD/YYYY. Set in `_DATE_PARSER_KW` as `DATE_ORDER='MDY'`.
- Tessitura HTML and Tribe REST use ISO 8601 internally — `_parse_one` tries `fromisoformat` first.
- Times stored as wall-clock America/Los_Angeles; localize on read, don't assume UTC.
- Display: 12-hour clock with AM/PM ("7:30 PM"), date as "May 9, 2026" / "Sunday, May 10".

## Geographic scope — 4 zones

LA traffic makes travel time the binding filter, not literal neighborhood. Use 4
zones (decided 2026-05-10) so each chip has 3-5 venues and matches how
Angelenos talk about distance:

- **Westside** — Hammer, Getty Center, Getty Villa (Pacific Palisades),
  Geffen Playhouse, Royce Hall, Broad Stage (Santa Monica), Skirball
  (Sepulveda Pass), Wallis (Beverly Hills)
- **Central LA** — LACMA (Mid-Wilshire), MOCA, Disney Hall, LA Phil,
  LA Opera, CTG (Music Center), Hollywood Bowl, American Cinematheque,
  The Broad
- **Pasadena & East** — Pasadena Playhouse, Norton Simon, Huntington
  (San Marino), A Noise Within, LACO (Glendale), Pasadena Symphony
- **Greater LA** — Long Beach Symphony, MOLAA, Soraya (Northridge),
  outliers >20mi from DTLA

Drop venues into one of these four. Edge cases: Sepulveda Pass / Skirball goes
to Westside (cultural circuit, not Valley); Glendale goes to Pasadena & East
(traffic and culture lean east); Pacific Palisades is Westside (Getty Villa
shares programming with the Center).

Discover LA aggregator events arrive with literal `addressLocality` (e.g.
"Pasadena", "Long Beach", "Hollywood"). The `json_ld_aggregator` parser maps
these to the 4 zones via a city-zone table inside the parser.

## Onboarding order (Tier-1)

1. **Discover Los Angeles** — JSON-LD Event aggregator, ~600 events/page. Onboard first.
2. **Pasadena Playhouse** — Tribe Events REST API (`/wp-json/tribe/events/v1/events`). 0-friction.
3. **LACMA** — server-rendered `card-event`.
4. **Getty Center + Villa** — server-rendered `calendar-event` tiles, single page covers both.
5. **Hammer Museum** — `result-item--program` cards with paginated `start_date` query.
6. **Wallis Annenberg** + Tessitura adapter (unlocks LA Phil, LA Opera, Geffen later).
7. **American Cinematheque** — WordPress RSS feed.

## Gotchas / non-obvious notes

- **No iCal hunting.** Reconnaissance showed 0/8 tier-1 venues expose `.ics`. Skip the grep step in onboarding.
- **Tessitura adapter is high-leverage.** Build it once; reuse for 5-8 venues.
- **Bot walls.** Center Theatre Group uses queue-it. May require Playwright. Skip until needed.
- **Time zones matter.** All event times are `America/Los_Angeles`. Display in local; don't UTC-shift.
- **Discover LA is the seeding play.** Onboarding it first means the calendar has 600+ events on day one without per-venue scrapers.
