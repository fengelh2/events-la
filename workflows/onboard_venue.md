# Workflow: onboard a new venue

**Trigger:** A new venue is added to `config/venues.yaml` with `kind: unknown`. Or an existing venue's `kind` is set but breaking.

**Goal:** Discover the cheapest viable extraction strategy for this venue's calendar, in this order: iCal → JSON-LD → handwritten selectors. Update the venue's row in `venues.yaml` in place. **Do not create a new file; do not write a per-venue scraper.**

## Inputs

- One row from `projects/momEvents/config/venues.yaml`.
- The venue's `calendar_url` (or, if missing, a guess based on conventions: `/spielplan`, `/programm`, `/veranstaltungen`, `/ausstellungen`, `/kalender`, `/termine`).

## Steps

### 1. Probe for iCal (preferred — no parsing needed)

```bash
# Fetch the calendar page
curl -sL "$CALENDAR_URL" > /tmp/probe.html

# Look for .ics URLs — both linked and constructed
grep -Eo '[^"]*\.ics' /tmp/probe.html | sort -u
grep -Eoi '(href|src)="[^"]*ical[^"]*"' /tmp/probe.html | sort -u

# TYPO3 spiritec convention (Theater Essen + likely other NRW theatres):
# look for /kalender-eintrag/{id}/ical-{date}-{id}.ics
grep -Eo '/kalender-eintrag/\d+/ical-\d{4}-\d{2}-\d{2}-\d+\.ics' /tmp/probe.html
```

If you find `.ics` URLs, fetch one. Confirm it parses as RFC 5545 (`BEGIN:VCALENDAR` / `BEGIN:VEVENT` / `DTSTART` / `SUMMARY`). Then update `venues.yaml`:

```yaml
kind: ical
ical_pattern: '<the regex you matched>'    # used by tools/scrape_venue_events.py to extract URLs from the listing page
```

**Stop here.** No further selectors needed.

### 2. Probe for JSON-LD `Event` (cheap upside)

```bash
# Look for embedded structured data
grep -A 200 'application/ld\+json' /tmp/probe.html | grep -E '"@type"\s*:\s*"Event"'
```

If found, also fetch a couple of detail pages (deeper-linked event URLs) and check for JSON-LD there — sometimes the listing page has nothing but each detail page is rich.

If a detail page has `"@type":"Event"` with `startDate`, `endDate`, `name`, `location`, update:

```yaml
kind: json_ld
selectors:
  detail_link: 'a.event-link'   # CSS selector that finds detail-page URLs from the listing page
```

The shared scraper handles JSON-LD parsing via `extruct` once `kind: json_ld` is set.

### 3. Handwritten selectors (the default fallback)

Open the calendar URL in a browser. Use DevTools to identify:

- **Item selector** — repeating element for each event in the listing.
- **Title selector** — relative to the item.
- **Date string selector** — relative to the item.
- **Detail link selector** — relative to the item.
- **Date format** — German formats are typical: `21. Juni 2026`, `Sa, 14.05.2026 19:30`, `14.05.–30.06.2026` (range for exhibitions). Capture the literal Python format string for `dateparser`/`strptime`.

Update:

```yaml
kind: html_list
selectors:
  item: 'article.event-card'
  title: 'h3.event-title'
  date: '.event-date'
  detail_link: 'a.event-link'
date_format: '%d. %B %Y'    # or "german_natural" if dateparser is doing the work
```

### 4. Validate

Run a one-off probe to confirm the venue produces ≥1 future event:

```bash
python tools/scrape_venue_events.py --venue-id <id> --once --dry-run
```

Expected output: a list of upcoming events with title + start datetime + URL. If empty or wrong, fix the selectors and rerun. **Do not commit broken selectors and rely on the nightly run to catch them.**

### 5. Edge cases — flag these in `notes:` rather than coding around them

- **Listing requires JS** — set `requires_js: true`. The shared scraper falls back to Playwright. (Add Playwright to project deps before flipping this.)
- **Listing is paginated with "load more"** — set `pagination: load_more` and add `pagination_param:` if the URL changes. Most venues with <100 upcoming events fit on the first page.
- **Multi-stage house** (TUP, Wuppertaler Bühnen, Theater Dortmund) — populate `produces_venue_ids:` and add a `stage_resolver` regex/keyword map in `notes:`. The merger uses this to route each event to the right `venue_id`.
- **Vernissages on private galleries (Sies+Höke, Konrad Fischer)** — these often only appear on Instagram or buried in "Aktuelles" pages. Best-effort. If the website has nothing structured, leave `kind: unknown` and skip — don't write fragile selectors that break monthly.

## Output

- Updated `venues.yaml` row with `kind` set to a real value.
- Optionally: a new entry in `venue_aliases.json` if the venue's name varies between its own site and aggregators (RuhrBühnen, visitessen).

## Self-improvement

If you discover a German CMS pattern that's reused across many venues (e.g., spiritec / TYPO3 calendar modules expose `.ics` consistently), document the pattern in this workflow under "Probe for iCal" so the next onboarding attempt knows to try it first. Don't make the discovery silently — update the SOP.
