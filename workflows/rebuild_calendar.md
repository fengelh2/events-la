# Workflow: rebuild calendar (nightly)

**Trigger:** Scheduled run (Windows Task Scheduler nightly), OR manual `python tools/rebuild_calendar.py`.

**Goal:** Produce a fresh `events.html` (and side-artifact `events.ics`) that lists all upcoming events from venues in `config/venues.yaml`, deduplicated, grouped by week and city.

## Inputs

- `projects/momEvents/config/venues.yaml`
- `projects/momEvents/config/venue_aliases.json`
- Network access to all venue calendar URLs

## Steps

### 1. Scrape each venue (parallel, with per-venue isolation)

For each row in `venues.yaml` where `kind != unknown`:

```python
events = tools.scrape_venue_events.scrape(venue_row)
# returns: list[CanonicalEvent] or [] on failure (failure logged, not raised)
```

Per-venue isolation: **a single venue scraping failure must not abort the run.** Log it, increment a counter, move on. The output report shows which venues failed.

Skip rows with `kind: unknown` — those are pending onboarding (see `onboard_venue.md`).

Cache raw responses in `projects/momEvents/.tmp/raw/<venue_id>/<YYYY-MM-DD>.html` for debug. Do not use these as a fallback if the live fetch fails (stale data is worse than missing data here).

### 2. Map multi-stage houses to per-stage venue_ids

For venues with `produces_venue_ids:`, route each scraped event to the correct `venue_id` using `stage_resolver` (regex or keyword map in the venue's `notes:`):

- TUP Essen → `aalto-essen` | `grillo-essen` | `philharmonie-essen` based on the `location` field in the .ics or the `<venue>` substring in the title.
- Deutsche Oper am Rhein → `oper-duesseldorf` | `oper-duisburg` based on the venue location.
- Wuppertaler Bühnen → `oper-wuppertal` | `schauspiel-wuppertal` | `sinfonieorchester-wuppertal`.
- Theater Dortmund → `oper-dortmund` | `ballett-dortmund` | `schauspiel-dortmund` | `dortmunder-philharmoniker`.

If the resolver can't decide, route to the parent `id` (e.g., `theater-dortmund`) and log a warning. **Do not silently drop events.**

### 3. Categorize each event

Each event needs `category ∈ {museum_exhibition, opera, ballet, concert, theatre, vernissage, other}`. Resolution order:

1. If the iCal/JSON-LD source already has a category-like field, use it.
2. Otherwise, use the venue's default `category` from `venues.yaml`.
3. If the venue is `mixed`, infer from title keywords: `Premiere|Aufführung|Vorstellung` → drama; `Sinfonie|Konzert|Orchester` → concert; `Oper|opera` → opera; `Ballett|Tanz` → ballet; `Eröffnung|Vernissage|Ausstellungseröffnung` → vernissage; default → other.

### 4. Deduplicate

Build dedup key:

```python
key = (
    normalize_title(e.title),       # lowercase, strip punctuation, drop "Premiere:" / "Vorstellung:" prefixes
    e.start.date(),                 # date only, not time
    canonical_venue_id(e.venue_name)  # via venue_aliases.json lookup
)
```

For collisions, keep the entry with the lowest `source_priority`. Tiebreak by longer `description`.

**Fuzzy second pass:** within `(date, venue_id)` groups that survived, run `rapidfuzz.fuzz.token_set_ratio` over the titles. Pairs ≥85 are merged using the same priority rule.

Log every dedup decision (which entry won, which lost) to `.tmp/dedup_log.jsonl` for debugging.

### 5. Filter

- Drop events with `start < today`.
- Drop events with `start > today + 90 days` (keep the page focused on the near future; the long tail is noise).
- Drop events whose `city` is `__aggregator__` AND no `venue_alias_match` was found — these are aggregator entries we couldn't pin to a known venue and would only confuse the user.

### 6. Render

```python
tools.render_events_html.render(
    events,
    out_path="projects/momEvents/.tmp/events.html",
    title="Was ist los — diese Wochen",
    week_groups=True,         # group by ISO week
    secondary_sort="city",    # within each week, group by city
    locale="de_DE",
)
```

Page requirements:
- Single column, large readable type, mobile-friendly. The user is 60+.
- Each event row shows: date + time + title + venue + city + (optional) category emoji.
- Each event title links to its detail URL.
- A small "last updated" line at the top with the run timestamp.
- No JavaScript required to view.

Also generate `events.ics` from the same event list as a side-artifact. No extra cost.

### 7. Publish

(Hosting target: TBD — see `CLAUDE.md`. For now, write to `.tmp/events.html` only and wait for the user's call on hosting.)

When hosting is decided:
- GitHub Pages — push `events.html` + `events.ics` to a `gh-pages` branch.
- Cloudflare Pages — same artefact, different remote.
- Local file — copy to a OneDrive folder the user has shared with mum.

### 8. Run report

Print a summary to stdout (and pin to `.tmp/run_report.md`):

```
Run 2026-05-09T03:00 — 33 venues, 31 OK, 2 failed
  Failed:
    - kunstpalast (selector miss: 'article.event-card' returned 0)
    - sies-hoeke (HTTP 503)
  Events: 412 raw → 387 after dedup (25 dupes) → 312 after future-only filter
  Output: .tmp/events.html (87 KB), .tmp/events.ics (43 KB)
```

A failed venue is **not a workflow failure** — but if more than 5 venues fail or the total event count drops by >30% vs. the prior run, exit with non-zero so the Task Scheduler logs it.

## Self-improvement

- If a venue fails 3 nights in a row, demote its `kind` back to `unknown` so it skips the next run, and add a TODO in `notes:`. Tell the user.
- If dedup keeps merging events that aren't actually the same (false positives), log examples and lower the rapidfuzz threshold.
- If the `mixed` keyword categorizer misclassifies, add the keyword to a per-venue override.
