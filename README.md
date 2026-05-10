# events-la

Cultural events calendar for Los Angeles + Greater LA County — museums, opera, ballet, classical concerts, theatre, jazz, repertory cinema.

**Live page:** https://fengelh2.github.io/events-la/

Auto-rebuilds nightly via GitHub Actions. State (favorites, "new since last visit" badges) lives in browser localStorage; no backend.

Sister project of [momevents](https://github.com/fengelh2/momevents) (German cultural calendar for Essen + Düsseldorf). Codebases are independent.

## Local development

```sh
pip install -r requirements.txt
python tools/rebuild_calendar.py --verbose
```

Output: `.tmp/events.html`. Serve locally:

```sh
cd .tmp && python -m http.server 18081
# then open http://localhost:18081/events.html
```

## Architecture

The scraper is a parametric layer over per-venue config in `config/venues.yaml`. Each venue declares `kind: ical | html_list | detail_pages | static` plus selectors / regex helpers. See [CLAUDE.md](CLAUDE.md) for goals, gotchas, scrape landscape.

Tier-1 onboarding starts with **Discover Los Angeles** (~600 JSON-LD Event entities/page) plus a Tessitura adapter that unlocks LA Phil, LA Opera, CTG, Wallis, and Geffen with one effort.
