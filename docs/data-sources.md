# Data sources (checked 2026-11-15)

## Deadlock API data lake
- DuckLake catalog: `ducklake:https://data.deadlock-api.com/v1/catalog.ducklake` (read-only, hourly updates).
  Manifest: `https://data.deadlock-api.com/v1/manifest.json`. Needs duckdb >= 2.0 with `ducklake` and `httpfs`.
- Tables: `match_player` (one row per player per match), `leaderboard`, `hero_leaderboard`.
- ~1.24M ranked matches from 2026-09-29 to 2026-11-15 (about 30k to 40k per day lately).
- `average_badge_team0/1` is **0 for every ranked match** since 2026-09-25: no usable rank filter in the lake.
- `stats.*` timeseries has only 10 snapshots per match (180s, 360s, ..., then every 300s). Too coarse for macro.
- Also has `death_details` (positions, times), `items` (purchase and sell times), objective and Mid Boss event times.

## Rank
- `GET /v1/players/rank?account_ids=a,b,c` (up to 1000). Limits: 10k accounts/min and 100k/h per IP.
- Returns the player's **current** rank (end of latest ranked match), not rank at match time.
- About 13% of sampled accounts return `badge=0` (unranked or uncalibrated).
- Badge = tier*10 + subrank; tiers 0 Obscurus ... 7 Emissary, 8 Oracle, 9 Phantom, 10 Ascendant, 11 Eternus.

## Replays
- `GET /v1/matches/{id}/salts` -> `demo_url` (null for some matches; 98 of 150 sampled available, about 65%).
- File is 210 to 270 MB and named `.dem.bz2` but is **zstd**, not bzip2.
- Server-side alternative (untested): `POST /v1/matches/demo/query` runs SQL against a demo and returns a Parquet URL
  (about 13s per match); schema at `GET /v1/matches/demo/schema`.

## Parser: boon (`boon-deadlock` on PyPI)
- Use boon 0.11 or later for demos recorded after the 2026-09-29 "City Never Sleeps" update.
- Decompress the zstd file first, then `Demo(path, preload=False)`.
- `snapshots("player_ticks", ticks=[...])` for per-player state; datasets include `objectives`, `mid_boss`, `kills`,
  `teamfights`, `neutrals`, `item_purchases`, `enemy_minimap_ticks`. Tick rate 64.
- Parse cost on a 32-minute match: about 0.1s open, 1.5 to 2s per dataset.

## Replay expiry (verified 2026-11-17)

Valve replay URLs return 404 for matches older than about 30 days (HEAD check: all 404 up to 2026-10-16, mixed on 17-18 Oct, all 200 from 19 Oct). `salts` still returns a `demo_url` for expired matches, so `select_matches.py` HEAD-checks each URL.

## Lake query hang

A `group by match_id` over `match_player` that returns rows hangs for windows longer than about 12 hours (12h: 1s; 18h+: never returns). Scan in 6h chunks.
