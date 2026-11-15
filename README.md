# deadlock-fruit-fly

teaching a neural network to make macro decisions in deadlock

Offline (no self-play) macro-decision modelling for Deadlock: a win-probability value model plus a behaviour-cloned
policy over macro actions, trained on public match data and replays.

## Pipeline

```
select_matches.py   data lake + rank API + salts API  ->  data/selected.parquet
parse_matches.py    download .dem, parse with boon    ->  data/matches/<match_id>/*.parquet
```

```bash
uv sync                      # or: uv venv && uv pip install -e .
python select_matches.py     # rank-filtered, replay-available post-patch ranked matches
python parse_matches.py --limit 3   # smoke test, then drop --limit
```

`parse_matches.py` is resumable (skips matches that already have `meta.parquet`), deletes each demo after parsing, and
logs failures to `data/failures.tsv`.

## Per-match output

`players` (positions, health, souls, items every 2s), `objectives`, `mid_boss`, `kills`, `teamfights`, `neutrals`,
`item_purchases`, `meta`.

## Data sources

See [docs/data-sources.md](docs/data-sources.md).
