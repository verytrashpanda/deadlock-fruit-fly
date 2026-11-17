"""Pick rank-filtered, replay-available ranked matches from the Deadlock API data lake.

Output: data/selected.parquet (match_id, demo_url, avg_badge, n_ranked, winning_team, duration_s, start_time)
"""
import argparse
import datetime as dt
import time

import duckdb
import polars as pl
import requests

API = "https://api.deadlock-api.com/v1"
LAKE = "ducklake:https://data.deadlock-api.com/v1/catalog.ducklake"

ap = argparse.ArgumentParser()
ap.add_argument("--since", default="2026-10-20")  # post 29 Sep patch; replays 404 after ~30 days
ap.add_argument("--until", default="2026-11-15")
ap.add_argument("--candidates", type=int, default=4000)
ap.add_argument("--target", type=int, default=500)
ap.add_argument("--min-avg-badge", type=float, default=60.0)  # tier*10+sub; 60 = Ritualist I
ap.add_argument("--min-ranked", type=int, default=10)  # of 12 players with a rank
ap.add_argument("--min-minutes", type=float, default=15.0)
ap.add_argument("--out", default="data/selected.parquet")
args = ap.parse_args()

c = duckdb.connect()
c.sql("SET memory_limit='4GB'; SET temp_directory='/tmp/duckdb_spill';")
c.sql("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;")
c.sql(f"ATTACH '{LAKE}' AS dl (READ_ONLY); USE dl;")

# Hash-sampled (deterministic) full 12-player ranked matches. A single wide-window query hangs on the lake
# (>~12h returns nothing), so scan in 6h chunks and fetch matches and player ids together.
t0 = time.time()
utc = dt.timezone.utc
since = dt.datetime.fromisoformat(args.since).replace(tzinfo=utc)
until = dt.datetime.fromisoformat(args.until).replace(tzinfo=utc)
m_parts, p_parts = [], []
lo = since
while lo < until:
    hi = min(lo + dt.timedelta(hours=6), until)
    w = f"start_time >= '{lo.isoformat()}' and start_time < '{hi.isoformat()}' and hash(match_id) % 200 = 0"
    m_parts.append(
        c.sql(
            f"""
            select match_id, any_value(start_time) start_time, any_value(duration_s) duration_s,
                   any_value(winning_team) winning_team
            from match_player
            where {w} and match_mode = 'Ranked' and game_mode = 'Normal' and match_outcome = 'TeamWin'
              and duration_s >= {args.min_minutes * 60}
            group by match_id having count(*) = 12
            """
        ).pl()
    )
    p_parts.append(c.sql(f"select match_id, account_id from match_player where {w}").pl())
    lo = hi
m = pl.concat(m_parts)
print(f"eligible matches: {len(m)} ({time.time() - t0:.0f}s)", flush=True)
cands = m.sort(pl.col("match_id").hash()).head(args.candidates)
print(f"candidates: {len(cands)}", flush=True)
players = pl.concat(p_parts).join(cands.select("match_id"), on="match_id")
print(f"player rows: {len(players)}", flush=True)
# Batch rank lookup (<=1000 accounts/call; 10k accounts/min IP limit)
accts = players["account_id"].unique().to_list()
badge: dict[int, int] = {}
for i in range(0, len(accts), 1000):
    chunk = accts[i : i + 1000]
    for attempt in range(5):
        r = requests.get(f"{API}/players/rank", params={"account_ids": ",".join(map(str, chunk))}, timeout=120)
        if r.status_code == 429:
            time.sleep(15)
            continue
        r.raise_for_status()
        break
    for x in r.json():
        badge[x["account_id"]] = x["badge"]
    print(f"ranked {min(i + 1000, len(accts))}/{len(accts)}", flush=True)

players = players.with_columns(pl.col("account_id").map_elements(lambda a: badge.get(a, 0), return_dtype=pl.Int64).alias("badge"))
agg = (
    players.filter(pl.col("badge") > 0)
    .group_by("match_id")
    .agg(pl.col("badge").mean().alias("avg_badge"), pl.len().alias("n_ranked"))
)
sel = cands.join(agg, on="match_id").filter(
    (pl.col("n_ranked") >= args.min_ranked) & (pl.col("avg_badge") >= args.min_avg_badge)
)
print(f"after rank filter: {len(sel)}", flush=True)

# Replay availability
urls = {}
for n, mid in enumerate(sel.sort("avg_badge", descending=True)["match_id"].to_list()):
    if len(urls) >= args.target:
        break
    for attempt in range(3):
        try:
            j = requests.get(f"{API}/matches/{mid}/salts", timeout=30).json()
            break
        except Exception:
            time.sleep(2)
    else:
        continue
    if j.get("demo_url") and requests.head(j["demo_url"], timeout=30).status_code == 200:  # old replays 404
        urls[mid] = j["demo_url"]
    if n % 50 == 0:
        print(f"salts {n}: {len(urls)} with demo", flush=True)

out = sel.filter(pl.col("match_id").is_in(list(urls))).with_columns(
    pl.col("match_id").map_elements(urls.get, return_dtype=pl.String).alias("demo_url")
)
out.write_parquet(args.out)
print(f"selected {len(out)} matches -> {args.out}; avg_badge range {out['avg_badge'].min():.1f}-{out['avg_badge'].max():.1f}")
