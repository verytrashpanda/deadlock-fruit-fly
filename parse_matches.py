"""Download + parse selected replays with boon into per-match Parquet, then delete the demo.

Output per match: data/matches/<match_id>/{players,objectives,mid_boss,kills,teamfights,neutrals,items,meta}.parquet
Resumable: skips matches whose meta.parquet already exists. Failures go to data/failures.tsv.
"""
import argparse
import os
import shutil
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

import polars as pl
import requests
import zstandard

ap = argparse.ArgumentParser()
ap.add_argument("--selected", default="data/selected.parquet")
ap.add_argument("--out", default="data/matches")
ap.add_argument("--workers", type=int, default=4)
ap.add_argument("--limit", type=int, default=0)
ap.add_argument("--step-seconds", type=float, default=2.0)
args = ap.parse_args()


def get(d, name):
    v = getattr(d, name)
    return v() if callable(v) else v


def process(mid: int, url: str):
    out = os.path.join(args.out, str(mid))
    if os.path.exists(os.path.join(out, "meta.parquet")):
        return mid, "skip", 0.0
    t0 = time.time()
    tmp = tempfile.mkdtemp(prefix=f"dl{mid}_", dir="data")
    try:
        comp = os.path.join(tmp, "m.dem.zst")
        for attempt in range(3):
            try:
                with requests.get(url, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    with open(comp, "wb") as f:
                        for ch in r.iter_content(1 << 20):
                            f.write(ch)
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(3)
        raw = os.path.join(tmp, "m.dem")
        with open(comp, "rb") as f, open(raw, "wb") as o:
            zstandard.ZstdDecompressor().copy_stream(f, o)
        os.remove(comp)

        from boon import Demo

        d = Demo(raw, preload=False)
        step = max(1, int(d.tick_rate * args.step_seconds))
        ticks = list(range(d.game_start_tick, d.game_over_tick, step))
        os.makedirs(out, exist_ok=True)
        d.snapshots("player_ticks", ticks=ticks).write_parquet(os.path.join(out, "players.parquet"))
        for name in ["objectives", "mid_boss", "kills", "teamfights", "neutrals", "item_purchases"]:
            get(d, name).write_parquet(os.path.join(out, f"{name}.parquet"))
        pl.DataFrame(
            {
                "match_id": [int(d.match_id)],
                "tick_rate": [int(d.tick_rate)],
                "game_start_tick": [int(d.game_start_tick)],
                "game_over_tick": [int(d.game_over_tick)],
                "winning_team_num": [int(d.winning_team_num)],
                "step_ticks": [step],
                "boon_parse_s": [round(time.time() - t0, 1)],
            }
        ).write_parquet(os.path.join(out, "meta.parquet"))  # written last = completion marker
        return mid, "ok", time.time() - t0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sel = pl.read_parquet(args.selected)
    rows = list(zip(sel["match_id"].to_list(), sel["demo_url"].to_list()))
    if args.limit:
        rows = rows[: args.limit]
    os.makedirs(args.out, exist_ok=True)
    done = fail = skip = 0
    t0 = time.time()
    with ProcessPoolExecutor(args.workers) as ex, open("data/failures.tsv", "a") as ff:
        futs = {ex.submit(process, m, u): m for m, u in rows}
        for f in as_completed(futs):
            m = futs[f]
            try:
                _, status, dt = f.result()
            except Exception as e:
                fail += 1
                ff.write(f"{m}\t{type(e).__name__}: {e}\n")
                ff.flush()
                traceback.print_exc()
                status = "FAIL"
                dt = 0
            else:
                skip += status == "skip"
                done += status == "ok"
            print(f"[{done}+{skip} ok/skip, {fail} fail / {len(rows)}] {m} {status} {dt:.0f}s elapsed={time.time()-t0:.0f}s", flush=True)
