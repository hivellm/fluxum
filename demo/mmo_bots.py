#!/usr/bin/env python3
"""The MMO sample's bot fleet: N real TCP clients, each one avatar.

Every bot is a genuine Fluxum session over the binary TCP transport
(:15801) — not a loop faking rows over HTTP — so the server sees real
connections, real per-identity admission rates, and real disconnect
despawns (closing a bot atomically deletes its `Player` row via the
table's `ephemeral` + `#[owner]` binding).

    python demo/mmo_bots.py --players 99 --hz 10

Each bot random-walks between waypoints and calls `move_player(x, y)` at
`--hz`, jittered so the fleet never phase-locks. Stats print every 2 s.
Ctrl+C stops the fleet; the avatars vanish with the connections.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdks" / "python"))

from fluxum.client import Connection  # noqa: E402

WORLD_W = 2000
WORLD_H = 1200


async def bot(index: int, args: argparse.Namespace, stats: dict) -> None:
    conn = await Connection.connect(args.url, token=f"mmo-bot-{index}".encode())
    stats["connected"] += 1
    x = random.uniform(0, WORLD_W)
    y = random.uniform(0, WORLD_H)
    wx, wy = random.uniform(0, WORLD_W), random.uniform(0, WORLD_H)
    speed = random.uniform(80.0, 220.0)  # world units per second
    period = 1.0 / args.hz
    try:
        while True:
            dx, dy = wx - x, wy - y
            dist = math.hypot(dx, dy)
            if dist < 8.0:
                wx, wy = random.uniform(0, WORLD_W), random.uniform(0, WORLD_H)
                continue
            step = min(dist, speed * period)
            x += dx / dist * step
            y += dy / dist * step
            try:
                await conn.call_reducer("move_player", [int(x), int(y)])
                stats["moves"] += 1
            except Exception as err:  # rate-limit or transient — count, keep walking
                stats["errors"] += 1
                stats["last_error"] = str(err)
            await asyncio.sleep(period * random.uniform(0.85, 1.15))
    finally:
        stats["connected"] -= 1
        await conn.close()


async def report(stats: dict, players: int) -> None:
    last_moves, last_t = 0, time.monotonic()
    while True:
        await asyncio.sleep(2.0)
        now = time.monotonic()
        rate = (stats["moves"] - last_moves) / (now - last_t)
        last_moves, last_t = stats["moves"], now
        line = (
            f"[mmo-bots] {stats['connected']}/{players} connected · "
            f"{rate:6.1f} moves/s · {stats['moves']} total · {stats['errors']} errors"
        )
        if stats["errors"] and stats.get("last_error"):
            line += f" (last: {stats['last_error'][:60]})"
        print(line, flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--players", type=int, default=99, help="bots to run (default 99)")
    parser.add_argument("--hz", type=float, default=10.0, help="moves per second per bot (default 10)")
    parser.add_argument("--url", default="127.0.0.1:15801", help="TCP transport (default 127.0.0.1:15801)")
    args = parser.parse_args()
    if args.hz > 50:
        parser.error("--hz must stay under the reducer's 60/s per-identity admission rate")

    stats = {"connected": 0, "moves": 0, "errors": 0}
    tasks = [asyncio.ensure_future(report(stats, args.players))]
    print(f"[mmo-bots] spawning {args.players} bots at {args.hz:g} Hz against {args.url}…", flush=True)
    for i in range(args.players):
        tasks.append(asyncio.ensure_future(bot(i, args, stats)))
        await asyncio.sleep(0.03)  # stagger the connects; no thundering herd
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[mmo-bots] stopped — avatars despawn with their connections")
