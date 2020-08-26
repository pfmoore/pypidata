import trio
import httpx
from datetime import datetime
from rich.progress import Progress

import sqlite3

DB = "PyPI.db"

# Open the database
conn = sqlite3.connect(DB)

# Get the packages to update
PACKAGE_SQL = """\
    SELECT DISTINCT name
    FROM changelog
    WHERE timestamp > (SELECT max(timestamp) FROM package_data_json)
"""
names = [n for (n,) in conn.execute(PACKAGE_SQL).fetchall()]

# Only do 100,000 at a time
if len(names) > 100000:
    print(f"Reading 100,000 of {len(names):,}")
    names = names[:100000]
else:
    print(f"Reading {len(names):,} packages")

def write_db(channel, p, t):
    conn = sqlite3.connect(DB)
    while True:
        try:
            name, timestamp, json = trio.from_thread.run(channel.receive)
        except trio.EndOfChannel:
            return
        else:
            if json is not None:
                with conn:
                    conn.execute("""\
                        INSERT INTO package_data_json (
                            name,
                            timestamp,
                            json
                        )
                        VALUES (:name, :timestamp, :json)
                        ON CONFLICT(name) DO UPDATE SET
                            timestamp = :timestamp,
                            json = :json
                        """,
                        dict(name=name, timestamp=timestamp, json=json)
                    )
                conn.commit()
            p.update(t, advance=1.0)


async def get_json(client, channel, name, p, t):
    timestamp = int(datetime.now().timestamp())
    response = await client.get(f"https://pypi.org/pypi/{name}/json")
    p.update(t, advance=1.0)
    json = response.text if not response.is_error else None
    await channel.send((name, timestamp, json))

async def producer(channel, names, p, t):
    async with httpx.AsyncClient() as client:
        async with channel:
            for name in names:
                await get_json(client, channel, name, p, t)

async def consumer(channel, p, t):
    await trio.to_thread.run_sync(write_db, channel, p, t)

async def main():
    send, receive = trio.open_memory_channel(500)
    with Progress() as progress:
        t1 = progress.add_task("Reading data...", total=len(names))
        t2 = progress.add_task("Writing data...", total=len(names))
        async with trio.open_nursery() as nursery:
            nursery.start_soon(producer, send, names, progress, t1)
            nursery.start_soon(consumer, receive, progress, t2)

trio.run(main)
