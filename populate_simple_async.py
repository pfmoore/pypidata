import re
import trio
import httpx
from datetime import datetime
from rich.progress import Progress
import sys
import sqlite3

DB = "PyPI.db"

def is_valid(name):
    return re.fullmatch(r"[-_.A-Za-z0-9]*", name) is not None

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

# Open the database
conn = sqlite3.connect(DB)

# Register the normalize function
conn.create_function("normalize", 1, normalize)

if len(sys.argv) > 1:
    with open(sys.argv[1]) as f:
        names = [name.strip() for name in f]
else:
    # Get the packages to update
    PACKAGE_SQL = """\
        SELECT DISTINCT name
        FROM changelog
        WHERE timestamp > (SELECT max(timestamp) FROM package_data_simple)
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
            name, timestamp, page = trio.from_thread.run(channel.receive)
        except trio.EndOfChannel:
            return
        else:
            with conn:
                if page is None:
                    conn.execute(
                        "DELETE FROM package_data_simple WHERE name=?",
                        (name,)
                    )
                else:
                    conn.execute("""\
                        INSERT INTO package_data_simple (
                            name,
                            timestamp,
                            page
                        )
                        VALUES (:name, :timestamp, :page)
                        ON CONFLICT(name) DO UPDATE SET
                            timestamp = :timestamp,
                            page = :page
                        """,
                        dict(name=name, timestamp=timestamp, page=page)
                    )
            conn.commit()
            p.update(t, advance=1.0)


async def get_page(client, channel, name, p, t):
    timestamp = int(datetime.now().timestamp())
    name = normalize(name)
    response = await client.get(f"https://pypi.org/simple/{name}/")
    p.update(t, advance=1.0)
    page = response.text if not response.is_error else None
    await channel.send((name, timestamp, page))

async def producer(channel, names, p, t):
    async with httpx.AsyncClient() as client:
        async with channel:
            for name in names:
                await get_page(client, channel, name, p, t)

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
