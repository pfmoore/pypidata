import asyncio
import aiosqlite
import httpx
import json
import argparse
import xmlrpc.client
import zlib
import re
import sys
from pathlib import Path
from rich.progress import Progress

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

URLs = {
    "json": "https://pypi.org/pypi/{name}/json",
    "simple": "https://pypi.org/simple/{name}/",
}

async def update_page(sem, db, page_type, name, last_serial, upd):
    async with sem:
        async with httpx.AsyncClient() as client:
            tries = 0
            while True:
                try:
                    response = await client.get(URLs[page_type].format(name=name))
                    break
                except httpx.ConnectTimeout:
                    tries += 1
                    if tries > 10:
                        print(f"Failed to fetch {name} ({page_type}) - skipping...")
                        return
                    await asyncio.sleep(1)
            data = response.text if not response.is_error else None
            if data is None:
                serial = last_serial
            elif page_type == "json":
                serial = json.loads(data)["last_serial"]
            else:
                last_line = data.splitlines()[-1]
                m = re.fullmatch(r"<!--SERIAL (\d+)-->", last_line)
                if m:
                    serial = int(m.group(1))
                else:
                    print(data)
                    print("Oops: Last line does not match:", last_line)
                    serial = None

            #assert serial >= last_serial, f"{name}: Page has {serial}, package list has {last_serial}"

            if data is not None:
                data = zlib.compress(data.encode("UTF-8"))

            await db.execute(f"""\
                INSERT INTO {page_type}_data (
                    name,
                    serial,
                    data
                )
                VALUES (:name, :serial, :data)
                ON CONFLICT(name) DO UPDATE SET
                    serial = :serial,
                    data = :data
                """,
                dict(name=name, serial=serial, data=data)
            )
        #print(page_type, name)
        #print(".", flush=True, end="")
        upd()

async def get_out_of_date(db, page_type, args):
    names = []
    if args.file:
        if args.file == "-":
            text = sys.stdin.read()
        else:
            text = Path(args.file).read_text(encoding="utf-8")
        for name in text.splitlines():
            name = name.strip()
            if not name or name.startswith("#"):
                continue
            names.append((name, 0))
    elif len(args.name) > 0:
        names = [(n,0) for n in args.name]
    else:
        SQL = f"""\
        SELECT name, last_serial
        FROM packages p LEFT JOIN {page_type}_data d USING (name)
        GROUP BY name
        HAVING p.last_serial > coalesce(max(d.serial), 0)
        ORDER BY name
        """
        async with db.execute(SQL) as cursor:
            async for row in cursor:
                name,serial = row
                names.append((name, serial))
    
    if args.limit:
        names = names[:args.limit]
    return names

async def update_all_pages(db, page_type, args, progress):
    packages = await get_out_of_date(db, page_type, args)
    print(f"Updating {len(packages)} {page_type} pages")
    taskbar = progress.add_task(f"Updating {page_type}", total=len(packages))
    sem = asyncio.Semaphore(100)
    updates = [
        update_page(
            sem,
            db,
            page_type,
            name,
            last_serial,
            upd=lambda: progress.update(taskbar, advance=1)
        )
        for name, last_serial in packages
    ]
    await asyncio.gather(*updates)

async def update_packages(db):
    # Get the data from XMLRPC
    XMLRPC = "https://pypi.org/pypi"
    pypi = xmlrpc.client.ServerProxy(XMLRPC)
    packages = { normalize(n): (n, s) for (n, s) in pypi.list_packages_with_serial().items() }
    def params():
        for norm, (n, s) in packages.items():
            yield dict(name=norm, display_name=n, ser=s)
    await db.executemany(
        """\
            INSERT INTO packages (name, display_name, last_serial)
            VALUES (:name, :display_name, :ser)
            ON CONFLICT(name) DO UPDATE SET
                display_name = :display_name,
                last_serial = :ser
        """,
        params()
    )

async def main(args):
    if not args.type:
        args.type = ["json", "simple"]
    async with aiosqlite.connect(args.database) as db:
        if not (args.file or args.name):
            print("Updating package list")
            await update_packages(db)
        print("Got package list")
        tasks = []
        with Progress() as progress:
            for page_type in args.type:
                tasks.append(asyncio.create_task(update_all_pages(db, page_type, args, progress)))
            await asyncio.wait(tasks)
        await db.commit()

if __name__ == "__main__":
    def parse_cmdline(args=None):
        parser = argparse.ArgumentParser(description="Update raw PyPI information database")
        parser.add_argument("names", nargs="?", help="A file of projects to update")
        parser.add_argument("--limit", "-l", type=int, help="Maximum number of projects to update")
        parser.add_argument("--database", "--DB", default="PyPI_raw.db", help="The database to update")
        parser.add_argument("--type", default="json", help="The type of data (json or simple) to update")

        return parser.parse_args(args)

    args = parse_cmdline()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
