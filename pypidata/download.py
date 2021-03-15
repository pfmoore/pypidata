import json
import re
import xmlrpc.client

URL = "https://pypi.org/pypi"
pypi = xmlrpc.client.ServerProxy(URL)

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

def get_latest_serial():
    return pypi.changelog_last_serial()

def get_packages():
    packages = pypi.list_packages_with_serial()
    for name in packages:
        yield normalize(name), name, packages[name]

def get_changelog(since=0):
    while True:
        next_batch = pypi.changelog_since_serial(since)
        if not next_batch:
            break

        for name, version, timestamp, action, serial in next_batch:
            yield normalize(name), name, version, timestamp, action, serial

        next_since = max(entry[-1] for entry in next_batch)
        since = next_since

# async with aiohttp.ClientSession() as session:

async def get_json(session, name):
    url = f"https://pypi.org/pypi/{name}/json"

    async with session.get(url) as response:
        if response.status >= 400:
            return None, 0
        data = await response.text()
        if not data:
            return None, 0

    return data, json.loads(data).get("last_serial", 0)

async def get_simple(session, name):
    url = f"https://pypi.org/simple/{name}/"

    async with session.get(url) as response:
        if response.status >= 400:
            return None, 0
        data = await response.text()
        if not data:
            return None, 0

    last_line = data.splitlines()[-1]
    m = re.fullmatch(r"<!--SERIAL (\d+)-->", last_line)
    if m:
        serial = int(m.group(1))
    else:
        serial = 0


    return data, serial
