import re
import xmlrpc.client
import html5lib
from urllib.request import urlopen
import sqlite3

def is_valid(name):
    return re.fullmatch(r"[-_.A-Za-z0-9]*", name) is not None

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

DB = "PyPI.db"

XMLRPC = "https://pypi.org/pypi"
SIMPLE = "https://pypi.org/simple/"

# Open the database
conn = sqlite3.connect(DB)

# Get the data from XMLRPC
pypi = xmlrpc.client.ServerProxy(XMLRPC)
packages = { normalize(n): (n, s) for (n, s) in pypi.list_packages_with_serial().items() }

with conn:
    def params():
        for norm, (n, s) in packages.items():
            yield dict(name=norm, display_name=n, ser=s)
    conn.executemany(
        """\
            INSERT INTO packages (name, display_name, last_serial)
            VALUES (:name, :display_name, :ser)
            ON CONFLICT(name) DO UPDATE SET
                display_name = :display_name,
                last_serial = :ser
        """,
        params()
    )

# Get the data from the simple index
with urlopen(SIMPLE) as f:
    simple = f.read()

doc = html5lib.parse(simple, namespaceHTMLElements=False)
refs = doc.findall(".//a")

with conn:
    def params():
        for ref in refs:
            name = ref.text
            norm = normalize(name)
            if norm in packages and packages[norm][0] != name:
                print(f"Warning: {norm} has display name {name} in simple, but {packages[norm][0]} in xmlrpc")
            url = ref.get("href")
            gpg = ref.get("data-gpg-sig")
            py = ref.get("data-requires-python")
            yield dict(name=norm, display_name=name, url=url, gpg=gpg, py=py)
    conn.executemany(
        """\
            INSERT INTO packages (
                name,
                display_name,
                url,
                gpg_sig,
                requires_python
            )
            VALUES (:name, :display_name, :url, :gpg, :py)
            ON CONFLICT (name) DO UPDATE SET
                display_name = :display_name,
                url = :url,
                gpg_sig = :gpg,
                requires_python = :py
        """,
        params()
    )

conn.close()
