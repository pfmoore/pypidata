import argparse
import asyncio

#from .pkg import main as pkg_main
from .chg import main as chg_main
#from .req import main as req_main
from .meta import main as meta_main
from .raw import main as raw_main


def make_parser():
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='pypidata')
    subparsers = parser.add_subparsers()

    parser_raw = subparsers.add_parser("raw", description="Update raw PyPI information database", help="Manage raw data from PyPI")
    parser_raw.add_argument("name", nargs="*", help="Names of projects to update")
    parser_raw.add_argument("--file", help="A file of projects to update")
    parser_raw.add_argument("--limit", "-L", type=int, help="Maximum number of projects to update")
    parser_raw.add_argument("--database", "--DB", default="PyPI_raw.db", help="The database to update")
    parser_raw.add_argument("--type", action="append", help="The type of data (json or simple) to update")
    parser_raw.add_argument("--list", "-l", action="store_true", help="List the packages to be updated")
    parser_raw.set_defaults(main=raw_main)

    #parser_pkg = subparsers.add_parser("pkg", description="Update package data from raw JSON", help="Manage package data")
    #parser_pkg.add_argument("name", nargs="*", help="Names of projects to update")
    #parser_pkg.add_argument("--file", help="A file of projects to update")
    #parser_pkg.add_argument("--limit", "-l", type=int, help="Maximum number of projects to update")
    #parser_pkg.add_argument("--database", "--DB", default="PackageData.db", help="The database to update")
    #parser_pkg.add_argument("--raw", default="PyPI_raw.db", help="The source database of raw PyPI data")
    #parser_pkg.add_argument("--list", "-L", action="store_true", help="List the packages to be updated")
    #parser_pkg.set_defaults(main=pkg_main)
    
    parser_chg = subparsers.add_parser("chg", description="Update changelog data", help="Manage changelog data")
    parser_chg.add_argument("--database", "--DB", default="PyPI_raw.db", help="The database to update")
    parser_chg.set_defaults(main=chg_main)

    parser_meta = subparsers.add_parser("meta", description="Add wheel metadata", help="Add wheel metadata")
    parser_meta.add_argument("--database", "--DB", default="Metadata.db", help="The database to update")
    parser_meta.add_argument("--raw", default="PyPI_raw.db", help="The raw PyPI data")
    parser_meta.add_argument("--limit", "-l", type=int, help="Maximum number of files to update")
    parser_meta.set_defaults(main=meta_main)

    #parser_req = subparsers.add_parser("req", description="Add requirement data", help="Add requirement data")
    #parser_req.add_argument("--database", "--DB", default="Requirements.db", help="The database to update")
    #parser_req.add_argument("--pkg", default="PackageData.db", help="The package information database")
    #parser_req.add_argument("name", nargs="*", help="Names of projects to update")
    #parser_req.set_defaults(main=req_main)

    return parser

def main():
    parser = make_parser()
    args = parser.parse_args()

    if args.main == raw_main:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(args.main(args))
    else:
        args.main(args)
