import html
import json
import html.parser
import urllib.parse

# Parser based on the one in mousebender:
# https://github.com/brettcannon/mousebender
class ArchiveLinkHTMLParser(html.parser.HTMLParser):
    def __init__(self):
        self.archive_links = []
        self.serial = None
        self.current_link = None
        super().__init__()

    def handle_data(self, data):
        if self.current_link:
            self.current_link["data"] = self.current_link.get(data, "") + data

    def handle_starttag(self, tag, attrs_list):
        if tag != "a":
            return
        attrs = dict(attrs_list)
        # PEP 503:
        # The href attribute MUST be a URL that links to the location of the
        # file for download ...
        full_url = attrs["href"]
        parsed_url = urllib.parse.urlparse(full_url)
        # PEP 503:
        # ... the text of the anchor tag MUST match the final path component
        # (the filename) of the URL.
        _, _, raw_filename = parsed_url.path.rpartition("/")
        filename = urllib.parse.unquote(raw_filename)
        url = urllib.parse.urlunparse((*parsed_url[:5], ""))
        hash_algo = None
        hash_value = None
        # PEP 503:
        # The URL SHOULD include a hash in the form of a URL fragment with the
        # following syntax: #<hashname>=<hashvalue> ...
        if parsed_url.fragment:
            hash_algo, hash_value = parsed_url.fragment.split("=", 1)
            hash_algo = hash_algo.lower()
        # PEP 503:
        # A repository MAY include a data-requires-python attribute on a file
        # link. This exposes the Requires-Python metadata field ...
        # In the attribute value, < and > have to be HTML encoded as &lt; and
        # &gt;, respectively.
        requires_python = html.unescape(attrs.get("data-requires-python", ""))
        # PEP 503:
        # A repository MAY include a data-gpg-sig attribute on a file link with
        # a value of either true or false ...
        gpg_sig = attrs.get("data-gpg-sig")
        # PEP 592:
        # Links in the simple repository MAY have a data-yanked attribute which
        # may have no value, or may have an arbitrary string as a value.
        is_yanked = "data-yanked" in attrs
        yank_reason = attrs.get("data-yanked")

        self.current_link = dict(
            filename=filename,
            url=url,
            requires_python=requires_python,
            hash_algo=hash_algo,
            hash_value=hash_value,
            gpg_sig=gpg_sig,
            is_yanked=is_yanked,
            yank_reason=yank_reason,
        )

    def handle_endtag(self, tag):
        if tag != "a":
            return
        if self.current_link["data"] != self.current_link["filename"]:
            print("Link text and target URL mismatch for", self.current_link["data"])
        self.archive_links.append(self.current_link)
        self.current_link = None

    def handle_comment(self, data):
        if data.startswith("SERIAL"):
            self.serial = int(data[6:].strip())

# Package info
#
#     author : str
#     author_email : str
#     bugtrack_url : str
#     classifiers : List[str]
#     description : str
#     description_content_type : str
#     docs_url : str
#     download_url : str
#     downloads : Dict["last_day", "last_month", "last_week"]
#     home_page : str
#     keywords : str
#     license : str
#     maintainer : str
#     maintainer_email : str
#     name : str
#     package_url : str
#     platform : str
#     project_url : str
#     project_urls Dict[type -> url]
#     release_url : str
#     requires_dist : List[str]
#     requires_python : str
#     summary : str
#     version : str
#     yanked : bool
#     yanked_reason : str

# Package URLs (in releases):
#
#     comment_text : str
#     digests : Dict[hash_type -> hash_val]
#     downloads : int
#     filename : str
#     has_sig : bool
#     md5_digest : str
#     packagetype : str
#     python_version : str
#     requires_python : str
#     size : int
#     upload_time : str
#     upload_time_iso_8601 : str
#     url : str
#     yanked : bool
#     yanked_reason : str

def parse_json(data):
    data = json.loads(data)
    def u(d):
        return (
            d["comment_text"],
            tuple(sorted(d["digests"].items())),
            d["downloads"],
            d["filename"],
            d["has_sig"],
            d["md5_digest"],
            d["packagetype"],
            d["python_version"],
            d["requires_python"],
            d["size"],
            d["upload_time"],
            d["upload_time_iso_8601"],
            d["url"],
            d["yanked"],
            d["yanked_reason"],
        )

    last_serial = data["last_serial"]
    info = data["info"]
    releases = data["releases"]
    urls = data["urls"]

    assert urls == releases[info["version"]]


if __name__ == "__main__":
    import sys
    from pathlib import Path
    parser = ArchiveLinkHTMLParser()
    parser.feed(Path(sys.argv[1]).read_text())
    from pprint import pprint
    print(parser.serial)
    pprint(parser.archive_links)
    for d in parser.archive_links:
        if d["data"] != d["filename"]:
            print("Violation: ", d)
    print(f"Links: {len(parser.archive_links)}, Unique: {len(set(d['filename'] for d in parser.archive_links))}")
    parse_json(Path(sys.argv[2]).read_text())
