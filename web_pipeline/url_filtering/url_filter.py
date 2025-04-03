from typing import Iterable
from datatrove.pipeline.filters.url_filter import URLFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.data import Document
from typing import Iterable
import os
import time
import re
import os

ASSETS_PATH = "url_filtering"

normalizer = re.compile(r"[^a-zA-Z0-9]+")

def normalize(text, replace=""):
    return normalizer.sub(replace, text).lower()

def parse_list(line, do_normalize=True):
    return {normalize(x) if do_normalize else x.strip() for x in line if x[0] != "#"}

def get_list(abs_path: str, file_name: str, extra: set, do_normalize: bool = True):
    with open(os.path.join(abs_path, file_name)) as f:
        return parse_list(f, do_normalize).union(extra)

class CustomURLFilterWithWhitelist(URLFilter):
    """
    Extends URLFilter to include a whitelist functionality.
    URLs from whitelisted domains or exact whitelisted URLs will bypass all other filters.
    """
    name = "😈Custom Url-filter With Whitelist"
    _requires_dependencies = ["tldextract", "fasteners", ("ahocorasick", "pyahocorasick")]

    def __init__(
        self,
        use_whitelist: bool = True,
        whitelist_domains: Iterable = None,
        whitelist_urls: Iterable = None,
        do_remove_curated_sources: bool = False,
        curated_domains: Iterable = None,
        do_load_from_cache: bool = True,
        do_add_extra_domain_and_urls: bool = False,
        exclusion_writer: DiskWriter = None,
        *args,
        **kwargs
    ):
        if do_add_extra_domain_and_urls:
            extra_domains, extra_urls = set(), set()
            blocklist_dir = os.path.join(ASSETS_PATH, "urls", "blocklist")
            for dirname in os.listdir(blocklist_dir):
                if not os.path.isdir(os.path.join(blocklist_dir, dirname)):
                    continue
                extra_domains = get_list(os.path.join(blocklist_dir, dirname), "domains", extra_domains , do_normalize=False)
                print(f"domain size: {len(extra_domains)}")
                extra_urls = get_list(os.path.join(blocklist_dir, dirname), "urls", extra_urls, do_normalize=False)
                print(f"domain size: {len(extra_urls)}")

            print(f"Extra domains ({len(extra_domains)}) and urls ({len(extra_urls)})")
            super().__init__(
                extra_domains = extra_domains, 
                extra_urls = extra_urls, 
                exclusion_writer = exclusion_writer
            )
            print("use extra domains and urls")
        else:
            super().__init__(
                exclusion_writer = exclusion_writer
            )
        self.whitelist_domains = set(whitelist_domains or [])
        self.whitelist_urls = set(whitelist_urls or [])
        self.use_whitelist = use_whitelist
        self.do_remove_curated_sources = do_remove_curated_sources
        self.curated_domains = set(curated_domains or [])

        if do_load_from_cache:
            whitelist_dir = os.path.join(ASSETS_PATH, "urls", "whitelist")
            self.whitelist_domains = get_list(whitelist_dir, "domains", self.whitelist_domains, do_normalize=False)
            self.whitelist_urls = get_list(whitelist_dir, "urls", self.whitelist_urls, do_normalize=False)

            curated_dir = os.path.join(ASSETS_PATH, "urls", "curated")
            self.curated_domains = get_list(curated_dir, "domains", self.curated_domains, do_normalize=False)

        if not self.use_whitelist:
            self.whitelist_domains = set()
            self.whitelist_urls = set()
        if not self.do_remove_curated_sources:
            self.curated_domains = set()

    def filter(self, document: Document) -> bool | tuple[bool, str]:
        self.download_data()
        url = document.metadata.get("url")

        assert url, "Document does not have url in its metadata"
        url_info = self.tldextractor(url)

        # Check if the URL or its domain is in the whitelist
        if url in self.whitelist_urls or url_info.registered_domain in self.whitelist_domains or url_info.fqdn in self.whitelist_domains:
            return True
        
        if url_info.registered_domain in self.curated_domains or url_info.fqdn in self.curated_domains:
            if not self.do_remove_curated_sources:
                assert self.curated_domains == set()
            return False, "curated"

        # If not whitelisted, proceed with the original filtering logic
        return super().filter(document)
