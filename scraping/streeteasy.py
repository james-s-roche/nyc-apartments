from __future__ import annotations
import time
import re
from dataclasses import dataclass
from typing import Iterator, List, Optional
import random
import requests
from requests import Response
from bs4 import BeautifulSoup

from config.settings import load_config


BASE_URL = "https://streeteasy.com"
API_BASE = "https://streeteasy.com/srp-service-api"
HEADERS_CACHE = None
USER_AGENTS = [
    # Recent Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Recent Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    # Recent Firefox
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def _headers():
    global HEADERS_CACHE
    if HEADERS_CACHE is None:
        cfg = load_config()
        ua = cfg.scrape.user_agent or random.choice(USER_AGENTS)
        HEADERS_CACHE = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
    return HEADERS_CACHE


@dataclass
class ListingPreview:
    external_id: str
    url: str
    address: Optional[str]
    price: Optional[int]
    beds: Optional[float]
    baths: Optional[float]
    neighborhood: Optional[str]
    borough: Optional[str]
    fee: Optional[bool]


@dataclass
class ListingDetail(ListingPreview):
    sqft: Optional[int] = None
    building_name: Optional[str] = None
    unit: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    pets: Optional[str] = None
    amenities: Optional[str] = None
    broker: Optional[str] = None


class StreetEasyScraper:
    def __init__(self, delay_seconds: Optional[float] = None, timeout_seconds: Optional[int] = None):
        cfg = load_config()
        base_delay = delay_seconds if delay_seconds is not None else cfg.scrape.request_delay_seconds
        # Introduce small jitter window (±30%)
        self.delay = max(0.1, base_delay)
        self.timeout = timeout_seconds if timeout_seconds is not None else cfg.scrape.request_timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(_headers())
        # Warm-up: visit homepage to establish cookies
        try:
            self.session.get(BASE_URL + "/", timeout=self.timeout)
        except Exception:
            pass

    def _sleep(self):
        # Apply jitter 0.7x to 1.3x
        jitter = random.uniform(0.7, 1.3)
        time.sleep(self.delay * jitter)

    def _get(self, url: str) -> Response:
        # Basic retry/backoff on 403 and some transient statuses
        attempts = 0
        backoff = 0.75
        is_api = url.startswith(API_BASE)
        while True:
            # Prepare request-specific headers
            headers = dict(self.session.headers)
            if is_api:
                headers.update({
                    "Accept": "application/json, text/plain, */*",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-Dest": "empty",
                    "Referer": f"{BASE_URL}/for-rent",
                    "Origin": BASE_URL,
                    "X-Requested-With": "XMLHttpRequest",
                })
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True, headers=headers)
            print(resp.status_code, url)
            if resp.status_code == 403 and attempts < 3:
                # Re-warm cookies and back off
                attempts += 1
                try:
                    self.session.get(BASE_URL + "/", timeout=self.timeout)
                except Exception:
                    pass
                time.sleep(backoff)
                backoff *= 2
                continue
            if resp.status_code in (429, 503) and attempts < 3:
                attempts += 1
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp

    def search_rentals(self, neighborhood: str, beds: Optional[int] = None, max_price: Optional[int] = None, page: int = 1) -> List[ListingPreview]:
        # Use JSON API endpoint discovered via network inspection.
        # Example: https://streeteasy.com/srp-service-api/for-rent/midtown/price:-4000|beds:1?page=1
        filters = []
        if max_price is not None:
            filters.append(f"price:-{max_price}")
        if beds is not None:
            filters.append(f"beds:{beds}")
        
        filter_part = ""
        if filters:
            filter_part = "|".join(filters)
        
        url = f"{API_BASE}/for-rent/{neighborhood}/{filter_part}?page={page}" if filter_part else f"{API_BASE}/for-rent/{neighborhood}?page={page}"
        self._sleep()
        resp = self._get(url)
        data = resp.json()
        return list(self._parse_search_json(data))

    def _parse_search_page(self, html: str) -> Iterator[ListingPreview]:
        # HTML parsing path deprecated in favor of JSON API; return empty iterator.
        return iter(())

    @staticmethod
    def _extract_external_id(url: str) -> str:
        # Use the trailing numeric id if present, otherwise the path
        m = re.search(r"/(rental|sale)/(?:[^/]+)-(\d+)$", url)
        if m:
            return m.group(2)
        return url.rsplit('/', 1)[-1]

    @staticmethod
    def _parse_price(text: str) -> Optional[int]:
        if not text:
            return None
        digits = re.sub(r"[^0-9]", "", text)
        return int(digits) if digits else None

    @staticmethod
    def _parse_beds_baths(text: Optional[str]) -> Optional[float]:
        if not text:
            return None
        # e.g., "1 bd", "Studio", "1.5 ba"
        t = text.lower()
        if 'studio' in t:
            return 0.0
        m = re.search(r"([0-9]+(?:\.[0-9])?)", t)
        return float(m.group(1)) if m else None

    def fetch_detail(self, url: str) -> ListingDetail:
        # No HTML detail scraping with API flow; return minimal detail object.
        external_id = self._extract_external_id(url)
        return ListingDetail(
            external_id=external_id,
            url=url,
            address=None,
            price=None,
            beds=None,
            baths=None,
            neighborhood=None,
            borough=None,
            fee=None,
            sqft=None,
            building_name=None,
            unit=None,
            latitude=None,
            longitude=None,
            pets=None,
            amenities=None,
            broker=None,
        )

    # @staticmethod
    # def _select_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    #     el = soup.select_one(selector)
    #     return el.get_text(strip=True) if el else None

    @staticmethod
    def _parse_int(text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        digits = re.sub(r"[^0-9]", "", text)
        return int(digits) if digits else None

    def _parse_search_json(self, data: dict) -> Iterator[ListingPreview]:
        listings = []
        if isinstance(data, dict):
            # Common paths observed for items
            for key in ("listings", "items", "results", "data"):
                if key in data and isinstance(data[key], list):
                    listings = data[key]
                    break
            if not listings:
                # Sometimes nested under another object
                for v in data.values():
                    if isinstance(v, dict):
                        inner = v.get("listings") or v.get("items") or v.get("results")
                        if isinstance(inner, list):
                            listings = inner
                            break
        for it in listings:
            try:
                external_id = str(
                    it.get("id")
                    or it.get("listing_id")
                    or it.get("seo_id")
                    or it.get("hash_id")
                )
                url = it.get("url") or it.get("canonical_url")
                if url and not url.startswith("http"):
                    url = BASE_URL + url

                address = it.get("address") or it.get("display_address")
                neighborhood = None
                nb = it.get("neighborhood")
                if isinstance(nb, dict):
                    neighborhood = nb.get("name")
                elif isinstance(nb, str):
                    neighborhood = nb
                borough = None
                br = it.get("borough")
                if isinstance(br, dict):
                    borough = br.get("name")
                elif isinstance(br, str):
                    borough = br

                price = None
                price_val = it.get("price") or it.get("price_display")
                if isinstance(price_val, (int, float)):
                    price = int(price_val)
                elif isinstance(price_val, str):
                    price = self._parse_price(price_val)

                beds = it.get("beds") or it.get("bedrooms")
                baths = it.get("baths") or it.get("bathrooms")
                if isinstance(beds, str):
                    beds = self._parse_beds_baths(beds)
                if isinstance(baths, str):
                    baths = self._parse_beds_baths(baths)
                beds = float(beds) if beds is not None else None
                baths = float(baths) if baths is not None else None

                fee = None
                if "no_fee" in it and isinstance(it.get("no_fee"), bool):
                    fee = not it.get("no_fee")
                elif isinstance(it.get("fee"), bool):
                    fee = it.get("fee")

                yield ListingPreview(
                    external_id=external_id,
                    url=url or "",
                    address=address,
                    price=price,
                    beds=beds,
                    baths=baths,
                    neighborhood=neighborhood,
                    borough=borough,
                    fee=fee,
                )
            except Exception:
                continue
