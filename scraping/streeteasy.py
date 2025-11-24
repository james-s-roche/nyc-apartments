from __future__ import annotations
import time
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Iterator, List, Optional
import random
import requests
from requests import Response
from bs4 import BeautifulSoup
import logging

from config.settings import load_config


BASE_URL = "https://streeteasy.com"
API_BASE = "https://streeteasy.com/srp-service-api"
USER_AGENTS = [
    # Recent Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Recent Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    # Recent Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Recent Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def _headers():
    cfg = load_config()
    ua = cfg.scrape.user_agent or random.choice(USER_AGENTS)
    return {
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


class StreetEasyScraper:
    def __init__(self, delay_seconds: Optional[float] = None, timeout_seconds: Optional[int] = None):
        cfg = load_config()
        base_delay = delay_seconds if delay_seconds is not None else cfg.scrape.request_delay_seconds
        # Introduce small jitter window (±30%)
        self.delay = max(0.1, base_delay)
        self.timeout = timeout_seconds if timeout_seconds is not None else cfg.scrape.request_timeout_seconds
        self.proxies = None
        if cfg.scrape.use_proxy_rotator:
            # Using ProxyScrape's free rotator. Note: Free proxies can be slow or unreliable.
            # The service requires a session parameter to rotate IPs.
            session_id = random.randint(1, 1000000)
            proxy_url = f"http://proxyscrape:free@dc.proxyscrape.com:6060?session={session_id}"
            self.proxies = {"http": proxy_url, "https": proxy_url}
            logging.info(f"Using free proxy rotator service.")

        self.session = None
        self._reinitialize_session()

    def _reinitialize_session(self):
        """Closes the current session and creates a new one with fresh headers and cookies."""
        if self.session:
            self.session.close()
        self.session = requests.Session()
        # Warm-up: visit homepage to establish cookies
        try:
            self.session.get(BASE_URL + "/", timeout=self.timeout, proxies=self.proxies)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Session warm-up failed: {e}")

    def _sleep(self):
        # Apply a wider jitter range from 0.6x to 1.8x of the base delay
        jitter = random.uniform(0.6, 1.8)
        time.sleep(self.delay * jitter)

    def _get(self, url: str) -> Response:
        # Basic retry/backoff on 403 and some transient statuses
        attempts = 0
        backoff = 15  # Start with a longer backoff
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
            headers["User-Agent"] = random.choice(USER_AGENTS) # Rotate User-Agent per request
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True, headers=headers, proxies=self.proxies)
            print(resp.status_code, url)
            if resp.status_code == 403 and attempts < 3:
                # 403 Forbidden: We are likely blocked. Re-initialize session and back off.
                attempts += 1
                logging.warning(f"Received 403 on attempt {attempts}. Re-initializing session and backing off for {backoff:.2f}s.")
                self._reinitialize_session()
                time.sleep(backoff)
                backoff *= (2 * random.uniform(0.8, 1.2)) # Exponential backoff with jitter
                continue
            if resp.status_code in (429, 503) and attempts < 3:
                attempts += 1
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp

    def search_rentals(self, neighborhood: str, beds: Optional[int] = None, max_price: Optional[int] = None, page: int = 1) -> List[dict]:
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


    @staticmethod
    def _parse_price(text: str) -> Optional[int]:
        if not text:
            return None
        digits = re.sub(r"[^0-9]", "", text)
        return int(digits) if digits else None

    def _parse_search_json(self, data: dict) -> Iterator[dict]:
        """
        Parses the JSON response from the StreetEasy SRP API.
        The main listings are in `listingData.edges`, where each edge has a `node`.
        """
        try:
            edges = data.get("listingData", {}).get("edges", [])
        except AttributeError:
            # If data is not a dict, .get will fail
            return iter(())

        for edge in edges:
            try:
                node = edge.get("node")
                if not isinstance(node, dict):
                    continue

                # Add a few constructed fields to the raw node data.
                url_path = node.get("urlPath")
                node['url'] = BASE_URL + url_path if url_path else ""
                node['source'] = 'streeteasy'

                # Handle upcomingOpenHouse
                open_house = node.get("upcomingOpenHouse") or {}
                open_house_start = open_house.get("startTime")
                if open_house_start:
                    # Parse ISO 8601 format like "2025-11-22T12:00:00.000-05:00"
                    try:
                        # fromisoformat handles this, but we need to strip timezone for naive datetime
                        dt_obj = datetime.fromisoformat(open_house_start.replace("Z", "+00:00"))
                        open_house_start = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError):
                        open_house_start = None
                else:
                    open_house_start = None
                
                node['upcomingOpenHouseStartTime'] = open_house_start

                yield node
            except Exception:
                continue
