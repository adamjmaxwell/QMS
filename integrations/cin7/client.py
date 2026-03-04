"""
Cin7 (DEAR Inventory) API Client
Base HTTP client: auth, requests, pagination, rate-limit handling.
"""

import os
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"


class Cin7Client:
    """
    Authenticated HTTP client for the Cin7 / DEAR Inventory REST API v2.

    Auth uses two HTTP headers on every request:
        api-auth-accountid       - your Cin7 Account ID
        api-auth-applicationkey  - your Cin7 Application Key

    Supply credentials via environment variables CIN7_ACCOUNT_ID / CIN7_APP_KEY,
    or pass them directly to __init__.
    """

    def __init__(self, account_id=None, app_key=None):
        self.account_id = account_id or os.environ.get("CIN7_ACCOUNT_ID")
        self.app_key    = app_key    or os.environ.get("CIN7_APP_KEY")
        if not self.account_id or not self.app_key:
            raise EnvironmentError(
                "Cin7 credentials missing. Set CIN7_ACCOUNT_ID and CIN7_APP_KEY "
                "environment variables (or pass them to Cin7Client)."
            )
        self.session = requests.Session()
        self.session.headers.update({
            "api-auth-accountid":      self.account_id,
            "api-auth-applicationkey": self.app_key,
            "Content-Type":            "application/json",
            "Accept":                  "application/json",
        })
        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    # ------------------------------------------------------------------

    def get(self, endpoint, params=None):
        """GET request. Returns parsed JSON. Raises on non-2xx."""
        url    = f"{BASE_URL}/{endpoint.lstrip('/')}"
        params = params or {}
        logger.debug("GET %s  params=%s", url, params)
        r = self.session.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 60))
            logger.warning("Rate-limited — waiting %ss.", wait)
            time.sleep(wait)
            r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------

    def get_all_pages(self, endpoint, result_key, extra_params=None, page_size=100):
        """
        Fetches every page of a paginated Cin7 endpoint.

        Args:
            endpoint:     API path, e.g. "product"
            result_key:   JSON key holding the record list, e.g. "Products"
            extra_params: Additional query params (date filters, etc.)
            page_size:    Records per page (Cin7 max is 1000 for most endpoints)

        Returns:
            Flat list of all record dicts across all pages.
        """
        params = dict(extra_params or {})
        params["Page"]  = 1
        params["Limit"] = page_size
        all_records = []
        while True:
            data         = self.get(endpoint, params=params)
            page_records = data.get(result_key, [])
            all_records.extend(page_records)
            logger.info(
                "Page %d of '%s' — %d records (total: %d)",
                params["Page"], endpoint, len(page_records), len(all_records),
            )
            if len(page_records) < page_size:
                break
            params["Page"] += 1
            time.sleep(0.3)
        return all_records
