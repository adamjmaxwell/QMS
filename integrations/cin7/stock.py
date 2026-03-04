"""
Cin7 Stock / Inventory Availability Import
Fetches current stock-on-hand levels for all products across all locations.

Cin7 endpoints:
    GET /ref/productavailability  - current SOH snapshot per product per location
    GET /stockadjustment          - stock movement / adjustment history log

Key fields in /ref/productavailability:
    ID, SKU, Name, Location, LocationID, Bin,
    OnHand, Allocated, Available, OnOrder
"""

import logging
from .client import Cin7Client

logger = logging.getLogger(__name__)


def fetch_stock_on_hand(client=None, location_id=None):
    client = client or Cin7Client()
    extra  = {}
    if location_id:
        extra["LocationID"] = location_id
    records = client.get_all_pages(
        endpoint="ref/productavailability",
        result_key="ProductAvailabilityList",
        extra_params=extra,
    )
    logger.info("Fetched stock levels for %d SKU/location rows.", len(records))
    return records


def map_stock_record(raw):
    return {
        "cin7_product_id": raw.get("ID"),
        "sku":             raw.get("SKU"),
        "product_name":    raw.get("Name"),
        "location":        raw.get("Location"),
        "location_id":     raw.get("LocationID"),
        "bin":             raw.get("Bin"),
        "on_hand":         raw.get("OnHand",    0),
        "allocated":       raw.get("Allocated", 0),
        "available":       raw.get("Available", 0),
        "on_order":        raw.get("OnOrder",   0),
    }


def fetch_stock_adjustments(client=None, modified_since=None):
    """
    Fetch the stock adjustment / movement history log from Cin7.
    Useful for traceability and audit logs in a QMS.
    """
    client = client or Cin7Client()
    extra  = {}
    if modified_since:
        extra["ModifiedSince"] = modified_since
    adjustments = client.get_all_pages(
        endpoint="stockadjustment",
        result_key="Adjustments",
        extra_params=extra,
    )
    logger.info("Fetched %d stock adjustments from Cin7.", len(adjustments))
    return adjustments


def import_stock(client=None, location_id=None):
    """
    Full pipeline: fetch stock-on-hand and return QMS-mapped dicts.
    Returns list of QMS-mapped stock dicts ready for database upsert.
    """
    raw_records = fetch_stock_on_hand(client=client, location_id=location_id)
    mapped      = [map_stock_record(r) for r in raw_records]
    logger.info("Mapped %d stock records for QMS import.", len(mapped))
    return mapped
