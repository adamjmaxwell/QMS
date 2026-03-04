"""
Cin7 Products Import
Fetches all products (SKUs, categories, units of measure, costs) from Cin7.

Cin7 endpoint: GET /product
Key response fields per product:
    ID, SKU, Name, Category, UOM, CostingMethod, AverageCost, DefaultCost,
    Type, IsActive, Tags, ShortDescription, Description, Barcode, BrandName,
    WeightUnit, Weight, LengthUnit, Length, Width, Height, Suppliers[]
"""

import logging
from .client import Cin7Client

logger = logging.getLogger(__name__)

ENDPOINT   = "product"
RESULT_KEY = "Products"


def fetch_products(client: Cin7Client = None, modified_since: str = None) -> list:
    """
    Fetch all products from Cin7.

    Args:
        client:        Cin7Client instance (created automatically if not supplied)
        modified_since: ISO-8601 date string, e.g. "2025-01-01T00:00:00Z".
                        When supplied, only products modified after this date
                        are returned (delta import).

    Returns:
        List of raw product dicts from the Cin7 API.
    """
    client = client or Cin7Client()
    extra  = {}
    if modified_since:
        extra["ModifiedSince"] = modified_since

    products = client.get_all_pages(ENDPOINT, RESULT_KEY, extra_params=extra)
    logger.info("Fetched %d products from Cin7.", len(products))
    return products


def map_product(raw: dict) -> dict:
    """
    Map a raw Cin7 product dict to the QMS internal product schema.
    Extend / adjust field mappings to match your database columns.
    """
    return {
        # Identity
        "cin7_id":          raw.get("ID"),
        "sku":              raw.get("SKU"),
        "name":             raw.get("Name"),
        "barcode":          raw.get("Barcode"),

        # Classification
        "category":         raw.get("Category"),
        "product_type":     raw.get("Type"),          # "I" = Inventory, "S" = Service, etc.
        "brand":            raw.get("BrandName"),
        "tags":             raw.get("Tags"),
        "is_active":        raw.get("IsActive", True),

        # Measurements
        "unit_of_measure":  raw.get("UOM"),
        "weight":           raw.get("Weight"),
        "weight_unit":      raw.get("WeightUnit"),
        "length":           raw.get("Length"),
        "width":            raw.get("Width"),
        "height":           raw.get("Height"),
        "dimensions_unit":  raw.get("LengthUnit"),

        # Costing
        "costing_method":   raw.get("CostingMethod"),  # "FIFO", "FEFO", "Average", etc.
        "average_cost":     raw.get("AverageCost"),
        "default_cost":     raw.get("DefaultCost"),

        # Description
        "short_description": raw.get("ShortDescription"),
        "description":       raw.get("Description"),
    }


def import_products(client: Cin7Client = None, modified_since: str = None) -> list:
    """
    Full pipeline: fetch products from Cin7 and return them as QMS-mapped dicts.

    Args:
        client:         Cin7Client instance (optional)
        modified_since: ISO-8601 date for delta imports (optional)

    Returns:
        List of QMS-mapped product dicts ready for database upsert.
    """
    raw_products = fetch_products(client=client, modified_since=modified_since)
    mapped       = [map_product(p) for p in raw_products]
    logger.info("Mapped %d products for QMS import.", len(mapped))
    return mapped
