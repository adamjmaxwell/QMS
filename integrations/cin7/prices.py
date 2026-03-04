"""
Cin7 Price Lists Import
Fetches all price tiers / price lists and the per-product prices within them.

Cin7 endpoints:
    GET /ref/pricetier           - lists all price tiers (name, currency, etc.)
    GET /product  (with IncludeProductPrices=true)  - product + price rows together

Each product in the /product response includes a "Prices" array of objects:
    {
      "Name":        "Wholesale",          # price tier name
      "Value":       12.50,               # unit price
      "IsTaxInclusive": false
    }

This module fetches both the tier definitions and the per-product price rows.
"""

import logging
from .client import Cin7Client

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# 1. Price Tiers (the named price levels, e.g. Retail, Wholesale)
# ------------------------------------------------------------------

def fetch_price_tiers(client: Cin7Client = None) -> list:
    """
    Fetch all price tier definitions from Cin7.

    Returns:
        List of price tier dicts, e.g. [{"Name": "Retail", "IsTaxInclusive": true}, ...]
    """
    client = client or Cin7Client()
    data   = client.get("ref/pricetier")
    tiers  = data.get("PriceTiers", [])
    logger.info("Fetched %d price tiers from Cin7.", len(tiers))
    return tiers


def map_price_tier(raw: dict) -> dict:
    """Map a raw price tier to the QMS internal schema."""
    return {
        "name":             raw.get("Name"),
        "is_tax_inclusive": raw.get("IsTaxInclusive", False),
        "currency":         raw.get("Currency"),
        "is_default":       raw.get("IsDefault", False),
    }


# ------------------------------------------------------------------
# 2. Per-product prices (fetched alongside products)
# ------------------------------------------------------------------

def fetch_product_prices(client: Cin7Client = None, modified_since: str = None) -> list:
    """
    Fetch all products with their embedded price arrays from Cin7.

    Cin7 returns a "Prices" list on every product object when you request
    the full product endpoint. Each price entry has: Name, Value, IsTaxInclusive.

    Args:
        client:         Cin7Client instance (optional)
        modified_since: ISO-8601 date for delta imports (optional)

    Returns:
        Flat list of price row dicts, one per (product, tier) combination.
    """
    client = client or Cin7Client()
    extra  = {}
    if modified_since:
        extra["ModifiedSince"] = modified_since

    products = client.get_all_pages("product", "Products", extra_params=extra)

    price_rows = []
    for product in products:
        product_id  = product.get("ID")
        sku         = product.get("SKU")
        for price in product.get("Prices", []):
            price_rows.append({
                "cin7_product_id": product_id,
                "sku":             sku,
                "tier_name":       price.get("Name"),
                "price":           price.get("Value"),
                "is_tax_inclusive": price.get("IsTaxInclusive", False),
            })

    logger.info(
        "Extracted %d price rows across %d products.",
        len(price_rows), len(products),
    )
    return price_rows


# ------------------------------------------------------------------
# 3. Convenience import function
# ------------------------------------------------------------------

def import_prices(
    client: Cin7Client = None,
    modified_since: str = None,
) -> dict:
    """
    Full pipeline: fetch price tiers and per-product prices.

    Returns:
        {
            "tiers":  [ ... ],  # QMS-mapped price tier definitions
            "prices": [ ... ],  # QMS-mapped (product, tier, price) rows
        }
    """
    client = client or Cin7Client()

    tiers       = [map_price_tier(t) for t in fetch_price_tiers(client)]
    price_rows  = fetch_product_prices(client=client, modified_since=modified_since)

    logger.info(
        "Price import complete — %d tiers, %d price rows.",
        len(tiers), len(price_rows),
    )
    return {"tiers": tiers, "prices": price_rows}
