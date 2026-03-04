"""
Cin7 Bill of Materials (BOM) Import
Fetches all BOM / assembly definitions and their component lines.

In Cin7/DEAR, BOMs live under the Production module.

Cin7 endpoints:
    GET /production/bom            - BOM headers (one record per finished product)
    GET /production/bom?ID=<id>    - BOM header + all component lines

Key BOM header fields:
    BOMID, ProductID, SKU, Name, Quantity (output qty), UOM, IsActive

Key BOM line (component) fields:
    ComponentProductID, ComponentSKU, ComponentName, Quantity, UOM,
    WastagePercent, Notes
"""

import logging
from .client import Cin7Client

logger = logging.getLogger(__name__)

BOM_ENDPOINT   = "production/bom"
BOM_RESULT_KEY = "BOMList"


def fetch_boms(client: Cin7Client = None, modified_since: str = None) -> list:
    """
    Fetch all BOM headers from Cin7.

    Args:
        client:         Cin7Client instance (optional)
        modified_since: ISO-8601 date string for delta imports (optional)

    Returns:
        List of raw BOM header dicts (without component lines).
    """
    client = client or Cin7Client()
    extra  = {}
    if modified_since:
        extra["ModifiedSince"] = modified_since

    boms = client.get_all_pages(BOM_ENDPOINT, BOM_RESULT_KEY, extra_params=extra)
    logger.info("Fetched %d BOM headers from Cin7.", len(boms))
    return boms


def fetch_bom_with_components(bom_id: str, client: Cin7Client = None) -> dict:
    """
    Fetch a single BOM with its full component lines by BOM ID.

    Args:
        bom_id: Cin7 BOMID string
        client: Cin7Client instance (optional)

    Returns:
        Raw BOM dict including a "Components" list.
    """
    client = client or Cin7Client()
    data   = client.get(BOM_ENDPOINT, params={"ID": bom_id})
    return data.get("BOM", {})


def map_bom_header(raw: dict) -> dict:
    """Map raw Cin7 BOM header to QMS internal schema."""
    return {
        "cin7_bom_id":       raw.get("BOMID"),
        "cin7_product_id":   raw.get("ProductID"),
        "sku":               raw.get("SKU"),
        "product_name":      raw.get("Name"),
        "output_quantity":   raw.get("Quantity", 1),
        "output_uom":        raw.get("UOM"),
        "is_active":         raw.get("IsActive", True),
        "notes":             raw.get("Notes"),
    }


def map_bom_component(bom_id: str, raw: dict) -> dict:
    """Map a single BOM component line to QMS internal schema."""
    return {
        "cin7_bom_id":           bom_id,
        "cin7_component_id":     raw.get("ComponentProductID"),
        "component_sku":         raw.get("ComponentSKU"),
        "component_name":        raw.get("ComponentName"),
        "quantity":              raw.get("Quantity", 0),
        "uom":                   raw.get("UOM"),
        "wastage_percent":       raw.get("WastagePercent", 0),
        "notes":                 raw.get("Notes"),
    }


def import_boms(
    client: Cin7Client = None,
    modified_since: str = None,
    include_components: bool = True,
) -> dict:
    """
    Full BOM import pipeline.

    Args:
        client:             Cin7Client instance (optional)
        modified_since:     ISO-8601 date for delta imports (optional)
        include_components: If True (default), fetches the component lines for
                            each BOM with a second API call per BOM.
                            Set to False for a fast header-only import.

    Returns:
        {
            "boms":       [ ... ],  # QMS-mapped BOM headers
            "components": [ ... ],  # QMS-mapped component lines (flat list)
        }
    """
    client      = client or Cin7Client()
    raw_boms    = fetch_boms(client=client, modified_since=modified_since)
    mapped_boms = [map_bom_header(b) for b in raw_boms]
    all_components = []

    if include_components:
        for bom in raw_boms:
            bom_id     = bom.get("BOMID")
            detail     = fetch_bom_with_components(bom_id, client=client)
            components = detail.get("Components", [])
            for comp in components:
                all_components.append(map_bom_component(bom_id, comp))

        logger.info(
            "BOM import complete — %d BOMs, %d component lines.",
            len(mapped_boms), len(all_components),
        )
    else:
        logger.info("BOM header-only import — %d BOMs.", len(mapped_boms))

    return {"boms": mapped_boms, "components": all_components}
