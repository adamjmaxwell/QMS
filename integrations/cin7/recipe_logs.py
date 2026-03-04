"""
Cin7 Production / Recipe Logs Import
Fetches production order runs (recipe/job logs) from Cin7.

In Cin7/DEAR, a Production Order represents one execution of a BOM/recipe.
It records:
  - Which finished product was produced and in what quantity
  - Which BOM / recipe was used
  - The actual components consumed (may differ from the BOM if there were
    substitutions or waste variances)
  - Start/completion dates and status

Cin7 endpoints:
    GET /production               - production order headers (job log)
    GET /production?ID=<id>       - full detail including component consumption lines

Key production order fields:
    ProductionID, OrderNumber, ProductID, SKU, ProductName, BOMID,
    OrderDate, CompletionDate, Status, OrderQuantity, ProducedQuantity, UOM

Key component consumption line fields:
    ComponentProductID, ComponentSKU, ComponentName,
    PlannedQuantity, ActualQuantity, UOM, WastageQuantity
"""

import logging
from .client import Cin7Client

logger = logging.getLogger(__name__)

PRODUCTION_ENDPOINT   = "production"
PRODUCTION_RESULT_KEY = "Productions"


def fetch_production_orders(
    client=None,
    modified_since=None,
    status=None,
):
    """
    Fetch production order headers (recipe log) from Cin7.

    Args:
        client:         Cin7Client instance (optional)
        modified_since: ISO-8601 date string for delta imports (optional)
        status:         Filter by order status, e.g. "Draft", "Released",
                        "InProgress", "Completed", "Voided" (optional)

    Returns:
        List of raw production order dicts.
    """
    client = client or Cin7Client()
    extra  = {}
    if modified_since:
        extra["ModifiedSince"] = modified_since
    if status:
        extra["Status"] = status

    orders = client.get_all_pages(
        PRODUCTION_ENDPOINT, PRODUCTION_RESULT_KEY, extra_params=extra
    )
    logger.info("Fetched %d production orders from Cin7.", len(orders))
    return orders


def fetch_production_order_detail(order_id, client=None):
    """
    Fetch a single production order with its full component consumption lines.

    Args:
        order_id: Cin7 ProductionID string
        client:   Cin7Client instance (optional)

    Returns:
        Raw production order dict including "Components" list.
    """
    client = client or Cin7Client()
    data   = client.get(PRODUCTION_ENDPOINT, params={"ID": order_id})
    return data.get("Production", {})


def map_production_order(raw):
    """Map a raw Cin7 production order to the QMS recipe log schema."""
    return {
        "cin7_production_id":  raw.get("ProductionID"),
        "order_number":        raw.get("OrderNumber"),
        "cin7_product_id":     raw.get("ProductID"),
        "sku":                 raw.get("SKU"),
        "product_name":        raw.get("ProductName"),
        "cin7_bom_id":         raw.get("BOMID"),

        # Dates
        "order_date":          raw.get("OrderDate"),
        "required_by_date":    raw.get("RequiredByDate"),
        "completion_date":     raw.get("CompletionDate"),

        # Status & quantities
        "status":              raw.get("Status"),        # Draft, Released, Completed, etc.
        "planned_quantity":    raw.get("OrderQuantity",   0),
        "produced_quantity":   raw.get("ProducedQuantity", 0),
        "uom":                 raw.get("UOM"),

        # Quality / notes
        "notes":               raw.get("Notes"),
        "location_id":         raw.get("LocationID"),
        "location":            raw.get("Location"),
    }


def map_consumption_line(production_id, raw):
    """Map a raw component consumption line to the QMS recipe log detail schema."""
    return {
        "cin7_production_id":  production_id,
        "cin7_component_id":   raw.get("ComponentProductID"),
        "component_sku":       raw.get("ComponentSKU"),
        "component_name":      raw.get("ComponentName"),
        "planned_quantity":    raw.get("PlannedQuantity",  0),
        "actual_quantity":     raw.get("ActualQuantity",   0),
        "wastage_quantity":    raw.get("WastageQuantity",  0),
        "uom":                 raw.get("UOM"),
    }


def import_recipe_logs(
    client=None,
    modified_since=None,
    status=None,
    include_consumption=True,
):
    """
    Full recipe log import pipeline.

    Args:
        client:              Cin7Client instance (optional)
        modified_since:      ISO-8601 date for delta imports (optional)
        status:              Filter production orders by status (optional)
        include_consumption: If True (default), fetches component consumption
                             lines for each order (one extra API call per order).
                             Set to False for a fast header-only import.

    Returns:
        {
            "orders":      [ ... ],  # QMS-mapped production order headers
            "consumption": [ ... ],  # QMS-mapped component consumption lines
        }
    """
    client          = client or Cin7Client()
    raw_orders      = fetch_production_orders(client=client, modified_since=modified_since, status=status)
    mapped_orders   = [map_production_order(o) for o in raw_orders]
    all_consumption = []

    if include_consumption:
        for order in raw_orders:
            order_id = order.get("ProductionID")
            detail   = fetch_production_order_detail(order_id, client=client)
            for line in detail.get("Components", []):
                all_consumption.append(map_consumption_line(order_id, line))

        logger.info(
            "Recipe log import complete — %d orders, %d consumption lines.",
            len(mapped_orders), len(all_consumption),
        )
    else:
        logger.info("Recipe log header-only import — %d orders.", len(mapped_orders))

    return {"orders": mapped_orders, "consumption": all_consumption}
