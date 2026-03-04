"""
Cin7 Import Orchestrator
Runs all or selected Cin7 data imports in the correct order.

Usage:
    # Full import (all data types)
    from integrations.cin7.importer import run_full_import
    results = run_full_import()

    # Delta import (only records changed since a given date)
    results = run_full_import(modified_since="2025-01-01T00:00:00Z")

    # Selective import
    from integrations.cin7.importer import Cin7Importer
    importer = Cin7Importer()
    products = importer.import_products()
    stock    = importer.import_stock()

Environment variables required:
    CIN7_ACCOUNT_ID   - your Cin7 Account ID
    CIN7_APP_KEY      - your Cin7 Application Key
"""

import logging
from datetime import datetime, timezone

from .client      import Cin7Client
from .products    import import_products
from .stock       import import_stock, fetch_stock_adjustments
from .prices      import import_prices
from .boms        import import_boms
from .recipe_logs import import_recipe_logs

logger = logging.getLogger(__name__)


class Cin7Importer:
    """
    Stateful importer that shares a single Cin7Client across all import calls.
    Useful when running multiple imports in sequence to reuse the HTTP session.
    """

    def __init__(self, account_id=None, app_key=None):
        self.client = Cin7Client(account_id=account_id, app_key=app_key)

    # ------------------------------------------------------------------
    # Individual import methods
    # ------------------------------------------------------------------

    def import_products(self, modified_since=None):
        logger.info("--- Importing Products ---")
        return import_products(client=self.client, modified_since=modified_since)

    def import_stock(self, location_id=None):
        logger.info("--- Importing Stock On Hand ---")
        return import_stock(client=self.client, location_id=location_id)

    def import_stock_adjustments(self, modified_since=None):
        logger.info("--- Importing Stock Adjustments ---")
        return fetch_stock_adjustments(client=self.client, modified_since=modified_since)

    def import_prices(self, modified_since=None):
        logger.info("--- Importing Price Lists ---")
        return import_prices(client=self.client, modified_since=modified_since)

    def import_boms(self, modified_since=None, include_components=True):
        logger.info("--- Importing Bills of Materials ---")
        return import_boms(
            client=self.client,
            modified_since=modified_since,
            include_components=include_components,
        )

    def import_recipe_logs(self, modified_since=None, status=None, include_consumption=True):
        logger.info("--- Importing Recipe / Production Logs ---")
        return import_recipe_logs(
            client=self.client,
            modified_since=modified_since,
            status=status,
            include_consumption=include_consumption,
        )

    # ------------------------------------------------------------------
    # All-in-one import
    # ------------------------------------------------------------------

    def run_all(self, modified_since=None) -> dict:
        """
        Run all imports in sequence. Returns a dict keyed by data type.

        Args:
            modified_since: Optional ISO-8601 date string.
                            When supplied, only fetches records changed after
                            this date (delta/incremental import).

        Returns:
            {
                "products":           [ ... ],
                "stock":              [ ... ],
                "stock_adjustments":  [ ... ],
                "price_tiers":        [ ... ],
                "prices":             [ ... ],
                "boms":               [ ... ],
                "bom_components":     [ ... ],
                "recipe_orders":      [ ... ],
                "recipe_consumption": [ ... ],
                "imported_at":        "2025-03-04T09:44:00Z",
            }
        """
        start = datetime.now(timezone.utc)
        logger.info("Starting Cin7 full import at %s (modified_since=%s)", start.isoformat(), modified_since)

        products      = self.import_products(modified_since=modified_since)
        stock         = self.import_stock()
        adjustments   = self.import_stock_adjustments(modified_since=modified_since)
        price_data    = self.import_prices(modified_since=modified_since)
        bom_data      = self.import_boms(modified_since=modified_since)
        recipe_data   = self.import_recipe_logs(modified_since=modified_since)

        end = datetime.now(timezone.utc)
        elapsed = (end - start).total_seconds()
        logger.info("Cin7 import complete in %.1fs.", elapsed)

        return {
            "products":           products,
            "stock":              stock,
            "stock_adjustments":  adjustments,
            "price_tiers":        price_data.get("tiers",       []),
            "prices":             price_data.get("prices",      []),
            "boms":               bom_data.get("boms",          []),
            "bom_components":     bom_data.get("components",    []),
            "recipe_orders":      recipe_data.get("orders",     []),
            "recipe_consumption": recipe_data.get("consumption", []),
            "imported_at":        end.isoformat(),
        }


# ------------------------------------------------------------------
# Convenience wrapper
# ------------------------------------------------------------------

def run_full_import(modified_since=None, account_id=None, app_key=None) -> dict:
    """
    Convenience function. Creates a Cin7Importer and runs all imports.

    Example:
        results = run_full_import()
        print(f"Imported {len(results['products'])} products")
        print(f"Imported {len(results['stock'])} stock records")
        print(f"Imported {len(results['boms'])} BOMs")
    """
    return Cin7Importer(account_id=account_id, app_key=app_key).run_all(modified_since=modified_since)


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = run_full_import()
    summary = {k: len(v) if isinstance(v, list) else v for k, v in results.items()}
    print(json.dumps(summary, indent=2))
