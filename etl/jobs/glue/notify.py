import json
import logging
import urllib.request
from typing import Iterable, List

from pyspark.sql import DataFrame, functions as F, types as T


logger = logging.getLogger(__name__)


def find_new_rows(spark, parquet_path: str, comparison_df: DataFrame, key_col: str = "slug") -> DataFrame:
    """Rows of comparison_df whose key is not in the pre-run current SCD2 snapshot at parquet_path.

    Must be called BEFORE update_scd2_table overwrites parquet_path — reads its pre-run state.
    Returns an empty frame (same schema as comparison_df) if the table doesn't exist yet (first
    run: no baseline to diff against, so nothing is reported as new).
    """
    schema = T.StructType([
        T.StructField(key_col, T.StringType(), False),
        T.StructField("is_current", T.BooleanType(), False),
    ])
    try:
        existing = spark.read.schema(schema).parquet(parquet_path).filter("is_current = true").select(key_col)
    except Exception:
        logger.info("No existing table at %s — skipping new-object detection on first run.", parquet_path)
        return comparison_df.limit(0)
    return comparison_df.join(existing, key_col, "left_anti")


def send_webhook(slugs: Iterable[str], webhook_url: str) -> None:
    """POST house.kg detail URLs for newly-discovered, qualifying listings — one call for all of them."""
    slugs = list(slugs)
    if not slugs or not webhook_url:
        return
    urls = "\n".join(f"https://house.kg/details/{slug}" for slug in slugs)
    body = json.dumps({"message": urls}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            logger.info("Webhook notified: %d listing(s), status=%s", len(slugs), resp.status)
    except Exception as exc:
        logger.warning("Webhook call failed for %d listing(s): %s", len(slugs), exc)
