import json
import logging
import urllib.request
from typing import Iterable, List

from pyspark.sql import DataFrame, functions as F, types as T


logger = logging.getLogger(__name__)

TELEGRAM_MESSAGE_LIMIT = 4096  # the webhook forwards `message` into a Telegram bot with this cap


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


def _chunk_lines(lines: List[str], limit: int) -> List[List[str]]:
    """Greedily pack lines into groups whose newline-joined length stays within `limit`."""
    chunks: List[List[str]] = []
    current: List[str] = []
    current_len = 0
    for line in lines:
        added = len(line) + (1 if current else 0)  # +1 for the joining newline
        if current and current_len + added > limit:
            chunks.append(current)
            current, current_len = [line], len(line)
        else:
            current.append(line)
            current_len += added
    if current:
        chunks.append(current)
    return chunks


def send_webhook(slugs: Iterable[str], webhook_url: str) -> None:
    """POST house.kg detail URLs for newly-discovered, qualifying listings.

    Split across multiple requests if the joined URL list would exceed Telegram's 4096-char
    message cap on the receiving end — one POST per chunk, each independently.
    """
    slugs = list(slugs)
    if not slugs or not webhook_url:
        return
    urls = [f"https://house.kg/details/{slug}" for slug in slugs]
    for chunk in _chunk_lines(urls, TELEGRAM_MESSAGE_LIMIT):
        body = json.dumps({"message": "\n".join(chunk)}).encode("utf-8")
        # Matches `curl -d '<json>' $WEBHOOK_URL` exactly (the confirmed-working call) — no
        # explicit Content-Type (curl -d defaults to application/x-www-form-urlencoded, not
        # application/json) and a curl-shaped User-Agent instead of Python's default, since
        # the endpoint 403'd a request that only differed from this in those two headers.
        req = urllib.request.Request(
            webhook_url,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "curl/8.4.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                logger.info("Webhook notified: %d listing(s), status=%s", len(chunk), resp.status)
        except Exception as exc:
            logger.warning("Webhook call failed for %d listing(s): %s", len(chunk), exc)
