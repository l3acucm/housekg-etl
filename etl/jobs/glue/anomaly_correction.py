import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence

import boto3
from pyspark.sql import DataFrame, functions as F


logger = logging.getLogger(__name__)


def flag_anomalies(
    df: DataFrame,
    *,
    value_col: str,
    group_cols: Sequence[str],
    hard_low: float,
    hard_high: float,
    min_group_size: int = 8,
    mad_k: float = 3.0,
) -> DataFrame:
    """Return df with a boolean `is_anomaly` column.

    A row is anomalous if either:
      - hard_prior: value_col is outside [hard_low, hard_high] (catches physically implausible values)
      - log-MAD one-sided: ln(value_col) > group_median(ln(value_col)) + mad_k * group_MAD, when the
        row's group has at least `min_group_size` samples. Small groups fall back to hard_prior only.

    MAD is the median absolute deviation in log space; one-sided because bad listings systematically
    produce too-high unit prices (square misreported small). log space because prices are right-skewed.
    """
    group_cols = list(group_cols)
    hard = (F.col(value_col) < hard_low) | (F.col(value_col) > hard_high)
    positive = F.col(value_col) > 0

    log_v = F.log(F.col(value_col))

    medians = (
        df.filter(positive)
        .groupBy(*group_cols)
        .agg(
            F.count(F.lit(1)).alias("_n"),
            F.expr(f"percentile_approx(ln({value_col}), 0.5)").alias("_median"),
        )
    )

    with_med = df.join(medians, group_cols, "left")
    abs_dev = with_med.withColumn(
        "_abs_dev",
        F.when(positive & F.col("_median").isNotNull(), F.abs(log_v - F.col("_median"))),
    )
    mads = abs_dev.filter(F.col("_abs_dev").isNotNull()).groupBy(*group_cols).agg(
        F.expr("percentile_approx(_abs_dev, 0.5)").alias("_mad")
    )

    enriched = with_med.join(mads, group_cols, "left")

    has_enough = F.col("_n") >= F.lit(min_group_size)
    mad_usable = F.col("_mad").isNotNull() & (F.col("_mad") > 0)
    threshold = F.col("_median") + F.lit(mad_k) * F.col("_mad")
    group_anomaly = positive & has_enough & mad_usable & (log_v > threshold)

    return (
        enriched.withColumn("is_anomaly", hard | group_anomaly)
        .drop("_n", "_median", "_mad")
    )


_SYSTEM_PROMPT = (
    "You validate Kyrgyz real-estate listings. You read seller descriptions in Russian, Kyrgyz or "
    "English and extract numeric facts that contradict the structured fields. Conversions you must "
    "apply: 1 гектар / 1 ГА = 100 ares (соток); 1 сотка = 1 are = 100 m². Respond with strict "
    "JSON only — no markdown fences, no prose, no explanations outside the JSON."
)


def _build_user_message(
    *,
    kind: str,
    unit_label: str,
    structured_square: Optional[float],
    structured_price_usd: Optional[float],
    implied_per_unit: Optional[float],
    description: str,
) -> str:
    desc = (description or "")[:3000]
    return (
        f"Listing kind: {kind}.\n"
        f"Structured fields claim: area = {structured_square} {unit_label}, total price = ${structured_price_usd}, "
        f"implied price-per-{unit_label} = ${implied_per_unit}. This is flagged as anomalous.\n\n"
        f"Seller's description:\n\"\"\"\n{desc}\n\"\"\"\n\n"
        f"Your task: extract the DOCUMENTED legal area and the DOCUMENTED total price for this listing if "
        f"the description clearly states them differently from the structured fields.\n\n"
        f"Rules:\n"
        f"1. Return the area as stated in legal documents (по документу / official / titled). IGNORE any "
        f"\"actual\" / \"по факту\" / \"фактически\" / \"available\" / \"usable\" area that EXCEEDS the "
        f"documented one — sellers commonly report a larger usable area, but we want the legal/titled one.\n"
        f"2. If the description says \"Цена за N гектар X$\" / \"Price per N hectares $X\" / similar (where "
        f"N is 1 or another whole-plot size descriptor like 0.5, 2), interpret N as the TOTAL plot size — "
        f"NOT as a per-unit price. Same pattern with ares/соток.\n"
        f"3. Convert units to {unit_label}: 1 гектар / 1 ГА / 1 hectare = 100 ares; 1 сотка = 1 are = 100 m².\n"
        f"4. If the documented area in the description matches the structured area, OR if no different "
        f"documented area is clearly stated, return null for actual_square_{unit_label}. Same for price. "
        f"Do not guess.\n\n"
        f"Respond with ONLY this JSON object (no markdown, no prose):\n"
        f"{{\n"
        f"  \"actual_square_{unit_label}\": <number or null>,\n"
        f"  \"actual_price_usd\": <number or null>,\n"
        f"  \"confidence\": \"high\" | \"medium\" | \"low\",\n"
        f"  \"reason\": \"<one short sentence>\"\n"
        f"}}"
    )


_JSON_BLOCK = re.compile(r"\{[\s\S]*\}")


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = _JSON_BLOCK.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _invoke_haiku(client, model_id: str, user_message: str) -> str:
    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 400,
            "temperature": 0.0,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}],
        }
    )
    resp = client.invoke_model(modelId=model_id, body=body, contentType="application/json")
    payload = json.loads(resp["body"].read())
    return payload["content"][0]["text"]


def correct_anomalies_with_haiku(
    rows: List[Dict[str, Any]],
    *,
    kind: str,
    unit_label: str,
    model_id: str,
    region: str,
    max_calls: int = 100,
    max_workers: int = 8,
) -> Dict[str, Dict[str, Any]]:
    """Call Bedrock Haiku once per anomalous row from the driver. Returns {slug: parsed_json}.

    Hard-caps at `max_calls`; excess anomalies are logged and skipped (no correction applied).
    Rows without a usable description are skipped silently.
    """
    if not rows:
        return {}

    eligible = [r for r in rows if (r.get("description") or "").strip()]
    capped = eligible[:max_calls]
    if len(rows) > max_calls:
        logger.warning(
            "LLM correction cap reached: %d anomalies exceed cap of %d; skipping %d.",
            len(rows), max_calls, len(rows) - max_calls,
        )

    client = boto3.client("bedrock-runtime", region_name=region)

    def task(row: Dict[str, Any]):
        slug = row["slug"]
        try:
            msg = _build_user_message(
                kind=kind,
                unit_label=unit_label,
                structured_square=row.get("structured_square"),
                structured_price_usd=row.get("structured_price_usd"),
                implied_per_unit=row.get("implied_per_unit"),
                description=row["description"],
            )
            text = _invoke_haiku(client, model_id, msg)
            return slug, _extract_json(text)
        except Exception as exc:
            logger.warning("Bedrock call failed for slug=%s: %s", slug, exc)
            return slug, None

    out: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed([ex.submit(task, r) for r in capped]):
            slug, parsed = fut.result()
            if parsed:
                out[slug] = parsed

    logger.info("Bedrock corrections: requested=%d, returned=%d", len(capped), len(out))
    return out
