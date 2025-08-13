# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install --quiet openpyxl pandas time-uuid

# COMMAND ----------
# Imports, Spark session, and SAFE runtime knobs (no hardcoded schemas; uses UC Volumes for schema/checkpoints)

import os
import re
import json
import uuid
import datetime as dt
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType

from utils import constants
from utils.etl_utils import (
    generate_json_config,
    add_audit_columns,
    write_audit_log,
    load_data_lineage,
    write_data_lineage,
    incremental_delta_load,
)
from transformations.transformations import transform
from target_loader.target_loader import write_target

spark = SparkSession.builder.getOrCreate()

# ---- SAFE config readers (Spark conf may raise if key missing with Spark Connect) ----
def _conf_int(key: str, env: str, default: int) -> int:
    try:
        v = spark.conf.get(key); return int(v)
    except Exception:
        pass
    ev = os.getenv(env)
    return int(ev) if ev else int(default)

def _conf_str(key: str, env: str, default: str) -> str:
    try:
        v = spark.conf.get(key); return str(v)
    except Exception:
        pass
    ev = os.getenv(env)
    return ev if ev else default

# Preview size only
LOG_SAMPLE_ROWS       = _conf_int("dtf.preview.rows",               "DTF_PREVIEW_ROWS",               20)

# These will be overridden dynamically to UC Volumes after we read metadata
CHECKPOINT_BASE       = _conf_str("dtf.stream.checkpoint_base",     "DTF_STREAM_CHECKPOINT_BASE",     "")
SCHEMA_BASE           = _conf_str("dtf.stream.schema_base",         "DTF_STREAM_SCHEMA_BASE",         "")
BRONZE_BASE_PATH      = _conf_str("dtf.bronze.base_path",           "DTF_BRONZE_BASE_PATH",           "")

# COMMAND ----------
# Utility helpers

def _job_name() -> str:
    try:
        name = spark.conf.get("spark.databricks.job.runDefinitionName")
        if name: return name
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
        return dbutils.notebook().getContext().notebookPath().getOrElse("DTF_Streaming")
    except Exception:
        return "DTF_Streaming"

def _strip_glob_dir(path: str) -> str:
    """Streaming sources must be concrete directories; trim trailing '/**' if present."""
    path = str(path).strip()
    return re.sub(r"/\*\*$", "", path)

def _volume_root_from(path: str) -> Optional[str]:
    """
    Extract '/Volumes/<catalog>/<schema>/<volume>' from a UC Volume path like:
    /Volumes/mycat/mysch/myvol/subdir/...
    """
    m = re.match(r"^/Volumes/([^/]+)/([^/]+)/([^/]+)", str(path).strip())
    if not m:
        return None
    return f"/Volumes/{m.group(1)}/{m.group(2)}/{m.group(3)}"

def _mkdirs(path: str):
    # Best-effort create; not required, as writers typically create paths on first write
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
        dbutils.fs.mkdirs(path)
    except Exception:
        pass

def _autoloader_csv_stream(src_dir: str, schema_loc: str, schema_hints: str | None = None):
    """
    Build an Auto Loader stream for CSV with dynamic schema inference & evolution.
    - Includes existing files
    - Adds new columns automatically (schema evolution)
    - Captures unexpected content into _rescued_data
    """
    reader = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
    )
    if schema_hints:
        # Pin critical columns (e.g., join keys) while still inferring the rest
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
    return reader.load(src_dir)

# COMMAND ----------
# STEP 1 — Load / snapshot configuration (Excel → JSON)

job_name = _job_name()
script_start_ts = dt.datetime.utcnow()
print(f"=== DTF STRUCTURED STREAMING (Auto Loader, UC Volumes) {job_name} ===")
print(f"Started at {script_start_ts:%Y-%m-%d %H:%M:%S UTC}")

print("STEP 1 preparing JSON configuration")
cfg_path = generate_json_config()
print(f"Using config snapshot: {cfg_path}")

with open(cfg_path) as fh:
    cfg = json.load(fh)

meta_rows: List[dict]   = cfg["metadata_info"]
mapping_rules: List[dict] = cfg["dtf_mapping"]
fj_rules: List[dict]      = cfg["filter_join_rules"]

print(f"metadata rows: {len(meta_rows)}, mapping rules: {len(mapping_rules)}, filter/join: {len(fj_rules)}")

# Build dtf_mapping_df for incremental merges (used inside foreachBatch)
if mapping_rules:
    sample_keys = list(mapping_rules[0].keys())
    map_schema = StructType([StructField(k, StringType(), True) for k in sample_keys])
    dtf_mapping_df = spark.createDataFrame(mapping_rules, map_schema)
else:
    from pyspark.sql.types import StructType as _StructType
    dtf_mapping_df = spark.createDataFrame([], _StructType([]))
    print("Warning: dtf_mapping empty")

# Identify streaming-enabled rows (expecting 'products' and 'sales')
rows_by_alias = {
    str(r.get("alias")).strip(): r
    for r in meta_rows
    if str(r.get("is_active","Y")).upper()=="Y" and str(r.get("is_streaming_enabled","N")).upper()=="Y"
}
sales_row    = rows_by_alias.get("sales")
products_row = rows_by_alias.get("products")
assert products_row and sales_row, "Expected 'products' and 'sales' rows with is_streaming_enabled='Y' in metadata_info"

# COMMAND ----------
# STEP 2 — Resolve UC Volume roots and derive paths for schema/checkpoints/bronze

sales_dir    = _strip_glob_dir(sales_row["source_path"])
products_dir = _strip_glob_dir(products_row["source_path"])

# Derive a UC Volume root automatically from the provided source paths
vol_root = _volume_root_from(products_dir) or _volume_root_from(sales_dir)
if not vol_root:
    raise ValueError(
        f"Source paths must be under a UC Volume. Got:\n  sales: {sales_dir}\n  products: {products_dir}\n"
        "Expected pattern: /Volumes/<catalog>/<schema>/<volume>/..."
    )

# If user didn't override via conf/env, derive these under the same Volume
if not SCHEMA_BASE:
    SCHEMA_BASE = f"{vol_root}/_dtf_schemas"
if not CHECKPOINT_BASE:
    CHECKPOINT_BASE = f"{vol_root}/_dtf_checkpoints"
if not BRONZE_BASE_PATH:
    BRONZE_BASE_PATH = f"{vol_root}/bronze"

# Ensure directories exist (best-effort)
for p in [
    f"{SCHEMA_BASE}/products", f"{SCHEMA_BASE}/sales",
    f"{CHECKPOINT_BASE}/products", f"{CHECKPOINT_BASE}/sales",
    f"{BRONZE_BASE_PATH}/products", f"{BRONZE_BASE_PATH}/sales",
]:
    _mkdirs(p)

schema_products = f"{SCHEMA_BASE}/products"
schema_sales    = f"{SCHEMA_BASE}/sales"
bronze_products_path = f"{BRONZE_BASE_PATH}/products"
bronze_sales_path    = f"{BRONZE_BASE_PATH}/sales"
chk_products = f"{CHECKPOINT_BASE}/products"
chk_sales    = f"{CHECKPOINT_BASE}/sales"

print("Sales source dir   :", sales_dir)
print("Products source dir:", products_dir)
print("UC Volume root     :", vol_root)
print("Schema registry    :", schema_products, "|", schema_sales)
print("Bronze products    :", bronze_products_path)
print("Bronze sales       :", bronze_sales_path)
print("Checkpoints        :", chk_products, "|", chk_sales)

# COMMAND ----------
# STEP 3 — Start products stream → Bronze (Delta) with dynamic schema (UC Volumes)

# Hint critical columns (optional, recommended for join keys/numeric types)
PRODUCTS_SCHEMA_HINTS = "product_id INT, unit_price DOUBLE"

products_stream = (
    _autoloader_csv_stream(
        src_dir=products_dir,
        schema_loc=schema_products,
        schema_hints=PRODUCTS_SCHEMA_HINTS
    )
    .withColumn("src_ingest_ts", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", chk_products)
    .option("mergeSchema", "true")
    .outputMode("append")
    .start(bronze_products_path)
)

print("Products stream (Auto Loader) started → Bronze.")

# COMMAND ----------
# STEP 4 — Sales stream with foreachBatch (joins each batch with latest Bronze products; UC Volumes)

SALES_SCHEMA_HINTS = "order_id INT, product_id INT, quantity INT, unit_price DOUBLE"

def _process_sales_batch(batch_df: DataFrame, batch_id: int):
    run_id = uuid.uuid4().hex[:12]
    batch_start = dt.datetime.utcnow()
    print(f"\n=== SALES BATCH START run_id={run_id} batch_id={batch_id} rows={batch_df.count()} at {batch_start:%Y-%m-%d %H:%M:%S UTC} ===")

    # Latest products snapshot from Bronze
    products_df = spark.read.format("delta").load(bronze_products_path)

    # Build source dict for DTF transform (join rule from Excel applies: sales.product_id = products.product_id)
    src_dfs: Dict[str, DataFrame] = {
        "sales": batch_df,
        "products": products_df
    }

    # Transform → add audit cols
    final_df = (
        transform(src_dfs, mapping_rules, fj_rules)
        .transform(add_audit_columns)
    )

    try:
        display(final_df.limit(LOG_SAMPLE_ROWS))
    except Exception:
        pass

    # Incremental MERGE if configured in dtf_mapping (rule_type='Incremental')
    try:
        incremental_delta_load(final_df, dtf_mapping_df)
    except Exception as e:
        print(f"[stream] incremental_delta_load skipped or failed: {e}")

    # Persist to targets (unified Silver table handled by write_target via metadata_info)
    write_target(final_df, meta_rows, mapping_rules)

    # Audit + Lineage per batch
    batch_end = dt.datetime.utcnow()
    try:
        write_audit_log(run_id, f"{_job_name()}::sales", batch_start, batch_end, src_dfs, final_df)
        print(f"[stream] Audit logged to {constants.AUDIT_TABLE}")
    except Exception as e:
        print(f"[stream] audit log failed: {e}")

    try:
        lineage_df = load_data_lineage(cfg, batch_start, batch_end, run_id)
        write_data_lineage(lineage_df, constants.DATA_LINEAGE_TABLE)
        print(f"[stream] Lineage written to {constants.DATA_LINEAGE_TABLE}")
    except Exception as e:
        print(f"[stream] lineage failed: {e}")

    print(f"=== SALES BATCH END run_id={run_id} duration={(batch_end - batch_start).total_seconds():.1f}s ===")

sales_stream = (
    _autoloader_csv_stream(
        src_dir=sales_dir,
        schema_loc=schema_sales,
        schema_hints=SALES_SCHEMA_HINTS
    )
    .writeStream
    .foreachBatch(_process_sales_batch)
    .option("checkpointLocation", chk_sales)
    .outputMode("update")  # ignored by foreachBatch, required by API
    .trigger(processingTime="30 seconds")
    .start()
)

print("Sales stream (Auto Loader) started with foreachBatch.")

# COMMAND ----------
# STEP 5 — (Optional) also persist raw sales to Bronze (parallel sink on UC Volumes)

# If you want raw sales persisted for auditing/backfill, uncomment this block:
# raw_sales_to_bronze = (
#     _autoloader_csv_stream(
#         src_dir=sales_dir,
#         schema_loc=f"{SCHEMA_BASE}/sales_bronze",
#         schema_hints=SALES_SCHEMA_HINTS
#     )
#     .withColumn("src_ingest_ts", F.current_timestamp())
#     .writeStream
#     .format("delta")
#     .option("checkpointLocation", f"{CHECKPOINT_BASE}/sales_bronze")
#     .option("mergeSchema", "true")
#     .outputMode("append")
#     .start(bronze_sales_path)
# )
# print("Raw sales → Bronze stream started.")

# COMMAND ----------
# STEP 6 — Await termination

print("Streaming is running (Auto Loader, UC Volumes). Press Stop to terminate the notebook.")
spark.streams.awaitAnyTermination()
