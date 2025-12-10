#!/usr/bin/env python3
"""
spark_joiner_final_local_write.py

Two outputs (written via Python, not Spark/Hadoop):

1) output/compact_join.jsonl  – minimal useful join output
2) output/full_join.jsonl     – full merged crawler + wiki records

Plus printed statistics about the join.
"""

import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split,
    array_intersect, size, expr
)

# ------------------------------------------------------------------------------
# SparkSession
# ------------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("CrawlerWikiJoinFinalLocalWrite")
    # You can keep this; it just disables native IO if Hadoop obeys it
    .config("spark.hadoop.io.nativeio.enabled", "false")
    .getOrCreate()
)

# ------------------------------------------------------------------------------
# Helper functions (column expressions)
# ------------------------------------------------------------------------------
def normalize_text(col_expr):
    # lowercase + strip non a-z0-9
    return regexp_replace(lower(col_expr), r"[^a-z0-9]+", "")

# ------------------------------------------------------------------------------
# Input files
# ------------------------------------------------------------------------------
CRAWLER_FILE = "merged_details_cleaned.jsonl"
WIKI_FILE    = "wiki_landmarks_merged.jsonl"

crawler_df = spark.read.json(CRAWLER_FILE)
wiki_df    = spark.read.json(WIKI_FILE)

print("Crawler schema:")
crawler_df.printSchema()
print("Wiki schema:")
wiki_df.printSchema()

# ------------------------------------------------------------------------------
# Tokenization
# ------------------------------------------------------------------------------

# tokens from crawler name
crawler_tok = (
    crawler_df
    .withColumn(
        "tokens_c",
        split(
            regexp_replace(
                lower(col("name")),
                r"[^\\p{L}\\p{Nd}]+",
                " "
            ),
            r"\s+"
        )
    )
    .withColumn(
        "city_norm",
        normalize_text(col("city"))
    )
)

# tokens from wiki title + safe city_norm from geo_parent[0] if present
wiki_tok = (
    wiki_df
    .withColumn(
        "tokens_w",
        split(
            regexp_replace(
                lower(col("title")),
                r"[^\\p{L}\\p{Nd}]+",
                " "
            ),
            r"\s+"
        )
    )
    # geo_parent is array; guard against empty arrays
    .withColumn(
        "city_norm",
        normalize_text(
            expr(
                "CASE WHEN geo_parent IS NOT NULL AND size(geo_parent) > 0 "
                "THEN geo_parent[0] ELSE '' END"
            )
        )
    )
)

# ------------------------------------------------------------------------------
# Aliases
# ------------------------------------------------------------------------------
c = crawler_tok.alias("c")
w = wiki_tok.alias("w")

# ------------------------------------------------------------------------------
# Join condition and join
# ------------------------------------------------------------------------------
join_condition = (col("c.city_norm") == col("w.city_norm"))

joined = (
    c.join(w, join_condition, "inner")
     .withColumn(
         "overlap",
         size(array_intersect(col("c.tokens_c"), col("w.tokens_w")))
     )
     .filter(col("overlap") > 0)
)

# ------------------------------------------------------------------------------
# Statistics
# ------------------------------------------------------------------------------
total_crawler = crawler_df.count()
total_wiki = wiki_df.count()
total_pairs = joined.count()
crawler_matched = joined.select(col("c.url")).distinct().count()
wiki_matched = joined.select(col("w.url")).distinct().count()

print("=== JOIN STATISTICS ===")
print(f"Total crawler records:           {total_crawler}")
print(f"Total wiki records:              {total_wiki}")
print(f"Total joined pairs (rows):       {total_pairs}")
print(f"Distinct crawler records joined: {crawler_matched}")
print(f"Distinct wiki records joined:    {wiki_matched}")

# ------------------------------------------------------------------------------
# 1) Compact result (minimal fields)
# ------------------------------------------------------------------------------
compact = joined.select(
    col("c.url").alias("crawler_url"),
    col("c.name").alias("crawler_name"),
    col("c.city").alias("city"),

    col("w.title").alias("wiki_title"),
    col("w.url").alias("wiki_url"),
    col("w.type").alias("wiki_type"),

    col("overlap")
)

# ------------------------------------------------------------------------------
# 2) Full merged result (all fields from both sides)
# ------------------------------------------------------------------------------
full = joined.select(
    *[col("c." + c_name).alias("crawler_" + c_name) for c_name in crawler_df.columns],
    *[col("w." + w_name).alias("wiki_" + w_name) for w_name in wiki_df.columns],
    col("overlap")
)

# ------------------------------------------------------------------------------
# Local JSONL writing (no Hadoop)
# ------------------------------------------------------------------------------
os.makedirs("output", exist_ok=True)

def write_jsonl(df, path):
    """
    Write DataFrame as JSONL using Python, not Spark's Hadoop writer.
    """
    print(f"Writing JSONL to {path} ...")
    with open(path, "w", encoding="utf-8") as f:
        # toLocalIterator streams rows, avoids huge collect() in memory
        for row in df.toLocalIterator():
            f.write(json.dumps(row.asDict(recursive=True), ensure_ascii=False) + "\n")
    print(f"Done writing {path}")

write_jsonl(compact, "output/compact_join.jsonl")
write_jsonl(full, "output/full_join.jsonl")

print("DONE: compact_join.jsonl and full_join.jsonl created in ./output")
spark.stop()