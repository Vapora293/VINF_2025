#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, array_intersect, size,
    expr, array_union, array_distinct
)

spark = (
    SparkSession.builder
    .appName("CrawlerWikiJoinImproved")
    .config("spark.hadoop.io.nativeio.enabled", "false")
    .getOrCreate()
)

# ----------------------------------------------------------
# Helper
# ----------------------------------------------------------
def normalize_text(col_expr):
    return regexp_replace(lower(col_expr), r"[^a-z0-9]+", "")

def tokenize(col_expr):
    return split(
        regexp_replace(lower(col_expr), r"[^\\p{L}\\p{Nd}]+", " "),
        r"\s+"
    )

# ----------------------------------------------------------
# Input
# ----------------------------------------------------------
CRAWLER = "merged_details_cleaned.jsonl"
WIKI    = "wiki_landmarks_merged.jsonl"

crawler = spark.read.json(CRAWLER)
wiki    = spark.read.json(WIKI)

# ----------------------------------------------------------
# Tokenization – CRAWLER
# ----------------------------------------------------------
crawler_tok = (
    crawler
    .withColumn("tokens_c_name", tokenize(col("name")))
    .withColumn("tokens_c_desc",
        tokenize(
            expr("coalesce(description,'') || ' ' || coalesce(highlights,'')")
        )
    )
    .withColumn("city_norm", normalize_text(col("city")))
)

# ----------------------------------------------------------
# Tokenization – WIKI
# ----------------------------------------------------------
wiki_tok = (
    wiki
    .withColumn("tokens_w_title", tokenize(col("title")))
    .withColumn("tokens_w_text", tokenize(col("cleaned_text")))
    .withColumn("city_norm",
        normalize_text(
            expr("""
                CASE
                    WHEN geo_parent IS NOT NULL AND size(geo_parent) > 0
                    THEN geo_parent[0]
                    ELSE ''
                END
            """)
        )
    )
    # all possible city candidates (for better matching)
    .withColumn(
        "city_norm_all",
        array_distinct(
            array_union(
                tokenize(expr("coalesce(geo_parent[0], '')")),
                tokenize(expr("coalesce(geo_parent[1], '')"))
            )
        )
    )
)

c = crawler_tok.alias("c")
w = wiki_tok.alias("w")

# ----------------------------------------------------------
# Improved JOIN condition
# ----------------------------------------------------------

join_condition = (
    (
        col("c.city_norm") == col("w.city_norm")
        |
        col("c.city_norm").isin(*["w.city_norm_all"])
    )
)

joined = (
    c.join(w, join_condition, "inner")
     .withColumn("overlap_name", size(array_intersect(col("c.tokens_c_name"), col("w.tokens_w_title"))))
     .withColumn("overlap_desc", size(array_intersect(col("c.tokens_c_desc"), col("w.tokens_w_text"))))
     .filter(
         (col("overlap_name") >= 1) |
         (col("overlap_desc") >= 2)
     )
)

# ----------------------------------------------------------
# Output writing
# ----------------------------------------------------------
os.makedirs("output", exist_ok=True)

def write_jsonl(df, path):
    print(f"Writing {path}...")
    with open(path, "w", encoding="utf-8") as f:
        for row in df.toLocalIterator():
            f.write(json.dumps(row.asDict(recursive=True), ensure_ascii=False) + "\n")

write_jsonl(joined, "output/full_join.jsonl")

spark.stop()