#!/usr/bin/env python3
# diagnostics_wiki.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, size, expr

spark = (
    SparkSession.builder
        .appName("WikiDiagnostics")
        .getOrCreate()
)

WIKI_FILE = "wiki_landmarks_enhanced_fixed.jsonl"
CRAWLER_FILE = "merged_details_cleaned.jsonl"

wiki = spark.read.json(WIKI_FILE)
crawler = spark.read.json(CRAWLER_FILE)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def normalize_text(c):
    """lowercase + remove non-alphanumeric (Unicode letters allowed)"""
    return regexp_replace(lower(c), r"[^\\p{L}\\p{Nd}]+", "")

# ------------------------------------------------------------
# Create diagnostics columns
# ------------------------------------------------------------

wiki_diag = (
    wiki
    .withColumn("geo_parent_norm",
                normalize_text(expr("geo_parent[1]")))  # safe: null → null
    .withColumn("title_tokens",
                split(regexp_replace(lower(col("title")),
                                     r"[^\\p{L}\\p{Nd}]+", " "), r"\\s+"))
)

crawler_diag = (
    crawler
    .withColumn("city_norm",
                normalize_text(col("city")))
)

crawler_cities = [r["city_norm"] for r in crawler_diag.select("city_norm").distinct().collect()]

print("\n====================")
print("1) BASIC WIKI STATS")
print("====================")

print("Total wiki rows:", wiki.count())
print("Wiki columns:", wiki.columns)

print("\n===================================")
print("2) geo_parent diagnostics")
print("===================================")

print("geo_parent is NULL:", wiki.filter(col("geo_parent").isNull()).count())
print("geo_parent not NULL:", wiki.filter(col("geo_parent").isNotNull()).count())

print("\nDistinct geo_parent_norm (first 100):")
wiki_diag.select("geo_parent_norm").distinct().show(100, False)

print("\nCount of rows with geo_parent_norm NULL or empty:")
print(wiki_diag.filter((col("geo_parent_norm").isNull()) | (col("geo_parent_norm") == "")).count())

print("\n===================================")
print("3) Title diagnostics")
print("===================================")

print("Title with <=1 token:",
      wiki_diag.filter(size(col("title_tokens")) <= 1).count())

print("\nExample titles with few tokens:")
wiki_diag.filter(size(col("title_tokens")) <= 2).select("title").show(20, False)

print("\n===================================")
print("4) Do wiki cities overlap with crawler cities?")
print("===================================")

wiki_city_overlap = wiki_diag.filter(col("geo_parent_norm").isin(crawler_cities))
print("Wiki rows where geo_parent_norm matches crawler city_norm:", wiki_city_overlap.count())

print("\nDistinct overlapping cities:")
wiki_city_overlap.select("geo_parent_norm").distinct().show(50, False)

print("\n===================================")
print("5) Print top 10 wiki rows with geo_parent")
print("===================================")
wiki.select("title", "geo_parent").show(10, False)

print("\nDONE.")
spark.stop()