import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    regexp_replace,
    split,
    array_intersect,
    size,
    expr,
    when,
    lit,
    coalesce,
    udf,
    explode
)
from pyspark.sql.types import ArrayType, StringType
import json
import re
from collections import Counter

# --- Configuration ---
CRAWLER_FILE = "merged_details_cleaned.jsonl"
WIKI_FILE = "wiki_landmarks_merged.jsonl"
WIKI_CITIES_FILE = "wiki_cities.jsonl"
TARGET_CITIES_FILE = "target_cities.json"
OUTPUT_DIR = "output"

# Stop words for tokenization (English + Portuguese + Common Terms)
STOP_WORDS = {
    "the", "a", "an", "of", "in", "on", "at", "to", "from", "by", "with", "and", "or", "for",
    "tour", "trip", "visit", "guide", "guided", "ticket", "entry", "skip", "line",
    "day", "half", "full", "hour", "hours", "min", "mins",
    "private", "group", "walking", "boat", "bus", "train",
    "museum", "park", "palace", "castle", "cathedral", "church", "basilica", "tower",
    "center", "centre", "city", "town", "village", "street", "square", "plaza",
    "food", "wine", "tasting", "lunch", "dinner", "breakfast", "meal", "drink",
    "show", "concert", "performance", "experience", "adventure", "activity",
    "transfer", "transport", "pickup", "dropoff", "airport", "port", "station",
    "hotel", "accommodation", "stay", "night", "nights",
    "o", "a", "os", "as", "um", "uma", "uns", "umas",
    "de", "do", "da", "dos", "das", "em", "no", "na", "nos", "nas",
    "por", "pelo", "pela", "pelos", "pelas", "para", "com", "e", "ou", "que",
    "entrance", "admission", "access", "priority", "fast", "track"
}

def main():
    spark = SparkSession.builder \
        .appName("WikiCrawlerJoiner_Improved") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Load Target Cities (for backfilling) ---
    try:
        with open(TARGET_CITIES_FILE, "r", encoding="utf-8") as f:
            target_cities = json.load(f)
        print(f"Loaded {len(target_cities)} target cities for backfilling.")
    except FileNotFoundError:
        print(f"Error: {TARGET_CITIES_FILE} not found. Run generate_city_targets.py first.")
        target_cities = []

    # Broadcast the list of cities for efficient lookup in UDF
    broadcast_cities = spark.sparkContext.broadcast(target_cities)

    # --- 2. Load and Preprocess Crawler Data ---
    crawler_df = spark.read.json(CRAWLER_FILE)

    # Normalize text helper
    def normalize_text(col_expr):
        return regexp_replace(lower(col_expr), r"[^\p{L}\p{Nd}]+", "")

    # UDF to backfill city from name
    def extract_city_from_name(name, existing_city):
        if existing_city and existing_city.strip():
            return existing_city.lower()
        
        if not name:
            return None
            
        name_lower = name.lower()
        # Check against the broadcasted list of top cities
        for city in broadcast_cities.value:
            # Simple check: is the city name in the activity name?
            # We use word boundary check to avoid partial matches (e.g. "bar" in "barcelona")
            if re.search(r'\b' + re.escape(city) + r'\b', name_lower):
                return city
        return None

    extract_city_udf = udf(extract_city_from_name, StringType())

    # Apply backfilling and normalization
    crawler_prep = crawler_df.withColumn(
        "city_backfilled",
        extract_city_udf(col("name"), col("city"))
    ).withColumn(
        "city_norm",
        normalize_text(col("city_backfilled"))
    ).filter(
        col("city_norm").isNotNull() & (col("city_norm") != "")
    )

    # Tokenize crawler name (excluding city name and stop words)
    # We use a UDF for cleaner token filtering logic
    def tokenize_and_filter(text, city_norm):
        if not text:
            return []
        # Lowercase and split by non-alphanumeric
        tokens = re.split(r"[^\w]+", text.lower())
        # Filter:
        # 1. Not in STOP_WORDS
        # 2. Not the city name itself (to avoid matching "Lisbon Tour" with "Lisbon" just on "Lisbon")
        # 3. Length > 1
        return [
            t for t in tokens 
            if t not in STOP_WORDS 
            and t != city_norm 
            and len(t) > 1
        ]

    tokenize_udf = udf(tokenize_and_filter, ArrayType(StringType()))

    crawler_tok = crawler_prep.withColumn(
        "tokens_c",
        tokenize_udf(col("name"), col("city_norm"))
    )

    # --- 3. Load and Preprocess Wikipedia Data ---
    wiki_landmarks_df = spark.read.json(WIKI_FILE)
    
    # Load extracted cities
    try:
        wiki_cities_df = spark.read.json(WIKI_CITIES_FILE)
        # Union landmarks and cities
        # Ensure schema compatibility (fill missing columns if any)
        # For simplicity, we select common columns if schemas differ, but here they should be similar enough
        # or we just rely on Spark's unionByName with allowMissingColumns=True (Spark 3.1+)
        wiki_df = wiki_landmarks_df.unionByName(wiki_cities_df, allowMissingColumns=True)
        print(f"Merged {wiki_landmarks_df.count()} landmarks with {wiki_cities_df.count()} cities.")
    except Exception as e:
        print(f"Warning: Could not load {WIKI_CITIES_FILE} ({e}). Using only landmarks.")
        wiki_df = wiki_landmarks_df

    # Extract city from geo_parent (first element)
    wiki_prep = wiki_df.withColumn(
        "city_norm",
        normalize_text(
            expr("CASE WHEN geo_parent IS NOT NULL AND size(geo_parent) > 0 THEN geo_parent[0] ELSE '' END")
        )
    ).withColumn(
        "title_norm",
        normalize_text(col("title"))
    ).withColumn(
        "geo_parent_tokens",
        expr("transform(geo_parent, x -> lower(regexp_replace(x, '[^a-zA-Z0-9]', '')))")
    )

    # Tokenize wiki title (using same logic as crawler, but city filtering is less critical here, 
    # though consistent filtering helps)
    wiki_tok = wiki_prep.withColumn(
        "tokens_w",
        tokenize_udf(col("title"), col("city_norm"))
    )

    # --- 4. Join Logic ---
    
    # Condition 1: City must match (normalized)
    city_match = (col("c.city_norm") == col("w.city_norm"))

    # Condition 2a: Landmark Match (Token Overlap)
    # Overlap between crawler name tokens and wiki title tokens
    token_overlap = size(array_intersect(col("c.tokens_c"), col("w.tokens_w"))) > 0

    # Condition 2b: Landmark Match (Geo Parent Overlap)
    # Overlap between crawler name tokens and wiki geo_parent tokens (e.g. "Belem" in name matches "Belem" in geo_parent)
    geo_overlap = size(array_intersect(col("c.tokens_c"), col("w.geo_parent_tokens"))) > 0

    # Condition 3: City Page Match
    # If the crawler city matches the wiki title exactly (e.g. city="barcelona", wiki_title="Barcelona")
    # This captures general "City Tours" that don't match a specific landmark.
    city_page_match = (col("c.city_norm") == col("w.title_norm"))

    # Combined Join Condition
    # We prioritize Landmark matches, but allow City Page matches if no landmark is found.
    # Since this is a join, we'll get all valid pairs. We can flag the type of match in the output.
    join_condition = city_match & (token_overlap | geo_overlap | city_page_match)

    joined_df = crawler_tok.alias("c").join(
        wiki_tok.alias("w"),
        join_condition,
        "inner"
    )

    # Determine Match Type for analysis
    final_df = joined_df.withColumn(
        "match_type",
        when(token_overlap | geo_overlap, lit("landmark"))
        .otherwise(lit("city_page"))
    ).withColumn(
        "overlap_score",
        size(array_intersect(col("c.tokens_c"), col("w.tokens_w")))
    ).select(
        col("c.url").alias("crawler_url"),
        col("c.name").alias("crawler_name"),
        col("c.city_backfilled").alias("city"),
        col("w.title").alias("wiki_title"),
        col("w.url").alias("wiki_url"),
        col("w.type").alias("wiki_type"),
        col("overlap_score"),
        col("match_type"),
        col("c.tokens_c").alias("tokens_c_debug"),
        col("c.city_norm").alias("city_norm_debug")
    )

    # --- 5. Output and Statistics ---
    
    # Cache for counting
    final_df.cache()
    
    total_matches = final_df.count()
    landmark_matches = final_df.filter(col("match_type") == "landmark").count()
    city_page_matches = final_df.filter(col("match_type") == "city_page").count()
    
    print(f"\n--- Join Statistics ---")
    print(f"Total Joined Records: {total_matches}")
    "private", "group", "walking", "boat", "bus", "train",
    "museum", "park", "palace", "castle", "cathedral", "church", "basilica", "tower",
    "center", "centre", "city", "town", "village", "street", "square", "plaza",
    "food", "wine", "tasting", "lunch", "dinner", "breakfast", "meal", "drink",
    "show", "concert", "performance", "experience", "adventure", "activity",
    "transfer", "transport", "pickup", "dropoff", "airport", "port", "station",
    "hotel", "accommodation", "stay", "night", "nights",
    "o", "a", "os", "as", "um", "uma", "uns", "umas",
    "de", "do", "da", "dos", "das", "em", "no", "na", "nos", "nas",
    "por", "pelo", "pela", "pelos", "pelas", "para", "com", "e", "ou", "que",
    "entrance", "admission", "access", "priority", "fast", "track"
}

def main():
    spark = SparkSession.builder \
        .appName("WikiCrawlerJoiner_Improved") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Load Target Cities (for backfilling) ---
    try:
        with open(TARGET_CITIES_FILE, "r", encoding="utf-8") as f:
            target_cities = json.load(f)
        print(f"Loaded {len(target_cities)} target cities for backfilling.")
    except FileNotFoundError:
        print(f"Error: {TARGET_CITIES_FILE} not found. Run generate_city_targets.py first.")
        target_cities = []

    # Broadcast the list of cities for efficient lookup in UDF
    broadcast_cities = spark.sparkContext.broadcast(target_cities)

    # --- 2. Load and Preprocess Crawler Data ---
    crawler_df = spark.read.json(CRAWLER_FILE)

    # Normalize text helper
    def normalize_text(col_expr):
        return regexp_replace(lower(col_expr), r"[^\p{L}\p{Nd}]+", "")

    # UDF to backfill city from name
    def extract_city_from_name(name, existing_city):
        if existing_city and existing_city.strip():
            return existing_city.lower()
        
        if not name:
            return None
            
        name_lower = name.lower()
        # Check against the broadcasted list of top cities
        for city in broadcast_cities.value:
            # Simple check: is the city name in the activity name?
            # We use word boundary check to avoid partial matches (e.g. "bar" in "barcelona")
            if re.search(r'\b' + re.escape(city) + r'\b', name_lower):
                return city
        return None

    extract_city_udf = udf(extract_city_from_name, StringType())

    # Apply backfilling and normalization
    crawler_prep = crawler_df.withColumn(
        "city_backfilled",
        extract_city_udf(col("name"), col("city"))
    ).withColumn(
        "city_norm",
        normalize_text(col("city_backfilled"))
    ).filter(
        col("city_norm").isNotNull() & (col("city_norm") != "")
    )

    # Tokenize crawler name (excluding city name and stop words)
    # We use a UDF for cleaner token filtering logic
    def tokenize_and_filter(text, city_norm):
        if not text:
            return []
        # Lowercase and split by non-alphanumeric
        tokens = re.split(r"[^\w]+", text.lower())
        # Filter:
        # 1. Not in STOP_WORDS
        # 2. Not the city name itself (to avoid matching "Lisbon Tour" with "Lisbon" just on "Lisbon")
        # 3. Length > 1
        return [
            t for t in tokens 
            if t not in STOP_WORDS 
            and t != city_norm 
            and len(t) > 1
        ]

    tokenize_udf = udf(tokenize_and_filter, ArrayType(StringType()))

    crawler_tok = crawler_prep.withColumn(
        "tokens_c",
        tokenize_udf(col("name"), col("city_norm"))
    )

    # --- 3. Load and Preprocess Wikipedia Data ---
    wiki_landmarks_df = spark.read.json(WIKI_FILE)
    
    # Load extracted cities
    try:
        wiki_cities_df = spark.read.json(WIKI_CITIES_FILE)
        # Union landmarks and cities
        # Ensure schema compatibility (fill missing columns if any)
        # For simplicity, we select common columns if schemas differ, but here they should be similar enough
        # or we just rely on Spark's unionByName with allowMissingColumns=True (Spark 3.1+)
        wiki_df = wiki_landmarks_df.unionByName(wiki_cities_df, allowMissingColumns=True)
        print(f"Merged {wiki_landmarks_df.count()} landmarks with {wiki_cities_df.count()} cities.")
    except Exception as e:
        print(f"Warning: Could not load {WIKI_CITIES_FILE} ({e}). Using only landmarks.")
        wiki_df = wiki_landmarks_df

    # Extract city from geo_parent (first element)
    wiki_prep = wiki_df.withColumn(
        "city_norm",
        normalize_text(
            expr("CASE WHEN geo_parent IS NOT NULL AND size(geo_parent) > 0 THEN geo_parent[0] ELSE '' END")
        )
    ).withColumn(
        "title_norm",
        normalize_text(col("title"))
    ).withColumn(
        "geo_parent_tokens",
        expr("transform(geo_parent, x -> lower(regexp_replace(x, '[^a-zA-Z0-9]', '')))")
    )

    # Tokenize wiki title (using same logic as crawler, but city filtering is less critical here, 
    # though consistent filtering helps)
    wiki_tok = wiki_prep.withColumn(
        "tokens_w",
        tokenize_udf(col("title"), col("city_norm"))
    )

    # --- 4. Join Logic ---
    
    # Condition 1: City must match (normalized)
    city_match = (col("c.city_norm") == col("w.city_norm"))

    # Condition 2a: Landmark Match (Token Overlap)
    # Overlap between crawler name tokens and wiki title tokens
    token_overlap = size(array_intersect(col("c.tokens_c"), col("w.tokens_w"))) > 0

    # Condition 2b: Landmark Match (Geo Parent Overlap)
    # Overlap between crawler name tokens and wiki geo_parent tokens (e.g. "Belem" in name matches "Belem" in geo_parent)
    geo_overlap = size(array_intersect(col("c.tokens_c"), col("w.geo_parent_tokens"))) > 0

    # Condition 3: City Page Match
    # If the crawler city matches the wiki title exactly (e.g. city="barcelona", wiki_title="Barcelona")
    # This captures general "City Tours" that don't match a specific landmark.
    city_page_match = (col("c.city_norm") == col("w.title_norm"))

    # Combined Join Condition
    # We prioritize Landmark matches, but allow City Page matches if no landmark is found.
    # Since this is a join, we'll get all valid pairs. We can flag the type of match in the output.
    join_condition = city_match & (token_overlap | geo_overlap | city_page_match)

    joined_df = crawler_tok.alias("c").join(
        wiki_tok.alias("w"),
        join_condition,
        "inner"
    )

    # Determine Match Type for analysis
    final_df = joined_df.withColumn(
        "match_type",
        when(token_overlap | geo_overlap, lit("landmark"))
        .otherwise(lit("city_page"))
    ).withColumn(
        "overlap_score",
        size(array_intersect(col("c.tokens_c"), col("w.tokens_w")))
    ).select(
        col("c.url").alias("crawler_url"),
        col("c.name").alias("crawler_name"),
        col("c.city_backfilled").alias("city"),
        col("w.title").alias("wiki_title"),
        col("w.url").alias("wiki_url"),
        col("w.type").alias("wiki_type"),
        col("overlap_score"),
        col("match_type"),
        col("c.tokens_c").alias("tokens_c_debug"),
        col("c.city_norm").alias("city_norm_debug")
    )

    # --- 5. Output and Statistics ---
    
    # Cache for counting
    final_df.cache()
    
    total_matches = final_df.count()
    landmark_matches = final_df.filter(col("match_type") == "landmark").count()
    city_page_matches = final_df.filter(col("match_type") == "city_page").count()
    
    print(f"\n--- Join Statistics ---")
    print(f"Total Joined Records: {total_matches}")
    print(f"Landmark Matches: {landmark_matches}")
    print(f"City Page Matches: {city_page_matches}")
    print(f"-----------------------\n")

    # Write output
    # Collect results to driver (safe for small datasets like 1500 records)
    results = final_df.collect()
    
    print(f"Collected {len(results)} records. Writing to local file...")
    
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with open(f"{OUTPUT_DIR}/full_join_improved.jsonl", "w", encoding="utf-8") as f:
        for row in results:
            f.write(json.dumps(row.asDict(), ensure_ascii=False) + "\n")
            
    print(f"Successfully wrote to {OUTPUT_DIR}/full_join_improved.jsonl")

    # Write compact version
    with open(f"{OUTPUT_DIR}/compact_join_improved.jsonl", "w", encoding="utf-8") as f:
        for row in results[:2000]:
            f.write(json.dumps(row.asDict(), ensure_ascii=False) + "\n")
    print(f"Successfully wrote to {OUTPUT_DIR}/compact_join_improved.jsonl")

    spark.stop()

if __name__ == "__main__":
    main()