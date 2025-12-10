#!/usr/bin/env python3
"""
spark_city_extractor.py
-----------------------
Runs the GENERAL city extraction process in parallel using Spark.
Extracts ALL pages with city-like infoboxes from wiki_chunks.
"""

from pyspark.sql import SparkSession
from pathlib import Path
import os

# Configuration
INPUT_DIR = Path("wiki_chunks")
OUTPUT_DIR = Path("wiki_output_cities_broad_v3")

def main():
    spark = SparkSession.builder \
        .appName("Wikipedia General City Extractor") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    sc = spark.sparkContext

    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Get list of chunks
    all_chunks = sorted(INPUT_DIR.glob("wiki_chunk_*.xml"))
    unprocessed = [c for c in all_chunks if not (OUTPUT_DIR / f"{c.stem}.jsonl").exists()]
    
    if not unprocessed:
        print("All chunks already processed.")
        spark.stop()
        return

    print(f"Processing {len(unprocessed)} chunks ({len(all_chunks)-len(unprocessed)} already done).")

    rdd = sc.parallelize([str(c) for c in unprocessed], numSlices=len(unprocessed))

    def parse_and_save(chunk_path):
        from wiki_city_extractor import process_chunk_for_cities
        from pathlib import Path
        
        OUTPUT = Path("wiki_output_cities_broad_v3")
        OUTPUT.mkdir(exist_ok=True)

        out_file = OUTPUT / (Path(chunk_path).stem + ".jsonl")
        
        # Process the chunk (no targets needed)
        data = process_chunk_for_cities(chunk_path)
        
        if data:
            out_file.write_text("\n".join(data), encoding="utf-8")
            return f"done {out_file.name} ({len(data)} records)"
        else:
            # Create empty file to mark as processed
            out_file.touch()
            return f"done {out_file.name} (0 records)"

    results = rdd.map(parse_and_save).collect()
    for r in results:
        print(r)

    print("Done – all available chunks processed.")
    spark.stop()

if __name__ == "__main__":
    main()
