#!/usr/bin/env python3
"""
spark_wiki_extractor.py
-----------------------
Spustí paralelné spracovanie Wikipedia dumpu pomocou PySpark.
Každý XML chunk sa spracuje nezávisle a uloží ako JSONL.
Ak výstup pre chunk existuje, preskočí sa (resume system).
"""

from pyspark.sql import SparkSession
from pathlib import Path
import json
from lxml import etree
from io import BytesIO
from wiki_extractor import extract_page

INPUT_DIR = Path("wiki_chunks")
OUTPUT_DIR = Path("wiki_output_new")

def process_chunk(chunk_path: str):
    """Parses one XML chunk and returns list of JSON strings."""
    out = []
    with open(chunk_path, "rb") as f:
        for _, elem in etree.iterparse(f, events=("end",), tag="{*}page"):
            title = elem.findtext(".//{*}title")
            text = elem.findtext(".//{*}revision/{*}text")
            if not title or not text:
                elem.clear()
                continue
            record = extract_page(title, text)
            if record:
                out.append(json.dumps(record, ensure_ascii=False))
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    return out


def main():
    spark = SparkSession.builder \
        .appName("Wikipedia Landmark Extractor") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    sc = spark.sparkContext

    OUTPUT_DIR.mkdir(exist_ok=True)
    all_chunks = sorted(INPUT_DIR.glob("wiki_chunk_*.xml"))
    unprocessed = [c for c in all_chunks if not (OUTPUT_DIR / f"{c.stem}.jsonl").exists()]
    if not unprocessed:
        print("All chunks already processed.")
        spark.stop()
        return

    print(f"Processing {len(unprocessed)} chunks ({len(all_chunks)-len(unprocessed)} already done).")

    rdd = sc.parallelize([str(c) for c in unprocessed], numSlices=len(unprocessed))

    def parse_and_save(chunk_path):
        import os, json
        from lxml import etree
        from wiki_extractor import extract_page
        from pathlib import Path

        OUTPUT = Path("wiki_output_new")
        OUTPUT.mkdir(exist_ok=True)

        out_file = OUTPUT / (Path(chunk_path).stem + ".jsonl")
        data = []
        with open(chunk_path, "rb") as f:
            for _, elem in etree.iterparse(f, events=("end",), tag="{*}page"):
                title = elem.findtext(".//{*}title")
                text = elem.findtext(".//{*}revision/{*}text")
                if not title or not text:
                    elem.clear()
                    continue
                record = extract_page(title, text)
                if record:
                    data.append(json.dumps(record, ensure_ascii=False))
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
        if data:
            out_file.write_text("\n".join(data), encoding="utf-8")
        return f"done {out_file.name} ({len(data)} records)"

    results = rdd.map(parse_and_save).collect()
    for r in results:
        print(r)

    print("Done – all available chunks processed.")
    spark.stop()


if __name__ == "__main__":
    main()
