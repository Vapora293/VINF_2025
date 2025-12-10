#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import lucene

from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory

from org.apache.lucene.document import (
    Document, TextField, StringField, StoredField, FloatPoint, Field,
    NumericDocValuesField, FloatDocValuesField
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.analysis import Analyzer
# Unused imports removed


INDEX_DIR = "index_v2"
INPUT_PATH = "full_join.jsonl"


# ----------------------------------------------------------------------
# SAFE JOIN
# ----------------------------------------------------------------------
def safe_join(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return " ".join(safe_join(x) for x in v)
    if isinstance(v, dict):
        return " ".join(safe_join(x) for x in v.values())
    return str(v)

# ----------------------------------------------------------------------
# HELPER FOR ENRICHED FIELDS
# ----------------------------------------------------------------------
def get_enriched_field(data, field_name):
    """
    Extracts a specific field from the primary match or list of matches.
    """
    # Check primary match first
    pm = data.get("primary_match")
    if pm and pm.get(field_name):
        return pm.get(field_name)
    
    # Check matches list
    for m in data.get("matches", []):
        if m.get(field_name):
            return m.get(field_name)
    return ""

# ----------------------------------------------------------------------
# INDEXER
# ----------------------------------------------------------------------

def build_index():
    lucene.initVM(vmargs=["-Djava.awt.headless=true"])

    # Use StandardAnalyzer (SynonymAnalyzer failed in Docker)
    analyzer = StandardAnalyzer()

    if not os.path.exists(INDEX_DIR):
        os.mkdir(INDEX_DIR)

    directory = NIOFSDirectory(Paths.get(INDEX_DIR))
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(directory, config)

    print("➡️ Building IMPROVED Lucene index (v2) with StandardAnalyzer...")

    failed = 0

    try:
        f = open(INPUT_PATH, encoding="utf-8")
        for i, line in enumerate(f):
            try:
                data = json.loads(line)
                doc = Document()

                # TITLE
                title = (
                        data.get("title")
                        or data.get("name")
                        or ""
                )
                doc.add(TextField("title", title, Field.Store.YES))

                # ENRICHED METADATA (Architect, Style)
                architect = get_enriched_field(data, "wiki_architect")
                style = get_enriched_field(data, "wiki_style")
                
                # CLICK TEXT (Description + Reviews + Enriched Data)
                # We include architect/style here to make them searchable
                click_text = safe_join([
                    data.get("description"),
                    data.get("highlights"),
                    data.get("featured_reviews"),
                    architect,
                    style
                ])
                doc.add(TextField("click_text", click_text, Field.Store.NO))

                # WIKI TEXT (Abstracts from all matches)
                wiki_abstracts = []
                for m in data.get("matches", []):
                    if m.get("wiki_abstract"):
                        wiki_abstracts.append(m.get("wiki_abstract"))
                
                wiki_text = safe_join(wiki_abstracts)
                doc.add(TextField("wiki_text", wiki_text, Field.Store.NO))

                # CITY
                city = (data.get("city") or "").lower()
                # Also index Wiki cities if available
                if not city:
                     for m in data.get("matches", []):
                         if m.get("wiki_type") == "city":
                             city = m.get("wiki_title", "").lower()
                             break
                
                if city:
                    doc.add(StringField("city", city, Field.Store.YES))

                # URL
                url = data.get("url")
                if url:
                    doc.add(StoredField("url", url))
                    
                # STORE EXTRA FIELDS FOR DISPLAY
                if architect: doc.add(StoredField("architect", architect))
                if style: doc.add(StoredField("style", style))

                # --- RANKING SIGNALS (Rating & Reviews) ---
                # We store them for display AND add DocValues for scoring
                
                # Rating (float)
                rating_val = 0.0
                raw_rating = data.get("rating")
                if raw_rating is not None:
                    try:
                        rating_val = float(raw_rating)
                        doc.add(FloatPoint("rating", rating_val))
                        doc.add(StoredField("rating", rating_val))
                        doc.add(FloatDocValuesField("rating_dv", rating_val))
                    except:
                        pass
                
                # Review Count (int)
                reviews_val = 0
                raw_reviews = data.get("reviewCount")
                if raw_reviews is not None:
                    try:
                        # Sometimes it might be a string "3,400" or similar, though usually int in JSON
                        if isinstance(raw_reviews, str):
                            reviews_val = int(raw_reviews.replace(",", "").replace(".", ""))
                        else:
                            reviews_val = int(raw_reviews)
                        
                        doc.add(StoredField("reviewCount", reviews_val))
                        doc.add(NumericDocValuesField("reviews_dv", reviews_val))
                    except:
                        pass

                doc.add(StringField("doc_id", str(i), Field.Store.YES))

                doc.add(StringField("doc_id", str(i), Field.Store.YES))

                writer.addDocument(doc)

                if i % 5000 == 0:
                    print(f"Indexed {i} docs...")

            except Exception as e:
                failed += 1
                # print(f"❌ Failed [{i}]: {e}")

        writer.commit()
        writer.close()
        print("✔ DONE — Index v2 created.")
        print(f"⚠ Failed documents: {failed}")
        
    except FileNotFoundError:
        print(f"❌ Input file {INPUT_PATH} not found!")


if __name__ == "__main__":
    build_index()
