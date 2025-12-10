#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import lucene
try:
    lucene.initVM(vmargs=["-Djava.awt.headless=true"])
except ValueError:
    pass # VM already initialized

from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer

from org.apache.lucene.search import (
    IndexSearcher,
    BooleanQuery,
    BooleanClause,
    TermQuery,
    FuzzyQuery,
    BoostQuery,
    PhraseQuery
)

# ANSI Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"

INDEX_DIR = "index_v2"


# =========================
# SYNONYM DICTIONARY
# =========================
SYNONYM_MAP = {
    "ticket": ["entry", "admission", "pass", "access"],
    "bus": ["shuttle", "transport", "coach", "hop-on"],
    "tour": ["trip", "excursion", "visit", "guide"],
    "show": ["performance", "concert", "dance", "event"],
    "walking": ["foot", "stroll", "pedestrian"],
    "museum": ["gallery", "exhibition"],
    "guide": ["host", "instructor", "leader"]
}

# =========================
# BUILD QUERY (Boolean AND / OR)
# =========================
def build_query(query_str, force_fuzzy=False):
    # Check for explicit "OR" operator
    is_or_query = " or " in query_str.lower()
    
    # Clean tokens: remove "or" if present
    tokens = [t.lower() for t in query_str.split() if t.strip() and t.lower() != "or"]
    
    # Determine explicit operator
    occur_operator = BooleanClause.Occur.SHOULD if is_or_query else BooleanClause.Occur.MUST

    # Top Level Query
    top_builder = BooleanQuery.Builder()

    for tok in tokens:
        # Per-token Builder
        token_builder = BooleanQuery.Builder()
        
        # === A. STANDARD MATCHING ===
        if not force_fuzzy:
            # 1. Title (Highest)
            token_builder.add(BoostQuery(TermQuery(Term("title", tok)), 5.0), BooleanClause.Occur.SHOULD)
            # 2. Click Text
            token_builder.add(BoostQuery(TermQuery(Term("click_text", tok)), 3.0), BooleanClause.Occur.SHOULD)
            # 3. Wiki Text
            token_builder.add(BoostQuery(TermQuery(Term("wiki_text", tok)), 1.5), BooleanClause.Occur.SHOULD)
            # 4. City
            token_builder.add(BoostQuery(TermQuery(Term("city", tok)), 8.0), BooleanClause.Occur.SHOULD)
            
            # 5. Weak Fuzzy (Fallback for minor typos)
            if len(tok) >= 4:
                token_builder.add(FuzzyQuery(Term("title", tok), 1), BooleanClause.Occur.SHOULD)
        
        # === B. FUZZY MATCHING (Spell Check Mode) ===
        else:
            # Stronger Fuzzy (Edit Distance 2) on Title & Click Text
            token_builder.add(FuzzyQuery(Term("title", tok), 2), BooleanClause.Occur.SHOULD)
            token_builder.add(FuzzyQuery(Term("click_text", tok), 2), BooleanClause.Occur.SHOULD)


        # === C. SYNONYM EXPANSION ===
        # Add Synonyms as SHOULD clauses (Boost 0.8 - slightly less than exact match)
        if tok in SYNONYM_MAP:
            for syn in SYNONYM_MAP[tok]:
                # Standard TermQuery for synonym
                token_builder.add(BoostQuery(TermQuery(Term("click_text", syn)), 2.5), BooleanClause.Occur.SHOULD)
                token_builder.add(BoostQuery(TermQuery(Term("wiki_text", syn)), 1.2), BooleanClause.Occur.SHOULD)


        # Add the per-token disjunction to top query
        top_builder.add(token_builder.build(), occur_operator)

    # === PHRASE BOOST (Only in Standard Mode) ===
    if len(tokens) >= 2 and not force_fuzzy:
        pq = PhraseQuery.Builder()
        for tok in tokens:
            pq.add(Term("title", tok)) 
        pq.setSlop(10)
        top_builder.add(BoostQuery(pq.build(), 4.0), BooleanClause.Occur.SHOULD)

    return top_builder.build()


# =========================
# SEARCH + PRINT
# =========================
def search(query_str):
    if not query_str: return

    directory = NIOFSDirectory(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)

    # 1. Try Standard Search (Exact + Synonyms)
    lucene_query = build_query(query_str, force_fuzzy=False)
    hits = searcher.search(lucene_query, 50).scoreDocs

    # 2. If NO Results -> Try Fuzzy Search (Spell Check)
    if len(hits) == 0:
        print(f"{RED}No exact matches found. Trying 'Did you mean?' (Fuzzy Search)...{RESET}")
        lucene_query = build_query(query_str, force_fuzzy=True)
        hits = searcher.search(lucene_query, 50).scoreDocs
        if len(hits) > 0:
            print(f"{YELLOW}Showing results for slightly different terms:{RESET}")

    print(f"\n{BOLD}Found {len(hits)} candidates. Re-ranking top 10 based on Popularity (Reviews/Rating)...{RESET}\n")

    scored_results = []

    for hit in hits:
        doc = searcher.storedFields().document(hit.doc)

        # Base text score
        original_score = hit.score

        # Popularity Signals
        rating = 0.0
        r_field = doc.get("rating")
        if r_field:
            try: rating = float(r_field)
            except: pass
        
        reviews = 0
        rv_field = doc.get("reviewCount")
        if rv_field:
            try: reviews = int(rv_field)
            except: pass

        # === CUSTOM SCORING FORMULA ===
        review_boost = math.log10(reviews + 1) if reviews > 0 else 0
        rating_factor = (rating / 5.0) if rating > 0 else 0.5
        final_score = original_score * (1.0 + review_boost) * rating_factor
        
        scored_results.append({
            "doc": doc,
            "original_score": original_score,
            "final_score": final_score,
            "rating": rating,
            "reviews": reviews,
            "city": doc.get("city") or "N/A",
            "title": doc.get("title") or "Unknown",
            "url": doc.get("url") or "#",
            "architect": doc.get("architect"),
            "style": doc.get("style")
        })

    # SORT BY FINAL SCORE DESCENDING
    scored_results.sort(key=lambda x: x["final_score"], reverse=True)

    # PRINT TOP 10
    for i, res in enumerate(scored_results[:10]):
        title = res["title"]
        city = res["city"]
        url = res["url"]
        rating = res["rating"]
        reviews = res["reviews"]
        
        score_bar = "*" * int(res["final_score"] / 2) 
        
        print(f"{CYAN}{i+1}. {title}{RESET} {YELLOW}({city.title()}){RESET}")
        print(f"   {BOLD}Final Score:{RESET} {res['final_score']:.2f} (Text: {res['original_score']:.2f}) {score_bar}")
        
        # Social Proof Display
        stars = "⭐" * int(round(rating)) if rating > 0 else "N/A"
        print(f"   {BOLD}Social Proof:{RESET} {stars} {rating}/5.0 ({reviews} reviews)")
            
        if res["architect"] or res["style"]:
            extras = []
            if res["architect"]: extras.append(f"Architect: {res['architect']}")
            if res["style"]: extras.append(f"Style: {res['style']}")
            print(f"   {GREEN}Enriched Metadata: {', '.join(extras)}{RESET}")
            
        print(f"   {BOLD}URL:{RESET} {url}")
        print(f"{YELLOW}{'-'*40}{RESET}")

    reader.close()
    directory.close()


# =========================
# MAIN LOOP
# =========================
if __name__ == "__main__":
    print(f"{GREEN}Welcome to the Improved Lucene Search Engine!{RESET}")
    print(f"{YELLOW}Type your query. Supports OR logic, Synonyms (ticket=entry), and Spell Check.{RESET}")
    
    while True:
        try:
            q = input(f"\n{BOLD}{CYAN}Query > {RESET}").strip()
        except EOFError:
            break

        if not q:
            print("Exiting...")
            break

        try:
            search(q)
        except Exception as e:
            print(f"{RED}Error executing search: {e}{RESET}")
