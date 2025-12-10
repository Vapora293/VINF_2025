import re
import os
import json
import time
import random
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


DEBUGGER = "127.0.0.1:9221"
BASE_URL = "https://www.getyourguide.com/"
OUT_DIR = "data_pages_3"

os.makedirs(OUT_DIR, exist_ok=True)

# Skip cities already processed
SKIP_CITIES = {
    "barcelona", "london", "madrid", "paris", "moulin_rouge",
    "colosseum", "ha_long_bay", "memorial_and_museum_auschwitz-birkenau",
    "notre_dame_cathedral", "sagrada_familia", "seine_river",
    "top_of_the_rock,_branson", "vatican_museums"
}


def attach_driver():
    opts = Options()
    opts.add_experimental_option("debuggerAddress", DEBUGGER)
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


def wait_css(driver, css, timeout=20):
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))


def abs_url(href):
    if not href:
        return None
    return href if href.startswith("http") else "https://www.getyourguide.com" + href


# ---------- STEP 1: COLLECT CITIES ----------
def get_top_destination_links(driver):
    print("Navigating to GetYourGuide homepage...")
    driver.get(BASE_URL)
    wait_css(driver, "body")
    time.sleep(2)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    try:
        tab = driver.find_element(By.XPATH, "//span[contains(., 'Top destinations')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", tab)
        time.sleep(0.5)
        tab.click()
        print("Clicked 'Top destinations' tab.")
        time.sleep(2)
    except Exception:
        print(" 'Top destinations' tab click not needed or missing, continuing...")

    html = driver.page_source

    # Extract only between “Top destinations” and “Top countries to visit”
    section = re.search(r"Top destinations(.*?)Top countries to visit", html, flags=re.S | re.I)
    if not section:
        print(" Could not find 'Top destinations' section.")
        return []

    section_html = section.group(1)

    city_links = re.findall(
        r'<a[^>]+href="(/[^"]+-l\d+/)"[^>]*>.*?<div[^>]*class="title"[^>]*>(.*?)</div>',
        section_html, flags=re.S | re.I
    )

    cities = []
    for href, name in city_links:
        full_url = abs_url(href)
        city_name = re.sub(r"<[^>]+>", "", name).strip().lower().replace(" ", "_")
        if city_name not in SKIP_CITIES:
            cities.append((city_name, full_url))

    print(f" Found {len(cities)} cities to crawl (skipped {len(SKIP_CITIES)}).")
    return cities


# ---------- STEP 2: PARSE CITY LIST ----------
def parse_listing_links(html):
    html = html.replace("\n", " ").replace("<!--[-->", "").replace("<!--]-->", "")
    cards = []

    blocks = re.findall(
        r'(<a[^>]*data-test-id="vertical-activity-card-link"[^>]*>.*?</a>)',
        html, flags=re.S | re.I
    )
    if not blocks:
        blocks = re.findall(
            r'(<a[^>]*class="[^"]*vertical-activity-card__container[^"]*"[^>]*>.*?</a>)',
            html, flags=re.S | re.I
        )

    for b in blocks:
        href_m = re.search(r'href="([^"]+)"', b, flags=re.I)
        href = abs_url(href_m.group(1)) if href_m else None

        title_m = re.search(r"<h3[^>]*>(.*?)</h3>", b, flags=re.S | re.I)
        title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else None

        rating_m = re.search(r"c-activity-rating__rating[^>]*>([\d.,]+)</", b, flags=re.S | re.I)
        rating = rating_m.group(1) if rating_m else None

        reviews_m = re.search(r"c-activity-rating__label[^>]*>\(?([\d,]+)\)?", b, flags=re.S | re.I)
        reviews = reviews_m.group(1).replace(",", "") if reviews_m else None

        price_block = re.search(r'(<div[^>]*class="[^"]*activity-price[^"]*"[^>]*>.*?</div>)', b, flags=re.S | re.I)
        price_text = None
        if price_block:
            block = price_block.group(1)
            price_parts = re.findall(
                r'>([^<>]*?(?:€|\$)\s*\d+[^\d<>]*)<|>(From|per person|per group up to \d+)<',
                block, flags=re.S | re.I
            )
            price_text = " ".join(p[0] or p[1] for p in price_parts if (p[0] or p[1])).strip()

        cards.append({
            "url": href,
            "title": title,
            "rating": rating,
            "reviews": reviews,
            "price": price_text
        })

    return cards


# ---------- STEP 3: DETAILS EXTRACTOR ----------
def extract_data_from_html(html, url, base_info=None):
    html = html.replace("\n", " ")
    out = base_info.copy() if base_info else {}
    out["url"] = url

    title = re.search(r"<h1[^>]*>(.*?)</h1>", html)
    out["title"] = re.sub(r"<[^>]+>", "", title.group(1)).strip() if title else out.get("title")

    # JSON-LD
    ld_blocks = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html)
    for block in ld_blocks:
        if '"@type"' not in block:
            continue
        if not re.search(r'"@type"\s*:\s*"(Product|TouristAttraction)"', block):
            continue
        name = re.search(r'"name"\s*:\s*"([^"]+)"', block)
        desc = re.search(r'"description"\s*:\s*"([^"]+)"', block)
        rating = re.search(r'"ratingValue"\s*:\s*([0-9.]+)', block)
        reviews = re.search(r'"reviewCount"\s*:\s*([0-9]+)', block)
        price = re.search(r'"price"\s*:\s*"([^"]+)"', block)
        currency = re.search(r'"priceCurrency"\s*:\s*"([^"]+)"', block)

        out.update({
            "ld_type": re.search(r'"@type"\s*:\s*"([^"]+)"', block).group(1),
            "name": name.group(1) if name else out.get("name"),
            "description": desc.group(1) if desc else out.get("description"),
            "rating": float(rating.group(1)) if rating else out.get("rating"),
            "reviewCount": int(reviews.group(1)) if reviews else out.get("reviewCount"),
            "price": price.group(1) if price else out.get("price"),
            "currency": currency.group(1) if currency else out.get("currency"),
        })
        break

    # Highlights
    highlights_section = re.search(r'<div[^>]*id="highlights-point"[^>]*>.*?<ul[^>]*>(.*?)</ul>', html, flags=re.S | re.I)
    highlights = []
    if highlights_section:
        items = re.findall(r"<li[^>]*>(.*?)</li>", highlights_section.group(1), flags=re.S | re.I)
        highlights = [re.sub(r"<[^>]+>", "", i).strip() for i in items if i.strip()]
    out["highlights"] = highlights if highlights else None

    out["featured_reviews"] = extract_featured_reviews(html)
    return out


def extract_featured_reviews(html):
    html = html.replace("\n", " ").replace("<!--[-->", "").replace("<!--]-->", "")
    review_texts = re.findall(
        r'<p[^>]*class="review-highlight-card__text--v2\s+review-highlight-card__text"[^>]*>.*?<span[^>]*>(.*?)</span>',
        html, flags=re.S | re.I
    )
    reviews = []
    for r in review_texts[:2]:
        clean = re.sub(r"<[^>]+>", "", r).strip()
        if clean:
            reviews.append(clean)
    return reviews


# ---------- STEP 4: PER CITY ----------
def crawl_city(driver, city_name, city_url):
    print(f"\n Crawling city: {city_name} -> {city_url}")
    CITY = city_name.lower().replace(" ", "_")
    city_dir = os.path.join(OUT_DIR, CITY)
    os.makedirs(city_dir, exist_ok=True)
    detail_file = os.path.join(city_dir, f"{CITY}_details.jsonl")

    driver.get(city_url)
    wait_css(driver, "body", 20)
    time.sleep(2)

    while True:
        html = driver.page_source
        if re.search(r'id="no-results-block"|No results found', html, re.IGNORECASE):
            break
        try:
            show_more = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(text(),'Show more')]]"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", show_more)
            driver.execute_script("arguments[0].click();", show_more)
            time.sleep(1.5 + random.random())
        except Exception:
            break

    html = driver.page_source
    cards = parse_listing_links(html)
    print(f"Found {len(cards)} cards in {city_name}")

    with open(detail_file, "w", encoding="utf-8") as sink:
        for i, card in enumerate(cards):
            link = card["url"]
            print(f"  [{i+1}/{len(cards)}] Visiting {link}")
            driver.get(link)
            try:
                wait_css(driver, "h1", 20)
            except Exception:
                pass
            time.sleep(1.5 + random.random())

            html = driver.page_source
            html_path = os.path.join(city_dir, f"{CITY}_{i}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)

            record = extract_data_from_html(html, link, base_info=card)
            sink.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f" Finished {city_name} — saved {len(cards)} records and HTMLs.")


# ---------- MAIN ----------
def main():
    driver = attach_driver()
    cities = get_top_destination_links(driver)
    for name, url in cities:
        crawl_city(driver, name, url)
    driver.quit()
    print(" Finished all cities.")


if __name__ == "__main__":
    main()
