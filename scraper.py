"""
movement.as — Product Feed Scraper
====================================
Generates feed_google.xml (GMC) and feed_meta.xml (Meta Retail)
from movement.as product pages.

Data hierarchy:
  1. JSON-LD (BreadcrumbList, Product structured data)
  2. og:image / meta tags
  3. DOM fallbacks (breadcrumb nav, image slider, description tab)
  4. URL-path fallback for breadcrumbs

Author: Amidays
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import random
import logging
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dataclasses import dataclass, field
from typing import Optional
import gzip

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL        = "https://www.movement.as"
SITEMAP_URL     = "https://www.movement.as/sitemap.xml"
CURRENCY        = "NOK"
COUNTRY         = "NO"
SHIPPING_PRICE  = "1000.00 NOK"         # Default shipping; override per product if found
CONDITION       = "used"                 # movement.as = used/pre-owned office furniture
IDENTIFIER_EXISTS = "no"                 # Used goods → no GTIN/MPN

# Price tier thresholds (ex. VAT, NOK) for custom_label_0
PRICE_TIER_HIGH   = 10_000
PRICE_TIER_MEDIUM = 3_000

# Crawl politeness
REQUEST_DELAY_MIN = 1.0   # seconds
REQUEST_DELAY_MAX = 3.0

# ─── HTTP Session ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.6",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":  "document",
    "Sec-Fetch-Mode":  "navigate",
    "Sec-Fetch-Site":  "none",
    "Cache-Control":   "max-age=0",
})


def _get(url: str, retries: int = 3) -> Optional[requests.Response]:
    """GET with retry logic and politeness delay."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200:
                return r
            elif r.status_code in (429, 503):
                wait = 10 * (attempt + 1)
                log.warning(f"Rate limited on {url}. Waiting {wait}s …")
                time.sleep(wait)
            elif r.status_code == 403:
                log.error(f"403 Forbidden: {url}. Check headers/IP.")
                return None
            else:
                log.warning(f"HTTP {r.status_code} for {url}")
        except requests.RequestException as e:
            log.error(f"Request error ({attempt+1}/{retries}) for {url}: {e}")
    return None


# ─── Data Model ───────────────────────────────────────────────────────────────
@dataclass
class Product:
    url: str
    product_id: str             = ""
    title_raw: str              = ""     # as found on page
    title_seo: str              = ""     # Smart Title: may have category appended
    description: str            = ""
    price: str                  = ""     # "1234.00 NOK"
    price_value: float          = 0.0
    availability: str           = "in_stock"
    condition: str              = CONDITION
    identifier_exists: str      = IDENTIFIER_EXISTS
    brand: str                  = ""
    image_main: str             = ""
    images_extra: list          = field(default_factory=list)
    breadcrumbs: list           = field(default_factory=list)  # ["Bord","Møtebord"]
    product_type: str           = ""     # "Bord > Møtebord"
    google_category: str        = ""
    custom_label_0: str         = ""     # Price tier
    custom_label_1: str         = ""     # Stock detail
    custom_label_2: str         = ""     # USPs (CO2, eco, etc.)
    custom_label_3: str         = ""     # Sale / seasonal
    custom_label_4: str         = ""     # Spare / dimensions
    shipping_price: str         = SHIPPING_PRICE
    leaf_category: str          = ""     # Most specific breadcrumb node


# ─── 1. SITEMAP DISCOVERY ─────────────────────────────────────────────────────
def discover_product_urls(sitemap_url: str = SITEMAP_URL) -> list[str]:
    """
    Walk the sitemap index → child sitemaps → product URLs.
    Product pages on movement.as end with a numeric ID before .html
    e.g. /bord/motebord/motebord-air-19462.html
    """
    log.info(f"Fetching sitemap: {sitemap_url}")
    product_urls = []
    visited_sitemaps = set()

    def _parse_sitemap(url: str):
        if url in visited_sitemaps:
            return
        visited_sitemaps.add(url)
        resp = _get(url)
        if not resp:
            return
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            log.error(f"XML parse error on {url}: {e}")
            return

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        # Sitemap index → recurse
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            _parse_sitemap(loc.text.strip())
        # URL entries → filter product pages
        for loc in root.findall(".//sm:url/sm:loc", ns):
            page_url = loc.text.strip()
            # Product pages have a trailing numeric ID in the slug
            if re.search(r'-\d{4,6}\.html$', page_url):
                product_urls.append(page_url)

    _parse_sitemap(sitemap_url)
    log.info(f"Discovered {len(product_urls)} product URLs")
    return list(dict.fromkeys(product_urls))   # deduplicate, preserve order


# ─── 2. PAGE PARSING ──────────────────────────────────────────────────────────
def _extract_ld_json(soup: BeautifulSoup) -> dict:
    """
    Extract all JSON-LD blocks and merge into a single lookup dict keyed by @type.
    Returns {"Product": {...}, "BreadcrumbList": {...}, ...}
    """
    result = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    result[item.get("@type", "")] = item
            elif isinstance(data, dict):
                t = data.get("@type", "")
                if t == "@graph":
                    for item in data.get("@graph", []):
                        result[item.get("@type", "")] = item
                else:
                    result[t] = data
        except (json.JSONDecodeError, AttributeError):
            pass
    return result


def _extract_breadcrumbs(soup: BeautifulSoup, ld: dict, url: str) -> list[str]:
    """
    Priority: JSON-LD BreadcrumbList → DOM nav.breadcrumb → URL path fallback.
    Returns list of breadcrumb names, e.g. ["Hjem", "Bord", "Møtebord"]
    """
    # 1. JSON-LD
    if "BreadcrumbList" in ld:
        items = sorted(
            ld["BreadcrumbList"].get("itemListElement", []),
            key=lambda x: x.get("position", 0)
        )
        crumbs = [i.get("item", {}).get("name", i.get("name", "")) for i in items]
        crumbs = [c.strip() for c in crumbs if c.strip() and c.lower() not in ("hjem", "home")]
        if crumbs:
            return crumbs

    # 2. DOM: common breadcrumb selectors
    for selector in [
        "nav.breadcrumb ol li",
        ".breadcrumb li",
        "[itemtype*='BreadcrumbList'] [itemprop='name']",
        ".breadcrumbs span",
    ]:
        nodes = soup.select(selector)
        if nodes:
            crumbs = [n.get_text(strip=True) for n in nodes]
            crumbs = [c for c in crumbs if c and c.lower() not in ("hjem", "home", ">", "/")]
            if crumbs:
                return crumbs

    # 3. URL path fallback
    path = urlparse(url).path.strip("/")
    parts = path.split("/")[:-1]   # drop the product slug itself
    crumbs = [p.replace("-", " ").title() for p in parts if p]
    log.debug(f"Breadcrumb fallback from URL: {crumbs}")
    return crumbs


def _build_product_type(breadcrumbs: list[str]) -> str:
    """Map breadcrumb list to Google product_type string."""
    return " > ".join(breadcrumbs) if breadcrumbs else ""


def _smart_title(raw_title: str, leaf_category: str) -> str:
    """
    If the leaf category keyword is not in the title, append it.
    Case-insensitive check.
    """
    if not leaf_category:
        return raw_title
    if leaf_category.lower() in raw_title.lower():
        return raw_title
    return f"{raw_title} - {leaf_category}"


def _extract_price(soup: BeautifulSoup, ld: dict) -> tuple[float, str]:
    """
    Priority:
      1. JSON-LD Product → priceWithoutVat custom property
      2. JSON-LD Product → offers → price
      3. DOM: [data-price-without-vat], .price-ex-vat, etc.
    Returns (float_value, "1234.00 NOK")
    """
    # 1. JSON-LD Product
    if "Product" in ld:
        product_ld = ld["Product"]

        # Check for custom priceWithoutVat field
        for key in ("priceWithoutVat", "price_without_vat", "priceExVat"):
            val = product_ld.get(key)
            if val:
                try:
                    fval = float(str(val).replace(",", ".").replace(" ", ""))
                    return fval, f"{fval:.2f} {CURRENCY}"
                except ValueError:
                    pass

        # Standard offers price
        offers = product_ld.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price_str = str(offers.get("price", "")).replace(",", ".").strip()
        if price_str:
            try:
                fval = float(price_str)
                return fval, f"{fval:.2f} {CURRENCY}"
            except ValueError:
                pass

    # 2. DOM selectors (ex. VAT variants first, then fallback to incl.)
    selectors = [
        "[data-price-without-vat]",       # data attribute
        ".price-without-vat",
        ".price-ex-vat",
        ".js-price-ex-vat",
        ".product-price-ex",
        ".price",                          # generic fallback
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            raw = el.get("data-price-without-vat") or el.get_text(strip=True)
            cleaned = re.sub(r"[^\d,\.]", "", raw).replace(",", ".")
            if cleaned:
                try:
                    fval = float(cleaned)
                    return fval, f"{fval:.2f} {CURRENCY}"
                except ValueError:
                    pass

    # movement.as bekreftet selektorer
    ex_el = soup.select_one(".sp__price.excluded")
    incl_el = soup.select_one(".sp__price.included")
    if ex_el:
        raw = re.sub(r"[^\d]", "", ex_el.get_text(strip=True).replace(".", ""))
        if raw:
            return float(raw), f"{float(raw):.2f} {CURRENCY}"
    if incl_el:
        raw = re.sub(r"[^\d]", "", incl_el.get_text(strip=True).replace(".", ""))
        if raw:
            return float(raw), f"{float(raw):.2f} {CURRENCY}"
    log.warning("Price not found")
    return 0.0, ""


def _extract_description(soup: BeautifulSoup) -> str:
    """
    Look for the 'Beskrivelse' tab content.
    Common patterns on Nordic e-com platforms.
    """
    # Tab panel approach
    for selector in [
        "#tab-description",
        "[data-tab='description']",
        ".product-description",
        ".tab-pane.active",
        "[aria-label='Beskrivelse']",
        ".product-details__description",
    ]:
        el = soup.select_one(selector)
        if el:
            return _clean_description(el.get_text(separator="\n", strip=True))

    # Heading-based fallback: find "Beskrivelse" heading and take next sibling
    for heading in soup.find_all(["h2", "h3", "h4"], string=re.compile(r"Beskrivelse", re.I)):
        sibling = heading.find_next_sibling()
        if sibling:
            return _clean_description(sibling.get_text(separator="\n", strip=True))

    # meta description as last resort
    meta = soup.find("meta", {"name": "description"})
    if meta:
        return meta.get("content", "").strip()

    return ""


def _clean_description(text: str) -> str:
    """Remove excessive whitespace/newlines while preserving readability."""
    lines = [l.strip() for l in text.splitlines()]
    lines = [l for l in lines if l]
    return "\n".join(lines)[:5000]   # GMC description cap


def _extract_images(soup: BeautifulSoup, ld: dict) -> tuple[str, list[str]]:
    """
    Main image: og:image (cleanest, no thumbnails).
    Extra images: product gallery slider imgs.
    Returns (main_url, [extra_url, ...])
    """
    # Main: og:image
    main_img = ""
    og = soup.find("meta", property="og:image")
    if og:
        main_img = og.get("content", "").strip()

    # JSON-LD fallback
    if not main_img and "Product" in ld:
        img_ld = ld["Product"].get("image", "")
        if isinstance(img_ld, list):
            main_img = img_ld[0] if img_ld else ""
        elif isinstance(img_ld, str):
            main_img = img_ld

    # Gallery: look for common slider patterns
    extra_imgs = []
    seen = {main_img}

    gallery_selectors = [
        ".product-images img",
        ".product-gallery img",
        ".slider img",
        ".swiper-slide img",
        "[data-gallery] img",
        ".thumbnails img",
        ".product-image-gallery img",
    ]
    for sel in gallery_selectors:
        imgs = soup.select(sel)
        if imgs:
            for img in imgs:
                src = img.get("data-src") or img.get("src") or ""
                # Prefer full-size: look for data-zoom-image or similar
                src = img.get("data-zoom-image") or img.get("data-large") or src
                src = urljoin(BASE_URL, src)
                # Filter out icons/tiny images and duplicates
                if src not in seen and "placeholder" not in src and len(src) > 20:
                    extra_imgs.append(src)
                    seen.add(src)
            break   # stop at first matching gallery selector

    return main_img, extra_imgs[:9]   # GMC allows up to 10 images total


def _extract_brand(soup: BeautifulSoup, ld: dict) -> str:
    """Extract brand from JSON-LD or DOM."""
    if "Product" in ld:
        brand = ld["Product"].get("brand", {})
        if isinstance(brand, dict):
            return brand.get("name", "")
        if isinstance(brand, str):
            return brand

    for sel in [".product-brand", "[itemprop='brand']", ".brand"]:
        el = soup.select_one(sel)
        if el:
            return el.get_text(strip=True)

    return "Movement"   # site default brand


def _extract_availability(soup: BeautifulSoup, ld: dict) -> str:
    """Map site stock status to Google values."""
    if "Product" in ld:
        offers = ld["Product"].get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        avail = offers.get("availability", "")
        if "InStock" in avail:
            return "in_stock"
        if "OutOfStock" in avail:
            return "out_of_stock"
        if "PreOrder" in avail:
            return "preorder"

    for sel in [".stock-status", ".availability", "[data-availability]"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True).lower()
            if any(x in txt for x in ("på lager", "tilgjengelig", "in stock")):
                return "in_stock"
            if any(x in txt for x in ("ikke på lager", "utsolgt", "out of stock")):
                return "out_of_stock"

    return "in_stock"   # safe default


def _extract_co2_label(soup: BeautifulSoup) -> str:
    """
    movement.as is a used-goods reseller — CO2 savings is a key USP.
    Look for any CO2/miljø text and return a clean label.
    """
    for sel in [".co2", ".sustainability", ".environment", ".eco", "[class*='co2']", "[class*='klima']"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                return txt[:100]

    # Text scan across full page
    text = soup.get_text()
    match = re.search(r'(CO2[^.\n]{5,80})', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()[:100]

    return "Brukt møbel - CO2-besparelse vs. nyproduksjon"   # evergreen label for used goods


def _price_tier(price_value: float) -> str:
    if price_value >= PRICE_TIER_HIGH:
        return "Høy"
    elif price_value >= PRICE_TIER_MEDIUM:
        return "Medium"
    return "Lav"


def _extract_product_id(soup: BeautifulSoup, ld: dict, url: str) -> str:
    """
    Priority: JSON-LD productID → DOM → URL slug number.
    """
    if "Product" in ld:
        pid = ld["Product"].get("productID") or ld["Product"].get("sku") or ""
        if pid:
            return str(pid)

    for sel in ["[itemprop='productID']", "[data-product-id]", ".product-id"]:
        el = soup.select_one(sel)
        if el:
            val = el.get("content") or el.get("data-product-id") or el.get_text(strip=True)
            if val:
                return val.strip()

    # URL fallback: last number before .html
    match = re.search(r'-(\d{4,6})\.html$', url)
    if match:
        return match.group(1)

    return ""


# ─── Main Product Scrape ──────────────────────────────────────────────────────
def scrape_product(url: str) -> Optional[Product]:
    """Scrape a single product page and return a Product dataclass."""
    resp = _get(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    ld   = _extract_ld_json(soup)

    p = Product(url=url)

    # ── Core fields ───────────────────────────────────────────────────────────
    p.product_id = _extract_product_id(soup, ld, url)

    # Title: JSON-LD → og:title → <title> tag
    if "Product" in ld:
        p.title_raw = ld["Product"].get("name", "").strip()
    if not p.title_raw:
        og_title = soup.find("meta", property="og:title")
        p.title_raw = og_title.get("content", "").strip() if og_title else ""
    if not p.title_raw:
        title_tag = soup.find("title")
        p.title_raw = title_tag.get_text(strip=True) if title_tag else ""

    # Breadcrumbs & taxonomy
    p.breadcrumbs  = _extract_breadcrumbs(soup, ld, url)
    p.product_type = _build_product_type(p.breadcrumbs)
    p.leaf_category = p.breadcrumbs[-1] if p.breadcrumbs else ""

    # Smart SEO title
    p.title_seo = _smart_title(p.title_raw, p.leaf_category)

    # Price
    p.price_value, p.price = _extract_price(soup, ld)

    # Other fields
    p.availability    = _extract_availability(soup, ld)
    p.description     = _extract_description(soup)
    p.brand           = _extract_brand(soup, ld)
    p.image_main, p.images_extra = _extract_images(soup, ld)

    # Custom Labels
    p.custom_label_0 = _price_tier(p.price_value)
    p.custom_label_1 = p.availability.replace("_", " ").title()
    p.custom_label_2 = _extract_co2_label(soup)
    p.custom_label_3 = "Brukt" if p.condition == "used" else "Nytt"
    p.custom_label_4 = f"ID:{p.product_id}"

    if not p.product_id:
        log.warning(f"No product ID found for {url}")

    return p


def scrape_all(urls: list[str], max_products: int = 0) -> list[Product]:
    """Scrape all URLs. Set max_products > 0 to limit (useful for testing)."""
    products = []
    total = len(urls) if not max_products else min(max_products, len(urls))
    for i, url in enumerate(urls[:total], 1):
        log.info(f"[{i}/{total}] Scraping: {url}")
        p = scrape_product(url)
        if p:
            products.append(p)
        else:
            log.warning(f"  → Skipped (failed to scrape)")
    log.info(f"Scraped {len(products)} products successfully")
    return products


# ─── 3. FEED GENERATION ───────────────────────────────────────────────────────
def _cdata(text: str) -> str:
    """Wrap text in CDATA to safely include HTML/special chars."""
    return f"<![CDATA[{text}]]>"


def _prettify(root, extra_ns: dict = None) -> str:
    """Return pretty-printed XML string. ET auto-injects registered namespaces."""
    raw = ET.tostring(root, encoding="unicode", xml_declaration=False)
    full = f'<?xml version="1.0" encoding="UTF-8"?>\n{raw}'
    parsed = minidom.parseString(full.encode("utf-8"))
    return parsed.toprettyxml(indent="  ", encoding=None)


def build_google_feed(products: list[Product]) -> str:
    """
    Generate Google Merchant Center RSS 2.0 feed.
    Spec: https://support.google.com/merchants/answer/7052112
    """
    # Register namespace so ET serialises as g:xxx not ns0:xxx
    ET.register_namespace("g", "http://base.google.com/ns/1.0")
    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text       = "Movement Google Shopping Feed"
    ET.SubElement(channel, "link").text        = BASE_URL
    ET.SubElement(channel, "description").text = "Brukte kvalitetsmøbler fra Movement AS"

    G = "http://base.google.com/ns/1.0"

    for p in products:
        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text                        = p.title_seo
        ET.SubElement(item, "{%s}id" % G).text                  = p.product_id
        ET.SubElement(item, "link").text                         = p.url
        ET.SubElement(item, "description").text                  = p.description
        ET.SubElement(item, "{%s}price" % G).text               = p.price
        ET.SubElement(item, "{%s}availability" % G).text        = p.availability
        ET.SubElement(item, "{%s}condition" % G).text           = p.condition
        ET.SubElement(item, "{%s}identifier_exists" % G).text   = p.identifier_exists
        ET.SubElement(item, "{%s}brand" % G).text               = p.brand

        if p.product_type:
            ET.SubElement(item, "{%s}product_type" % G).text = p.product_type

        if p.image_main:
            ET.SubElement(item, "{%s}image_link" % G).text = p.image_main
        for img in p.images_extra:
            ET.SubElement(item, "{%s}additional_image_link" % G).text = img

        # Custom labels
        ET.SubElement(item, "{%s}custom_label_0" % G).text = p.custom_label_0
        ET.SubElement(item, "{%s}custom_label_1" % G).text = p.custom_label_1
        ET.SubElement(item, "{%s}custom_label_2" % G).text = p.custom_label_2
        ET.SubElement(item, "{%s}custom_label_3" % G).text = p.custom_label_3
        ET.SubElement(item, "{%s}custom_label_4" % G).text = p.custom_label_4

        # Shipping
        ship = ET.SubElement(item, "{%s}shipping" % G)
        ET.SubElement(ship, "{%s}country" % G).text = COUNTRY
        ET.SubElement(ship, "{%s}price" % G).text   = p.shipping_price

    return _prettify(rss)


def build_meta_feed(products: list[Product]) -> str:
    """
    Generate Meta Commerce Manager / Automotive Inventory Ads feed.
    Uses <listings>/<listing> format.
    """
    root = ET.Element("listings")

    for p in products:
        listing = ET.SubElement(root, "listing")

        ET.SubElement(listing, "id").text              = p.product_id
        ET.SubElement(listing, "title").text           = p.title_seo
        ET.SubElement(listing, "description").text     = p.description
        ET.SubElement(listing, "url").text             = p.url
        ET.SubElement(listing, "price").text           = p.price
        ET.SubElement(listing, "availability").text    = p.availability
        ET.SubElement(listing, "condition").text       = p.condition

        if p.image_main:
            ET.SubElement(listing, "image_link").text = p.image_main
        for img in p.images_extra:
            ET.SubElement(listing, "additional_image_link").text = img

        ET.SubElement(listing, "brand").text = p.brand

        if p.product_type:
            ET.SubElement(listing, "product_type").text = p.product_type

        ET.SubElement(listing, "custom_label_0").text = p.custom_label_0
        ET.SubElement(listing, "custom_label_1").text = p.custom_label_1
        ET.SubElement(listing, "custom_label_2").text = p.custom_label_2

    return _prettify(root)


# ─── 4. ENTRY POINT ──────────────────────────────────────────────────────────
def main(test_mode: bool = False, test_limit: int = 5):
    import os

    # Discover URLs from sitemap
    urls = discover_product_urls()

    if not urls:
        log.error("No product URLs found. Check sitemap or site structure.")
        return

    if test_mode:
        log.info(f"TEST MODE: limiting to {test_limit} products")
        urls = urls[:test_limit]

    # Scrape
    products = scrape_all(urls)

    if not products:
        log.error("No products scraped. Aborting feed generation.")
        return

    # Output directory
    out_dir = os.environ.get("FEED_OUTPUT_DIR", ".")
    os.makedirs(out_dir, exist_ok=True)

    # Write feeds
    google_xml = build_google_feed(products)
    meta_xml   = build_meta_feed(products)

    google_path = os.path.join(out_dir, "feed_google.xml")
    meta_path   = os.path.join(out_dir, "feed_meta.xml")

    with open(google_path, "w", encoding="utf-8") as f:
        f.write(google_xml)
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(meta_xml)

    log.info(f"✅ Google feed → {google_path}  ({len(products)} products)")
    log.info(f"✅ Meta feed   → {meta_path}  ({len(products)} products)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="movement.as feed scraper")
    parser.add_argument("--test",       action="store_true", help="Test mode (5 products)")
    parser.add_argument("--test-limit", type=int, default=5,  help="Number of products in test mode")
    args = parser.parse_args()
    main(test_mode=args.test, test_limit=args.test_limit)
