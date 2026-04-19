"""
movement.as — Product Feed Scraper v2
======================================
Genererer feed_google.xml (GMC RSS 2.0) og feed_meta.xml (Meta Retail)

Datastruktur bekreftet fra live HTML:
  Pris eks. mva : .sp__price.excluded
  Pris inkl mva : .sp__price.included
  Tilbudspris   : .sp__price--sale / .sp__price.sale (hvis tilbud)
  Egenskaper    : .attributes → .attributeName + søsken
  Lagerstatus   : .sp__amount-info
  Bilder        : og:image (hoved) + .sp__image-gallery img (ekstra)
  Breadcrumbs   : JSON-LD BreadcrumbList → DOM → URL-fallback
  Brand         : .sp__brand

Author: Amidays
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import random
import logging
import os
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dataclasses import dataclass, field
from typing import Optional

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL       = "https://www.movement.as"
SITEMAP_URL    = "https://www.movement.as/sitemap.xml"
CURRENCY       = "NOK"
COUNTRY        = "NO"
CONDITION      = "used"
IDENTIFIER_EXISTS = "no"

REQUEST_DELAY_MIN = 1.2
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
})


def _get(url: str, retries: int = 3) -> Optional[requests.Response]:
    """GET med retry og politeness-delay."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200:
                return r
            elif r.status_code in (429, 503):
                wait = 15 * (attempt + 1)
                log.warning(f"Rate limited. Venter {wait}s …")
                time.sleep(wait)
            elif r.status_code == 403:
                log.error(f"403 Forbidden: {url}")
                return None
            else:
                log.warning(f"HTTP {r.status_code} for {url}")
        except requests.RequestException as e:
            log.error(f"Request error ({attempt+1}/{retries}): {e}")
    return None


# ─── Data Model ───────────────────────────────────────────────────────────────
@dataclass
class Product:
    url: str

    # Identifikasjon
    product_id: str     = ""
    title_raw: str      = ""
    title_seo: str      = ""
    description: str    = ""
    brand: str          = ""

    # Breadcrumbs
    breadcrumbs: list   = field(default_factory=list)
    product_type: str   = ""
    leaf_category: str  = ""

    # Tilstand
    condition: str            = CONDITION
    identifier_exists: str    = IDENTIFIER_EXISTS
    google_product_category: str = ""
    availability: str         = "in_stock"

    # Priser — eks. mva (standard / B2B)
    price_ex: float     = 0.0   # Grunnpris eks mva
    price_ex_str: str   = ""    # "790 NOK"
    sale_ex: float      = 0.0   # Tilbudspris eks mva (0 = ingen tilbud)
    sale_ex_str: str    = ""    # "590 NOK" (tomt hvis ingen tilbud)

    # Priser — inkl. mva (B2C)
    price_incl: float   = 0.0
    price_incl_str: str = ""
    sale_incl: float    = 0.0
    sale_incl_str: str  = ""

    # Bilder
    image_main: str     = ""
    images_extra: list  = field(default_factory=list)

    # Produktegenskaper (fra .attributes)
    attributes: dict    = field(default_factory=dict)
    color: str          = ""    # Hovedfarge
    color_secondary: str = ""   # Sekundærfarge
    material: str       = ""    # Materiale (fra attributter eller beskrivelse)
    mpn: str            = ""    # Modellnummer

    # Dimensjoner (cm, float)
    width: float        = 0.0
    height: float       = 0.0
    depth: float        = 0.0
    seat_height: float  = 0.0
    diameter: float     = 0.0

    # Beregnet fraktvekt
    shipping_weight: float = 0.0   # kg, volum-metoden

    # Lager
    quantity: str       = ""

    # Custom labels (settes manuelt i GMC)
    custom_label_0: str = ""
    custom_label_1: str = ""
    custom_label_2: str = ""
    custom_label_3: str = ""
    custom_label_4: str = ""


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _parse_price(raw: str) -> float:
    """
    Parser norske prisstrenger til float.
    "3.950 ,-eks mva" → 3950.0
    "988 ,-inkl mva"  → 988.0
    """
    cleaned = re.sub(r"[^\d,\.]", "", raw)
    if not cleaned:
        return 0.0
    # Norsk format: punktum = tusenskille, komma = desimal
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _fmt(value: float) -> str:
    """Formater pris uten desimaler: 3950.0 → '3950 NOK'"""
    if not value:
        return ""
    return f"{int(value)} {CURRENCY}"


def _parse_dim(raw: str) -> float:
    """Parser dimensjonsstreng: '180.00 cm' → 180.0"""
    match = re.search(r"([\d,\.]+)", raw)
    if match:
        try:
            return float(match.group(1).replace(",", "."))
        except ValueError:
            pass
    return 0.0


MAX_SHIPPING_WEIGHT = 500.0   # kg — cap for urealistiske volum

def _calc_shipping_weight(width: float, height: float, depth: float) -> float:
    """
    Volum-metoden for fraktvekt.
    (B × H × D) / 1000 / 5 = fraktvekt i kg
    Brukes kun hvis alle tre dimensjoner er tilgjengelige.
    Hvis høyde mangler (f.eks. bordplate), bruk 10cm som estimat.
    """
    if not width or not depth:
        return 0.0
    h = height if height else 10.0  # estimat for flate gjenstander
    volume_liters = (width * h * depth) / 1000
    weight = round(volume_liters / 5, 1)
    return min(weight, MAX_SHIPPING_WEIGHT)



# Google Product Category — numeriske IDer
# Ref: https://www.google.com/basepages/producttype/taxonomy-with-ids.en-US.txt
GOOGLE_CATEGORY_MAP = {
    "kontorstol":   "447",    # Office Supplies > Office Furniture > Chairs > Office Chairs
    "gjestestol":   "447",
    "barstol":      "447",
    "stoler":       "446",    # Office Supplies > Office Furniture > Chairs
    "loungestol":   "2786",   # Furniture > Living Room Furniture > Chairs
    "sofa":         "2634",   # Furniture > Living Room Furniture > Sofas
    "bordplater":   "4090",   # Office Supplies > Office Furniture > Tables > Office Table Tops
    "møtebord":     "4317",   # Office Supplies > Office Furniture > Tables > Conference Tables
    "skrivebord":   "4191",   # Office Supplies > Office Furniture > Desks
    "hev":          "4191",   # Hevsenk = skrivebord
    "bord":         "4316",   # Office Supplies > Office Furniture > Tables
    "reol":         "4318",   # Office Supplies > Office Furniture > Bookcases & Shelving Units
    "oppbevaring":  "4318",
    "skap":         "4163",   # Office Supplies > Office Furniture > Filing Cabinets
    "garderobe":    "4163",
    "whiteboard":   "932",    # Office Supplies > Presentation Supplies > Whiteboards
    "tavle":        "932",
    "belysning":    "2634",   # Lighting
    "lampe":        "594",    # Lighting > Lamps
}

def _map_google_category(breadcrumbs: list, title: str) -> str:
    """Mapper breadcrumbs/tittel til Google Product Category numerisk ID."""
    search_text = " ".join(breadcrumbs + [title]).lower()
    for keyword, cat_id in GOOGLE_CATEGORY_MAP.items():
        if keyword in search_text:
            return cat_id
    return "4319"   # Office Supplies > Office Furniture (fallback)

# ─── Extraction Functions ─────────────────────────────────────────────────────
def _extract_ld_json(soup: BeautifulSoup) -> dict:
    """Henter alle JSON-LD blokker, returnerer dict keyed by @type."""
    result = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    result[item.get("@type", "")] = item
            elif isinstance(data, dict):
                t = data.get("@type", "")
                result[t] = data
        except (json.JSONDecodeError, AttributeError):
            pass
    return result


def _extract_breadcrumbs(soup: BeautifulSoup, ld: dict, url: str) -> list:
    """JSON-LD → DOM → URL-fallback. Returnerer liste uten 'Hjem'."""
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

    # 2. DOM
    for selector in [".breadcrumb li", "nav.breadcrumb ol li", "[itemtype*='BreadcrumbList'] [itemprop='name']"]:
        nodes = soup.select(selector)
        if nodes:
            crumbs = [n.get_text(strip=True) for n in nodes]
            crumbs = [c for c in crumbs if c and c.lower() not in ("hjem", "home", ">", "/")]
            if crumbs:
                return crumbs

    # 3. URL-fallback
    path = urlparse(url).path.strip("/")
    parts = path.split("/")[:-1]
    return [p.replace("-", " ").title() for p in parts if p]


def _extract_prices(soup: BeautifulSoup) -> tuple:
    """
    Henter alle fire prisvariantene fra movement.as.

    Normal situasjon (ingen tilbud):
      .sp__price.excluded = grunnpris eks mva
      .sp__price.included = grunnpris inkl mva
      sale = 0

    Ved tilbud:
      .priceReduce span = ORIGINALPRISEN (strøket, eks mva)
      .sp__price.excluded = TILBUDSPRISEN (eks mva)
      .sp__price.included = TILBUDSPRISEN (inkl mva)

    Returns:
      (price_ex, price_incl, sale_ex, sale_incl)
      price_ex  = alltid originalprisen
      sale_ex   = tilbudspris hvis tilbud, ellers 0
    """
    current_ex = current_incl = original_ex = 0.0

    # Nåværende priser (alltid til stede)
    ex_el   = soup.select_one(".sp__price.excluded")
    incl_el = soup.select_one(".sp__price.included")

    if ex_el:
        current_ex = _parse_price(ex_el.get_text(strip=True))
    if incl_el:
        current_incl = _parse_price(incl_el.get_text(strip=True))

    # Sjekk om det er tilbud — .priceReduce inneholder originalprisen
    reduce_el = soup.select_one(".priceReduce span:not(i)")
    if not reduce_el:
        reduce_el = soup.select_one(".priceReduce span")
    if reduce_el:
        val = _parse_price(reduce_el.get_text(strip=True))
        if val and val > current_ex:
            original_ex = val

    if original_ex:
        # Tilbud: price = original, sale = nåværende
        price_ex   = original_ex
        price_incl = round(original_ex * 1.25, 0)
        sale_ex    = current_ex
        sale_incl  = current_incl if current_incl else round(current_ex * 1.25, 0)
    else:
        # Ingen tilbud
        price_ex   = current_ex
        price_incl = current_incl if current_incl else round(current_ex * 1.25, 0)
        sale_ex    = 0.0
        sale_incl  = 0.0

    if not price_ex:
        log.warning("Pris ikke funnet")

    return price_ex, price_incl, sale_ex, sale_incl


def _extract_mpn(description: str, attributes: dict) -> str:
    """
    Henter modellnummer fra beskrivelse eller attributter.
    Ser etter mønstre som 'Modell: X', 'Vare: X', 'MPN: X'
    """
    # Sjekk attributter først
    for key in ["Modell", "Modellnummer", "MPN", "Varenr", "Artikkelnummer"]:
        if key in attributes:
            return attributes[key].strip()

    # Søk i beskrivelse
    patterns = [
        r"[Mm]odell[:\s]+([A-Za-z0-9\-\/]+)",
        r"[Vv]are[:\s]+([A-Za-z0-9\-\/]+)",
        r"MPN[:\s]+([A-Za-z0-9\-\/]+)",
        r"[Aa]rtikkel[:\s]+([A-Za-z0-9\-\/]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, description or "")
        if match:
            return match.group(1).strip()
    return ""


def _extract_attributes(soup: BeautifulSoup) -> dict:
    """
    Henter produktegenskaper fra .attributes-seksjonen.
    Returnerer: {"Hovedfarge": "Sort", "Bredde": "180.00 cm", ...}
    """
    result = {}
    attrs_el = soup.select_one(".attributes")
    if not attrs_el:
        return result
    for name_el in attrs_el.find_all(class_="attributeName"):
        key = name_el.get_text(strip=True).rstrip(":")
        val_el = name_el.find_next_sibling()
        if val_el:
            result[key] = val_el.get_text(strip=True)
    return result


def _extract_quantity(soup: BeautifulSoup) -> str:
    """Henter lagerbeholdning fra .sp__amount-info"""
    el = soup.select_one(".sp__amount-info")
    if el:
        txt = el.get_text(strip=True)
        match = re.search(r"(\d+\+?)\s*stk", txt, re.IGNORECASE)
        if match:
            return match.group(1)
        # Sjekk hidden maxamount input
    hidden = soup.find("input", {"name": "maxamount"})
    if hidden:
        return hidden.get("value", "")
    return ""


def _extract_images(soup: BeautifulSoup, ld: dict) -> tuple:
    """
    Hoved: og:image
    Ekstra: bilder fra galleri-slider
    Returns: (main_url, [extra_urls])
    """
    main_img = ""
    og = soup.find("meta", property="og:image")
    if og:
        main_img = og.get("content", "").strip()

    if not main_img and "Product" in ld:
        img = ld["Product"].get("image", "")
        main_img = img[0] if isinstance(img, list) else img

    extra_imgs = []
    seen = {main_img}

    for sel in [".sp__image-gallery img", ".product-images img",
                ".swiper-slide img", ".slider img", ".thumbnails img"]:
        imgs = soup.select(sel)
        if imgs:
            for img in imgs:
                src = img.get("data-zoom-image") or img.get("data-src") or img.get("src") or ""
                src = urljoin(BASE_URL, src)
                if src not in seen and "placeholder" not in src and len(src) > 20:
                    extra_imgs.append(src)
                    seen.add(src)
            break

    return main_img, extra_imgs[:9]


def _extract_brand(soup: BeautifulSoup) -> str:
    """Henter brand fra .sp__brand"""
    el = soup.select_one(".sp__brand")
    if el:
        brand = el.get_text(strip=True)
        if brand:
            return brand
    return "Movement"


def _extract_description(soup: BeautifulSoup) -> str:
    """Henter beskrivelse fra Beskrivelse-tabben."""
    for sel in ["#tab-description", ".product-description",
                ".tab-pane", "[data-tab='description']"]:
        el = soup.select_one(sel)
        if el:
            return _clean_text(el.get_text(separator="\n", strip=True))

    # Heading-fallback
    for h in soup.find_all(["h2", "h3"], string=re.compile(r"Beskrivelse", re.I)):
        sib = h.find_next_sibling()
        if sib:
            return _clean_text(sib.get_text(separator="\n", strip=True))

    # og:description fallback
    meta = soup.find("meta", {"name": "description"})
    if meta:
        return meta.get("content", "").strip()

    return ""


def _clean_text(text: str) -> str:
    """Fjerner overflødig whitespace, beholder lesbarhet."""
    lines = [l.strip() for l in text.splitlines()]
    lines = [l for l in lines if l]
    return "\n".join(lines)[:5000]


def _extract_availability(soup: BeautifulSoup, ld: dict) -> str:
    """Mapper lagerstatus til Google-verdier."""
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

    qty_el = soup.select_one(".sp__amount-info")
    if qty_el:
        txt = qty_el.get_text(strip=True).lower()
        if "ikke" in txt or "utsolgt" in txt:
            return "out_of_stock"

    return "in_stock"


def _extract_product_id(soup: BeautifulSoup, ld: dict, url: str) -> str:
    """ID fra JSON-LD → DOM → URL-slug."""
    if "Product" in ld:
        pid = ld["Product"].get("productID") or ld["Product"].get("sku") or ""
        if pid:
            return str(pid)

    id_el = soup.select_one(".sp__info-id")
    if id_el:
        match = re.search(r"\d+", id_el.get_text())
        if match:
            return match.group()

    match = re.search(r"-(\d{4,6})\.html$", url)
    if match:
        return match.group(1)

    return ""


def _smart_title(raw: str, leaf_category: str, color: str, material: str) -> str:
    """
    Smart Title v2:
    Legg til leafkategori, farge og materiale hvis de ikke allerede er i tittelen.
    Format: {Navn} - {Kategori} - {Farge} - {Materiale}
    """
    result = raw
    appended = []

    if leaf_category and leaf_category.lower() not in result.lower():
        appended.append(leaf_category)

    if color and color.lower() not in result.lower():
        appended.append(color)

    if material and material.lower() not in result.lower():
        appended.append(material)

    if appended:
        result = f"{result} - {' - '.join(appended)}"

    return result


# ─── Main Scrape ──────────────────────────────────────────────────────────────
def scrape_product(url: str) -> Optional[Product]:
    """Scraper én produktside og returnerer Product."""
    resp = _get(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    ld   = _extract_ld_json(soup)
    p    = Product(url=url)

    # Identifikasjon
    p.product_id  = _extract_product_id(soup, ld, url)
    p.brand       = _extract_brand(soup)
    p.description = _extract_description(soup)
    p.availability = _extract_availability(soup, ld)

    # Condition: NY/UBRUKT i tittel eller URL = new, ellers used
    _ny_kw = ["ny-ubrukt", "ny/ubrukt", "nyubrukt", "ubrukt", "ny-i-eske", "-ny-"]
    _check = ((p.title_raw or "") + " " + p.url).lower()
    p.condition = "new" if any(k in _check for k in _ny_kw) else CONDITION

    # Tittel
    if "Product" in ld:
        p.title_raw = ld["Product"].get("name", "").strip()
    if not p.title_raw:
        og = soup.find("meta", property="og:title")
        p.title_raw = og.get("content", "").strip() if og else ""
    if not p.title_raw:
        h1 = soup.select_one("h1")
        p.title_raw = h1.get_text(strip=True) if h1 else ""

    # Breadcrumbs
    p.breadcrumbs   = _extract_breadcrumbs(soup, ld, url)
    p.product_type  = " > ".join(p.breadcrumbs)
    p.leaf_category = p.breadcrumbs[-1] if p.breadcrumbs else ""

    # Bilder
    p.image_main, p.images_extra = _extract_images(soup, ld)

    # Lager
    p.quantity = _extract_quantity(soup)

    # Priser
    p.price_ex, p.price_incl, p.sale_ex, p.sale_incl = _extract_prices(soup)
    p.price_ex_str   = _fmt(p.price_ex)
    p.price_incl_str = _fmt(p.price_incl)
    p.sale_ex_str    = _fmt(p.sale_ex)    # tomt hvis ingen tilbud
    p.sale_incl_str  = _fmt(p.sale_incl)  # tomt hvis ingen tilbud

    # Produktegenskaper
    p.attributes      = _extract_attributes(soup)
    p.color           = p.attributes.get("Hovedfarge", "")
    p.color_secondary = p.attributes.get("Sekundærfarge", "")
    p.material        = p.attributes.get("Materiale", p.attributes.get("Stoff", ""))

    # Dimensjoner
    p.width      = _parse_dim(p.attributes.get("Bredde", ""))
    p.height     = _parse_dim(p.attributes.get("Høyde", ""))
    p.depth      = _parse_dim(p.attributes.get("Dybde", ""))
    p.seat_height = _parse_dim(p.attributes.get("Sittehøyde", ""))
    p.diameter   = _parse_dim(p.attributes.get("Diameter", ""))

    # Beregnet fraktvekt
    p.shipping_weight = _calc_shipping_weight(p.width, p.height, p.depth)

    # MPN
    p.mpn = _extract_mpn(p.description, p.attributes)

    # Google Product Category
    p.google_product_category = _map_google_category(p.breadcrumbs, p.title_raw)

    # Material — fra attributter eller scrape fra beskrivelse
    p.material = p.attributes.get("Materiale", p.attributes.get("Stoff", ""))
    if not p.material:
        # Prioritert rekkefølge — mer spesifikke materialer først
        for kw in ["laminat", "eik", "bjørk", "bøk", "finer", "mdf",
                   "mesh", "skinn", "stoff", "tekstil",
                   "glass", "metall", "aluminium", "stål", "tre"]:
            if kw in (p.description or "").lower() or kw in (p.title_raw or "").lower():
                p.material = kw.capitalize()
                break

    # Smart Title v2
    p.title_seo = _smart_title(p.title_raw, p.leaf_category, p.color, p.material)

    if not p.product_id:
        log.warning(f"Ingen produkt-ID for {url}")

    return p


def scrape_all(urls: list, max_products: int = 0) -> list:
    """Scraper alle URLer. max_products > 0 begrenser antall (test-modus)."""
    products = []
    total = len(urls) if not max_products else min(max_products, len(urls))
    for i, url in enumerate(urls[:total], 1):
        log.info(f"[{i}/{total}] {url}")
        p = scrape_product(url)
        if p:
            products.append(p)
        else:
            log.warning(f"  → Hoppet over")
    log.info(f"Scraped {len(products)} produkter")
    return products


# ─── Sitemap Discovery ────────────────────────────────────────────────────────
def discover_product_urls(sitemap_url: str = SITEMAP_URL) -> list:
    """Walker sitemap-indeks → child-sitemaps → produkt-URLer."""
    log.info(f"Henter sitemap: {sitemap_url}")
    product_urls = []
    visited = set()

    def _parse(url: str):
        if url in visited:
            return
        visited.add(url)
        resp = _get(url)
        if not resp:
            return
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            log.error(f"XML-feil på {url}: {e}")
            return
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            _parse(loc.text.strip())
        for loc in root.findall(".//sm:url/sm:loc", ns):
            page_url = loc.text.strip()
            if re.search(r"-\d{4,6}\.html$", page_url):
                product_urls.append(page_url)

    _parse(sitemap_url)
    log.info(f"Fant {len(product_urls)} produkt-URLer")
    return list(dict.fromkeys(product_urls))


# ─── Feed: Google Merchant Center ────────────────────────────────────────────
def build_google_feed(products: list) -> str:
    """
    Genererer Google Merchant Center RSS 2.0 feed.
    Følger Golden Standard-strukturen med product_detail for ekstra attributter.
    """
    ET.register_namespace("g", "http://base.google.com/ns/1.0")
    G   = "http://base.google.com/ns/1.0"
    rss = ET.Element("rss", {"version": "2.0"})
    ch  = ET.SubElement(rss, "channel")

    ET.SubElement(ch, "title").text       = "Movement Google Shopping Feed"
    ET.SubElement(ch, "link").text        = BASE_URL
    ET.SubElement(ch, "description").text = "Brukte kvalitetsmøbler – Movement AS"

    for p in products:
        item = ET.SubElement(ch, "item")

        # ── Kjernefelt ────────────────────────────────────────────────────────
        ET.SubElement(item, "{%s}id" % G).text                = p.product_id
        ET.SubElement(item, "{%s}title" % G).text             = p.title_seo
        ET.SubElement(item, "{%s}description" % G).text       = p.description
        ET.SubElement(item, "{%s}link" % G).text              = p.url
        ET.SubElement(item, "{%s}brand" % G).text             = p.brand
        ET.SubElement(item, "{%s}condition" % G).text    = p.condition
        ET.SubElement(item, "{%s}availability" % G).text = p.availability

        # Identifikatorer
        if p.mpn:
            ET.SubElement(item, "{%s}mpn" % G).text = p.mpn
        else:
            ET.SubElement(item, "{%s}identifier_exists" % G).text = "no"

        if p.google_product_category:
            ET.SubElement(item, "{%s}google_product_category" % G).text = p.google_product_category
        if p.product_type:
            ET.SubElement(item, "{%s}product_type" % G).text = p.product_type

        # ── Bilder ────────────────────────────────────────────────────────────
        if p.image_main:
            ET.SubElement(item, "{%s}image_link" % G).text = p.image_main
        for img in p.images_extra:
            ET.SubElement(item, "{%s}additional_image_link" % G).text = img

        # ── Priser (eks. mva — standard) ──────────────────────────────────────
        ET.SubElement(item, "{%s}price" % G).text      = p.price_ex_str
        ET.SubElement(item, "{%s}sale_price" % G).text  = p.sale_ex_str if p.sale_ex_str else p.price_ex_str

        # ── Priser (inkl. mva — B2C) ──────────────────────────────────────────
        if p.price_incl_str:
            ET.SubElement(item, "price_incl_vat").text = p.price_incl_str
        ET.SubElement(item, "sale_price_incl_vat").text = p.sale_incl_str if p.sale_incl_str else p.price_incl_str

        # ── Produktegenskaper ─────────────────────────────────────────────────
        if p.color:
            ET.SubElement(item, "{%s}color" % G).text = p.color
        if p.material:
            ET.SubElement(item, "{%s}material" % G).text = p.material

        # ── Dimensjoner som egne felt ─────────────────────────────────────────
        if p.width:
            ET.SubElement(item, "{%s}product_width" % G).text  = f"{int(p.width)} cm"
        if p.height:
            ET.SubElement(item, "{%s}product_height" % G).text = f"{int(p.height)} cm"
        if p.depth:
            ET.SubElement(item, "{%s}product_length" % G).text = f"{int(p.depth)} cm"

        # ── Fraktvekt (volum-metoden) ─────────────────────────────────────────
        if p.shipping_weight:
            ET.SubElement(item, "{%s}shipping_weight" % G).text = f"{p.shipping_weight} kg"

        # ── Lager ─────────────────────────────────────────────────────────────
        if p.quantity:
            ET.SubElement(item, "quantity").text = p.quantity

        # ── product_detail for ekstra dimensjoner ────────────────────────────
        extra_dims = {}
        if p.seat_height:
            extra_dims["Sittehøyde"] = f"{int(p.seat_height)} cm"
        if p.diameter:
            extra_dims["Diameter"] = f"{int(p.diameter)} cm"
        if p.color_secondary:
            extra_dims["Sekundærfarge"] = p.color_secondary

        for attr_name, attr_val in extra_dims.items():
            pd = ET.SubElement(item, "{%s}product_detail" % G)
            ET.SubElement(pd, "{%s}section_name" % G).text    = "Dimensjoner"
            ET.SubElement(pd, "{%s}attribute_name" % G).text  = attr_name
            ET.SubElement(pd, "{%s}attribute_value" % G).text = attr_val

        # ── Andre produktattributter i product_detail ────────────────────────
        skip_keys = {"Hovedfarge", "Sekundærfarge", "Materiale", "Stoff",
                     "Bredde", "Høyde", "Dybde", "Sittehøyde", "Diameter"}
        for key, val in p.attributes.items():
            if key not in skip_keys:
                pd = ET.SubElement(item, "{%s}product_detail" % G)
                ET.SubElement(pd, "{%s}section_name" % G).text    = "Spesifikasjoner"
                ET.SubElement(pd, "{%s}attribute_name" % G).text  = key
                ET.SubElement(pd, "{%s}attribute_value" % G).text = val

        # ── Custom labels (tomme — settes manuelt i GMC) ──────────────────────
        for i in range(5):
            ET.SubElement(item, "{%s}custom_label_%d" % (G, i)).text = getattr(p, f"custom_label_{i}", "")

    return _prettify(rss)


# ─── Feed: Meta Retail ───────────────────────────────────────────────────────
def build_meta_feed(products: list) -> str:
    """
    Genererer Meta Commerce Manager feed.
    Inkluderer dimensjoner og begge prisvariantene.
    """
    root = ET.Element("listings")

    for p in products:
        listing = ET.SubElement(root, "listing")

        ET.SubElement(listing, "id").text           = p.product_id
        ET.SubElement(listing, "title").text        = p.title_seo
        ET.SubElement(listing, "description").text  = p.description
        ET.SubElement(listing, "url").text          = p.url
        ET.SubElement(listing, "availability").text = p.availability
        ET.SubElement(listing, "condition").text    = p.condition
        ET.SubElement(listing, "brand").text        = p.brand

        # Identifikatorer
        if p.mpn:
            ET.SubElement(listing, "mpn").text = p.mpn
        else:
            ET.SubElement(listing, "identifier_exists").text = "no"

        if p.product_type:
            ET.SubElement(listing, "product_type").text = p.product_type

        # Bilder
        if p.image_main:
            ET.SubElement(listing, "image_link").text = p.image_main
        for img in p.images_extra:
            ET.SubElement(listing, "additional_image_link").text = img

        # Priser eks. mva
        ET.SubElement(listing, "price").text = p.price_ex_str
        if p.sale_ex_str:
            ET.SubElement(listing, "sale_price").text = p.sale_ex_str

        # Priser inkl. mva
        if p.price_incl_str:
            ET.SubElement(listing, "price_incl_vat").text = p.price_incl_str
        if p.sale_incl_str:
            ET.SubElement(listing, "sale_price_incl_vat").text = p.sale_incl_str

        # Egenskaper
        if p.color:
            ET.SubElement(listing, "color").text = p.color
        if p.color_secondary:
            ET.SubElement(listing, "color_secondary").text = p.color_secondary
        if p.material:
            ET.SubElement(listing, "material").text = p.material

        # Dimensjoner
        if p.width:
            ET.SubElement(listing, "product_width").text  = f"{int(p.width)} cm"
        if p.height:
            ET.SubElement(listing, "product_height").text = f"{int(p.height)} cm"
        if p.depth:
            ET.SubElement(listing, "product_length").text = f"{int(p.depth)} cm"
        if p.seat_height:
            ET.SubElement(listing, "seat_height").text = f"{int(p.seat_height)} cm"
        if p.diameter:
            ET.SubElement(listing, "diameter").text = f"{int(p.diameter)} cm"

        # Lager
        if p.quantity:
            ET.SubElement(listing, "quantity").text = p.quantity

    return _prettify(root)


# ─── XML Pretty Print ─────────────────────────────────────────────────────────
def _prettify(root) -> str:
    """Returnerer pen XML-streng med korrekt encoding."""
    raw    = ET.tostring(root, encoding="unicode", xml_declaration=False)
    full   = f'<?xml version="1.0" encoding="UTF-8"?>\n{raw}'
    parsed = minidom.parseString(full.encode("utf-8"))
    return parsed.toprettyxml(indent="  ", encoding=None)


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main(test_mode: bool = False, test_limit: int = 5):
    urls = discover_product_urls()
    if not urls:
        log.error("Ingen produkt-URLer funnet.")
        return

    if test_mode:
        log.info(f"TEST MODE: {test_limit} produkter")
        urls = urls[:test_limit]

    products = scrape_all(urls)
    if not products:
        log.error("Ingen produkter scraped.")
        return

    out_dir = os.environ.get("FEED_OUTPUT_DIR", ".")
    os.makedirs(out_dir, exist_ok=True)

    google_path = os.path.join(out_dir, "feed_google.xml")
    meta_path   = os.path.join(out_dir, "feed_meta.xml")

    with open(google_path, "w", encoding="utf-8") as f:
        f.write(build_google_feed(products))
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(build_meta_feed(products))

    log.info(f"✅ Google feed → {google_path}  ({len(products)} produkter)")
    log.info(f"✅ Meta feed   → {meta_path}  ({len(products)} produkter)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="movement.as feed scraper v2")
    parser.add_argument("--test",       action="store_true")
    parser.add_argument("--test-limit", type=int, default=5)
    args = parser.parse_args()
    main(test_mode=args.test, test_limit=args.test_limit)
