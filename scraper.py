"""
movement.as — Product Feed Scraper v3
======================================
Én feed (feed_google.xml) som fungerer for både Google Merchant Center og Meta.
Format: RSS 2.0 med g:-namespace (Google-standard, Meta-kompatibel)

Bekreftet HTML-struktur fra movement.as:
  H1 + .sp__desc      = full produkttittel (må kombineres)
  .sp__price.excluded = nåværende pris eks. mva
  .sp__price.included = nåværende pris inkl. mva
  .priceReduce span   = originalprisen eks. mva (kun ved tilbud)
  .attributes         = produktegenskaper (farge, mål osv.)
  .sp__amount-info    = lagerstatus
  .sp__brand          = merkenavn
  og:image            = hovedbilde

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
BASE_URL    = "https://www.movement.as"
SITEMAP_URL = "https://www.movement.as/sitemap.xml"
CURRENCY    = "NOK"
COUNTRY     = "NO"

REQUEST_DELAY_MIN = 1.2
REQUEST_DELAY_MAX = 3.0
MAX_SHIPPING_WEIGHT = 500.0  # kg cap

# Google Product Category numeriske IDer
# Ref: https://www.google.com/basepages/producttype/taxonomy-with-ids.en-US.txt
GOOGLE_CATEGORY_MAP = {
    "kontorstol":  "447",   # Office Supplies > Office Furniture > Chairs > Office Chairs
    "gjestestol":  "447",
    "barstol":     "447",
    "stoler":      "446",   # Office Supplies > Office Furniture > Chairs
    "loungestol":  "2786",  # Furniture > Living Room Furniture > Chairs
    "sofa":        "2634",  # Furniture > Living Room Furniture > Sofas
    "bordplater":  "4090",  # Office Supplies > Office Furniture > Tables > Office Table Tops
    "møtebord":    "4317",  # Office Supplies > Office Furniture > Tables > Conference Tables
    "skrivebord":  "4191",  # Office Supplies > Office Furniture > Desks
    "hev":         "4191",  # Hevsenk = skrivebord
    "bord":        "4316",  # Office Supplies > Office Furniture > Tables
    "reol":        "4318",  # Office Supplies > Office Furniture > Bookcases & Shelving Units
    "oppbevaring": "4318",
    "skap":        "4163",  # Office Supplies > Office Furniture > Filing Cabinets
    "garderobe":   "4163",
    "whiteboard":  "932",   # Office Supplies > Presentation Supplies > Whiteboards
    "tavle":       "932",
    "belysning":   "594",   # Lighting > Lamps
    "lampe":       "594",
}
GOOGLE_CATEGORY_DEFAULT = "4319"  # Office Supplies > Office Furniture


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
    product_id: str      = ""
    title_raw: str       = ""   # kombinert H1 + .sp__desc
    title_seo: str       = ""   # smart tittel med kategori/farge/materiale
    description: str     = ""
    brand: str           = ""
    mpn: str             = ""   # modellkode fra attributter eller beskrivelse
    gtin: str            = ""   # EAN/strekkode hvis finnes

    # Kategorier
    breadcrumbs: list    = field(default_factory=list)
    product_type: str    = ""   # norsk, fra breadcrumbs
    google_category: str = ""   # numerisk GMC ID

    # Tilstand
    condition: str       = "used"
    availability: str    = "in_stock"

    # Priser eks. mva (standard/B2B)
    price_ex: float      = 0.0  # alltid originalprisen
    price_ex_str: str    = ""   # "790 NOK"
    sale_ex: float       = 0.0  # tilbudspris (0 = ingen tilbud)
    sale_ex_str: str     = ""   # "590 NOK"

    # Priser inkl. mva (B2C)
    price_incl: float    = 0.0
    price_incl_str: str  = ""
    sale_incl: float     = 0.0
    sale_incl_str: str   = ""

    # Bilder
    image_main: str      = ""
    images_extra: list   = field(default_factory=list)

    # Produktegenskaper
    attributes: dict     = field(default_factory=dict)
    color: str           = ""
    color_secondary: str = ""
    material: str        = ""

    # Dimensjoner (cm)
    width: float         = 0.0
    height: float        = 0.0
    depth: float         = 0.0
    seat_height: float   = 0.0
    diameter: float      = 0.0

    # Beregnet fraktvekt
    shipping_weight: float = 0.0

    # Lager
    quantity: str        = ""

    # Custom labels
    custom_label_0: str  = ""   # Tilbud / tom
    custom_label_1: str  = ""   # Outlet (rabatt >= 30%) / tom
    custom_label_2: str  = ""   # Priskategori
    custom_label_3: str  = ""   # tom — manuell bruk i GMC/Meta
    custom_label_4: str  = ""   # tom — manuell bruk i GMC/Meta


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _parse_price(raw: str) -> float:
    """
    Parser norske prisstrenger til float.
    "3.950 ,-eks mva" → 3950.0
    "24.938 ,- inkl mva" → 24938.0
    """
    cleaned = re.sub(r"[^\d,\.]", "", raw)
    if not cleaned:
        return 0.0
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _fmt(value: float) -> str:
    """3950.0 → '3950 NOK'"""
    if not value:
        return ""
    return f"{int(value)} {CURRENCY}"


def _parse_dim(raw: str) -> float:
    """'180.00 cm' → 180.0"""
    match = re.search(r"([\d,\.]+)", raw)
    if match:
        try:
            return float(match.group(1).replace(",", "."))
        except ValueError:
            pass
    return 0.0


def _calc_shipping_weight(width: float, height: float, depth: float) -> float:
    """
    Volum-metoden: (B × H × D) / 1000 / 5 = fraktvekt kg
    Høyde estimeres til 10 cm hvis mangler (f.eks. bordplater)
    Cap: 500 kg
    """
    if not width or not depth:
        return 0.0
    h = height if height else 10.0
    volume_liters = (width * h * depth) / 1000
    return min(round(volume_liters / 5, 1), MAX_SHIPPING_WEIGHT)


def _clean_text(text: str) -> str:
    """Fjerner overflødig whitespace."""
    lines = [l.strip() for l in text.splitlines()]
    return "\n".join(l for l in lines if l)[:5000]


# ─── Extraction ───────────────────────────────────────────────────────────────
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
                result[data.get("@type", "")] = data
        except (json.JSONDecodeError, AttributeError):
            pass
    return result


def _extract_title(soup: BeautifulSoup) -> str:
    """
    Kombinerer H1 + .sp__desc for full tittel.
    H1: "Hvit bordplate til skrivebord fra"
    DESC: "Narbutas, 80x60cm, NY/UBRUKT"
    → "Hvit bordplate til skrivebord fra Narbutas, 80x60cm, NY/UBRUKT"
    """
    h1 = soup.select_one("h1.sp__title")
    desc = soup.select_one(".sp__desc")
    h1_text = h1.get_text(strip=True) if h1 else ""
    desc_text = desc.get_text(strip=True) if desc else ""
    if h1_text and desc_text:
        return f"{h1_text} {desc_text}".strip()
    return h1_text or desc_text


def _extract_breadcrumbs(soup: BeautifulSoup, ld: dict, url: str) -> list:
    """JSON-LD → DOM → URL-fallback. Returnerer liste uten 'Hjem'."""
    if "BreadcrumbList" in ld:
        items = sorted(
            ld["BreadcrumbList"].get("itemListElement", []),
            key=lambda x: x.get("position", 0)
        )
        crumbs = [i.get("item", {}).get("name", i.get("name", "")) for i in items]
        crumbs = [c.strip() for c in crumbs if c.strip() and c.lower() not in ("hjem", "home")]
        if crumbs:
            return crumbs

    for selector in [".breadcrumb li", "nav.breadcrumb ol li"]:
        nodes = soup.select(selector)
        if nodes:
            crumbs = [n.get_text(strip=True) for n in nodes]
            crumbs = [c for c in crumbs if c and c.lower() not in ("hjem", "home", ">", "/")]
            if crumbs:
                return crumbs

    path = urlparse(url).path.strip("/")
    parts = path.split("/")[:-1]
    return [p.replace("-", " ").title() for p in parts if p]


def _extract_prices(soup: BeautifulSoup) -> tuple:
    """
    Normal situasjon (ingen tilbud):
      .sp__price.excluded = nåværende pris eks mva → price_ex
      .sp__price.included = nåværende pris inkl mva → price_incl
      sale_ex = sale_incl = 0

    Ved tilbud (.priceReduce finnes):
      .priceReduce span = ORIGINALPRISEN eks mva → price_ex
      .sp__price.excluded = TILBUDSPRISEN eks mva → sale_ex
      .sp__price.included = TILBUDSPRISEN inkl mva → sale_incl
      price_incl = original * 1.25

    Returns: (price_ex, price_incl, sale_ex, sale_incl)
    """
    current_ex = current_incl = 0.0

    ex_el   = soup.select_one(".sp__price.excluded")
    incl_el = soup.select_one(".sp__price.included")

    if ex_el:
        current_ex = _parse_price(ex_el.get_text(strip=True))
    if incl_el:
        current_incl = _parse_price(incl_el.get_text(strip=True))

    # Tilbud: .priceReduce inneholder originalprisen
    original_ex = 0.0
    reduce_el = soup.select_one(".priceReduce")
    if reduce_el:
        # Finn span-teksten (ikke i-taggen med %-rabatt)
        for span in reduce_el.find_all("span"):
            if span.find("i") is None:
                val = _parse_price(span.get_text(strip=True))
                if val and val > current_ex:
                    original_ex = val
                    break

    if original_ex:
        price_ex    = original_ex
        price_incl  = round(original_ex * 1.25, 0)
        sale_ex     = current_ex
        sale_incl   = current_incl if current_incl else round(current_ex * 1.25, 0)
    else:
        price_ex    = current_ex
        price_incl  = current_incl if current_incl else round(current_ex * 1.25, 0)
        sale_ex     = 0.0
        sale_incl   = 0.0

    if not price_ex:
        log.warning("Pris ikke funnet")

    return price_ex, price_incl, sale_ex, sale_incl


def _extract_attributes(soup: BeautifulSoup) -> dict:
    """Henter produktegenskaper fra .attributes-seksjonen."""
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


def _extract_mpn(attributes: dict, description: str) -> str:
    """
    Henter MPN (modellkode) fra:
    1. Attributter: "Modell", "Varenr", "MPN"
    2. Beskrivelse: "Vare: PNN01508M1" eller "Modell: AIR-PCIM220"
    Koden må være minst 4 tegn og inneholde både bokstaver og tall.
    """
    # 1. Attributter
    for key in ["Modell", "Modellnummer", "MPN", "Varenr", "Artikkelnummer"]:
        if key in attributes:
            val = attributes[key].strip()
            if len(val) >= 4 and re.search(r"[A-Za-z]", val) and re.search(r"\d", val):
                return val

    # 2. Beskrivelse — "Vare: PNN01508M1" eller "Modell: AIR-PCIM220-M1E"
    patterns = [
        r"[Vv]are[:\s]+([A-Z0-9][A-Z0-9\-\/ ]{3,}?)(?:\s*[\n,.]|$)",
        r"[Mm]odell[:\s]+([A-Z0-9][A-Z0-9\-\/ ]{3,}?)(?:\s*[\n,.]|$)",
        r"MPN[:\s]+([A-Z0-9][A-Z0-9\-\/ ]{3,}?)(?:\s*[\n,.]|$)",
        r"[Aa]rt(?:ikkel)?(?:nr)?[:\s]+([A-Z0-9][A-Z0-9\-\/ ]{3,}?)(?:\s*[\n,.]|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, description or "")
        if match:
            val = match.group(1).strip()
            if re.search(r"[A-Za-z]", val) and re.search(r"\d", val):
                return val

    return ""


def _extract_images(soup: BeautifulSoup, ld: dict) -> tuple:
    """og:image som hoved, galleri som ekstra."""
    main_img = ""
    og = soup.find("meta", property="og:image")
    if og:
        main_img = og.get("content", "").strip()

    if not main_img and "Product" in ld:
        img = ld["Product"].get("image", "")
        main_img = img[0] if isinstance(img, list) else img

    extra_imgs = []
    seen = {main_img}
    for sel in [".sp__image-gallery img", ".product-images img", ".swiper-slide img"]:
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
    """Henter brand fra .sp__brand."""
    el = soup.select_one(".sp__brand")
    if el:
        brand = el.get_text(strip=True)
        if brand:
            return brand
    return "Movement"


def _extract_description(soup: BeautifulSoup) -> str:
    """Henter beskrivelse fra Beskrivelse-tabben."""
    for sel in ["#tab-description", ".product-description", ".tab-pane"]:
        el = soup.select_one(sel)
        if el:
            return _clean_text(el.get_text(separator="\n", strip=True))
    for h in soup.find_all(["h2", "h3"], string=re.compile(r"Beskrivelse", re.I)):
        sib = h.find_next_sibling()
        if sib:
            return _clean_text(sib.get_text(separator="\n", strip=True))
    meta = soup.find("meta", {"name": "description"})
    if meta:
        return meta.get("content", "").strip()
    return ""


def _extract_availability(soup: BeautifulSoup) -> str:
    """Mapper lagerstatus til Google-verdier."""
    qty_el = soup.select_one(".sp__amount-info")
    if qty_el:
        txt = qty_el.get_text(strip=True).lower()
        if "ikke" in txt or "utsolgt" in txt:
            return "out_of_stock"
    return "in_stock"


def _extract_quantity(soup: BeautifulSoup) -> str:
    """Henter lagerbeholdning."""
    el = soup.select_one(".sp__amount-info")
    if el:
        txt = el.get_text(strip=True)
        match = re.search(r"(\d+\+?)\s*stk", txt, re.IGNORECASE)
        if match:
            return match.group(1)
    hidden = soup.find("input", {"name": "maxamount"})
    if hidden:
        return hidden.get("value", "")
    return ""


def _extract_product_id(soup: BeautifulSoup, url: str) -> str:
    """ID fra .sp__info-id → URL-slug."""
    id_el = soup.select_one(".sp__info-id")
    if id_el:
        match = re.search(r"\d+", id_el.get_text())
        if match:
            return match.group()
    match = re.search(r"-(\d{4,6})\.html$", url)
    if match:
        return match.group(1)
    return ""


def _map_google_category(breadcrumbs: list, title: str) -> str:
    """Mapper breadcrumbs/tittel til Google Product Category numerisk ID."""
    search_text = " ".join(breadcrumbs + [title]).lower()
    for keyword, cat_id in GOOGLE_CATEGORY_MAP.items():
        if keyword in search_text:
            return cat_id
    return GOOGLE_CATEGORY_DEFAULT


def _detect_condition(title: str, url: str) -> str:
    """NY/UBRUKT i tittel eller URL = new, ellers used."""
    ny_kw = ["ny-ubrukt", "ny/ubrukt", "nyubrukt", "ubrukt", "ny-i-eske", "-ny-"]
    check = (title + " " + url).lower()
    return "new" if any(k in check for k in ny_kw) else "used"


def _detect_material(attributes: dict, description: str, title: str) -> str:
    """Henter materiale fra attributter eller søker i tekst."""
    for key in ["Materiale", "Stoff", "Material"]:
        if key in attributes:
            return attributes[key]
    text = (description + " " + title).lower()
    for kw in ["laminat", "eik", "bjørk", "bøk", "finer", "mdf",
                "mesh", "skinn", "stoff", "tekstil", "glass",
                "metall", "aluminium", "stål"]:
        if kw in text:
            return kw.capitalize()
    return ""


def _smart_title(raw: str, leaf_category: str, color: str, material: str) -> str:
    """
    Legger til leafkategori, farge og materiale hvis de ikke er i tittelen.
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


def _price_category(price_ex: float) -> str:
    if price_ex >= 20000:
        return "Over 20000"
    elif price_ex >= 5000:
        return "5000-20000"
    elif price_ex >= 1000:
        return "1000-5000"
    return "Under 1000"


# ─── Main Scrape ──────────────────────────────────────────────────────────────
def scrape_product(url: str) -> Optional[Product]:
    """Scraper én produktside og returnerer Product."""
    resp = _get(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    ld   = _extract_ld_json(soup)
    p    = Product(url=url)

    p.product_id  = _extract_product_id(soup, url)
    p.title_raw   = _extract_title(soup)
    p.description = _extract_description(soup)
    p.brand       = _extract_brand(soup)
    p.availability = _extract_availability(soup)
    p.quantity    = _extract_quantity(soup)

    p.breadcrumbs   = _extract_breadcrumbs(soup, ld, url)
    p.product_type  = " > ".join(p.breadcrumbs)
    p.leaf_category = p.breadcrumbs[-1] if p.breadcrumbs else ""
    p.google_category = _map_google_category(p.breadcrumbs, p.title_raw)

    p.image_main, p.images_extra = _extract_images(soup, ld)

    p.price_ex, p.price_incl, p.sale_ex, p.sale_incl = _extract_prices(soup)
    p.price_ex_str   = _fmt(p.price_ex)
    p.price_incl_str = _fmt(p.price_incl)
    p.sale_ex_str    = _fmt(p.sale_ex)
    p.sale_incl_str  = _fmt(p.sale_incl)

    p.attributes      = _extract_attributes(soup)
    p.color           = p.attributes.get("Hovedfarge", "")
    p.color_secondary = p.attributes.get("Sekundærfarge", "")
    p.material        = _detect_material(p.attributes, p.description, p.title_raw)
    p.mpn             = _extract_mpn(p.attributes, p.description)

    p.width      = _parse_dim(p.attributes.get("Bredde", ""))
    p.height     = _parse_dim(p.attributes.get("Høyde", ""))
    p.depth      = _parse_dim(p.attributes.get("Dybde", ""))
    p.seat_height = _parse_dim(p.attributes.get("Sittehøyde", ""))
    p.diameter   = _parse_dim(p.attributes.get("Diameter", ""))
    p.shipping_weight = _calc_shipping_weight(p.width, p.height, p.depth)

    p.condition  = _detect_condition(p.title_raw, url)
    p.title_seo  = _smart_title(p.title_raw, p.leaf_category, p.color, p.material)

    # Custom labels
    disc_pct = round((1 - p.sale_ex / p.price_ex) * 100) if p.sale_ex and p.price_ex else 0
    p.custom_label_0 = "Tilbud" if p.sale_ex else ""
    p.custom_label_1 = "Outlet" if disc_pct >= 30 else ""
    p.custom_label_2 = _price_category(p.price_ex)
    p.custom_label_3 = ""
    p.custom_label_4 = ""

    if not p.product_id:
        log.warning(f"Ingen produkt-ID for {url}")

    return p


def fetch_outlet_ids() -> set:
    """
    Henter alle produkt-IDer fra /outlet.
    Bruker requests direkte (ikke _get) for å unngå delay.
    """
    outlet_ids = set()
    page = 1
    while page <= 20:   # maks 20 sider som sikkerhet
        url = f"{BASE_URL}/outlet" if page == 1 else f"{BASE_URL}/outlet?page={page}"
        try:
            resp = SESSION.get(url, timeout=15)
            if resp.status_code != 200:
                break
        except Exception as e:
            log.error(f"Outlet fetch feilet: {e}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        found = 0
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            match = re.search(r"-(\d{4,6})\.html", href)
            if match and "?" in href:   # outlet-lenker har ?h= parameter
                outlet_ids.add(match.group(1))
                found += 1
        if not found:
            break
        next_page = soup.select_one("a.next, .pagination .next, [rel='next']")
        if not next_page:
            break
        page += 1
    log.info(f"Fant {len(outlet_ids)} outlet-produkter")
    return outlet_ids


def scrape_all(urls: list, max_products: int = 0) -> list:
    outlet_ids = fetch_outlet_ids()
    products = []
    total = len(urls) if not max_products else min(max_products, len(urls))
    for i, url in enumerate(urls[:total], 1):
        log.info(f"[{i}/{total}] {url}")
        p = scrape_product(url)
        if p:
            if p.product_id in outlet_ids:
                p.custom_label_1 = "Outlet"
            products.append(p)
        else:
            log.warning("  → Hoppet over")
    log.info(f"Scraped {len(products)} produkter")
    return products


# ─── Sitemap ──────────────────────────────────────────────────────────────────
def discover_product_urls(sitemap_url: str = SITEMAP_URL) -> list:
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


# ─── Feed Builder ─────────────────────────────────────────────────────────────
def build_feed(products: list) -> str:
    """
    Bygger én XML-feed som fungerer for både Google Merchant Center og Meta.
    Format: RSS 2.0 med g:-namespace (Google-standard).
    """
    ET.register_namespace("g", "http://base.google.com/ns/1.0")
    G   = "http://base.google.com/ns/1.0"
    rss = ET.Element("rss", {"version": "2.0"})
    ch  = ET.SubElement(rss, "channel")

    ET.SubElement(ch, "title").text       = "Movement Product Feed"
    ET.SubElement(ch, "link").text        = BASE_URL
    ET.SubElement(ch, "description").text = "Brukte kvalitetsmøbler – Movement AS"

    for p in products:
        item = ET.SubElement(ch, "item")

        # ── Kjernefelt ────────────────────────────────────────────────────────
        ET.SubElement(item, "{%s}id" % G).text          = p.product_id
        ET.SubElement(item, "{%s}title" % G).text       = p.title_seo
        ET.SubElement(item, "{%s}description" % G).text = p.description
        ET.SubElement(item, "{%s}link" % G).text        = p.url
        ET.SubElement(item, "{%s}brand" % G).text       = p.brand
        ET.SubElement(item, "{%s}condition" % G).text   = p.condition
        ET.SubElement(item, "{%s}availability" % G).text = p.availability

        # ── Identifikatorer ───────────────────────────────────────────────────
        if p.gtin:
            ET.SubElement(item, "{%s}gtin" % G).text = p.gtin
        if p.mpn:
            ET.SubElement(item, "{%s}mpn" % G).text = p.mpn
        if not p.gtin and not p.mpn:
            ET.SubElement(item, "{%s}identifier_exists" % G).text = "no"

        # ── Kategorier ────────────────────────────────────────────────────────
        ET.SubElement(item, "{%s}google_product_category" % G).text = p.google_category
        if p.product_type:
            ET.SubElement(item, "{%s}product_type" % G).text = p.product_type

        # ── Bilder ────────────────────────────────────────────────────────────
        if p.image_main:
            ET.SubElement(item, "{%s}image_link" % G).text = p.image_main
        for img in p.images_extra:
            ET.SubElement(item, "{%s}additional_image_link" % G).text = img

        # ── Priser eks. mva ───────────────────────────────────────────────────
        ET.SubElement(item, "{%s}price" % G).text      = p.price_ex_str
        ET.SubElement(item, "{%s}sale_price" % G).text = p.sale_ex_str if p.sale_ex_str else p.price_ex_str

        # ── Priser inkl. mva ──────────────────────────────────────────────────
        ET.SubElement(item, "price_incl_vat").text      = p.price_incl_str
        ET.SubElement(item, "sale_price_incl_vat").text = p.sale_incl_str if p.sale_incl_str else p.price_incl_str

        # ── Produktegenskaper ─────────────────────────────────────────────────
        if p.color:
            ET.SubElement(item, "{%s}color" % G).text = p.color
        if p.color_secondary:
            ET.SubElement(item, "{%s}color_secondary" % G).text = p.color_secondary
        if p.material:
            ET.SubElement(item, "{%s}material" % G).text = p.material

        # ── Dimensjoner ───────────────────────────────────────────────────────
        if p.width:
            ET.SubElement(item, "{%s}product_width" % G).text  = f"{int(p.width)} cm"
        if p.height:
            ET.SubElement(item, "{%s}product_height" % G).text = f"{int(p.height)} cm"
        if p.depth:
            ET.SubElement(item, "{%s}product_length" % G).text = f"{int(p.depth)} cm"

        # ── Fraktvekt ─────────────────────────────────────────────────────────
        if p.shipping_weight:
            sw = int(p.shipping_weight) if p.shipping_weight == int(p.shipping_weight) else p.shipping_weight
            ET.SubElement(item, "{%s}shipping_weight" % G).text = f"{sw} kg"

        # ── Lager ─────────────────────────────────────────────────────────────
        if p.quantity:
            ET.SubElement(item, "quantity").text = p.quantity

        # ── product_detail for ekstra dimensjoner ─────────────────────────────
        extra_details = {}
        if p.seat_height:
            extra_details["Sittehøyde"] = f"{int(p.seat_height)} cm"
        if p.diameter:
            extra_details["Diameter"] = f"{int(p.diameter)} cm"

        for attr_name, attr_val in extra_details.items():
            pd = ET.SubElement(item, "{%s}product_detail" % G)
            ET.SubElement(pd, "{%s}section_name" % G).text    = "Dimensjoner"
            ET.SubElement(pd, "{%s}attribute_name" % G).text  = attr_name
            ET.SubElement(pd, "{%s}attribute_value" % G).text = attr_val

        # Andre attributter i product_detail
        skip = {"Hovedfarge", "Sekundærfarge", "Materiale", "Stoff", "Material",
                "Bredde", "Høyde", "Dybde", "Sittehøyde", "Diameter"}
        for key, val in p.attributes.items():
            if key not in skip:
                pd = ET.SubElement(item, "{%s}product_detail" % G)
                ET.SubElement(pd, "{%s}section_name" % G).text    = "Spesifikasjoner"
                ET.SubElement(pd, "{%s}attribute_name" % G).text  = key
                ET.SubElement(pd, "{%s}attribute_value" % G).text = val

        # ── Custom labels ─────────────────────────────────────────────────────
        ET.SubElement(item, "{%s}custom_label_0" % G).text = p.custom_label_0
        ET.SubElement(item, "{%s}custom_label_1" % G).text = p.custom_label_1
        ET.SubElement(item, "{%s}custom_label_2" % G).text = p.custom_label_2
        ET.SubElement(item, "{%s}custom_label_3" % G).text = p.custom_label_3
        ET.SubElement(item, "{%s}custom_label_4" % G).text = p.custom_label_4

    return _prettify(rss)


def _prettify(root) -> str:
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

    feed_xml = build_feed(products)
    feed_path = os.path.join(out_dir, "feed_google.xml")

    with open(feed_path, "w", encoding="utf-8") as f:
        f.write(feed_xml)

    log.info(f"✅ Feed → {feed_path}  ({len(products)} produkter)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="movement.as feed scraper v3")
    parser.add_argument("--test",       action="store_true")
    parser.add_argument("--test-limit", type=int, default=5)
    args = parser.parse_args()
    main(test_mode=args.test, test_limit=args.test_limit)
