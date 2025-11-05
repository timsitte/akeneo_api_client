#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import tempfile
import mimetypes
import re
from typing import Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

from logzero import logger

# Try importing client; if running from repo, add repo root to sys.path
try:
    from akeneo_api_client.client import Client
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from akeneo_api_client.client import Client


ASSET_FAMILY_CODE = "product_images"
MAIN_ATTRIBUTE = "product_image_main"
OTHER_ATTRIBUTE = "product_images"
DEFAULT_LABEL_LOCALE = "de_DE"


class AkeneoHttp:
    def __init__(self, client: Client, base_url: str):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.session = client._session

    def _url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def get(self, path: str, params: Optional[Dict] = None) -> requests.Response:
        return self.session.get(self._url(path), params=params)

    def post(self, path: str, data=None, json_data=None, files=None, headers: Optional[Dict] = None) -> requests.Response:
        url = self._url(path)
        # For multipart we should not send application/json default header.
        # Use direct requests.post with auth to avoid inheriting session default headers.
        if files:
            # Ensure we don't pass Content-Type explicitly; requests will set proper multipart boundary
            return requests.post(url, data=data, files=files, headers=headers, auth=self.session.auth)
        if json_data is not None:
            return self.session.post(url, data=json.dumps(json_data, separators=(',', ':')))
        return self.session.post(url, data=data, headers=headers)

    def patch(self, path: str, json_data=None, headers: Optional[Dict] = None) -> requests.Response:
        url = self._url(path)
        if json_data is not None:
            return self.session.patch(url, data=json.dumps(json_data, separators=(',', ':')), headers=headers)
        return self.session.patch(url, headers=headers)


def load_input_csv(path: str) -> List[Dict]:
    """Load CSV data and group by SKU to create product entries with images"""
    import csv
    
    products = {}
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            sku = row['sku']
            product_title = row['product_title']
            image_url = row['image_url']
            image_seo_filename = row['image_seo_filename']
            image_position = row['image_position']
            
            # Initialize product if not seen before
            if sku not in products:
                products[sku] = {
                    'sku': sku,
                    'name': product_title,
                    'images': []
                }
            
            # Add image if it has data
            if image_url and image_seo_filename and image_position:
                products[sku]['images'].append({
                    'url': image_url,
                    'seo_filename': image_seo_filename,
                    'position': int(image_position)
                })
    
    # Convert to list and sort images by position
    result = []
    for sku, product in products.items():
        # Sort images by position
        product['images'].sort(key=lambda x: x.get('position', 9999))
        result.append(product)
    
    return result


def sanitize_asset_code(filename: str) -> str:
    base = os.path.splitext(os.path.basename(filename))[0]
    # Replace any non-alphanumeric character with underscore (dashes included)
    code = re.sub(r'[^a-zA-Z0-9]+', '_', base.lower()).strip('_')
    if not code:
        code = f"asset_{int(time.time())}"
    return code[:255]


def find_product_by_sku(http: AkeneoHttp, sku: str) -> Tuple[List[Dict], Optional[str]]:
    search = {"identifier": [{"operator": "=", "value": sku}]}
    params = {"search": json.dumps(search)}
    r = http.get("/api/rest/v1/products", params=params)
    if r.status_code != 200:
        return [], f"Product search failed ({r.status_code}): {r.text}"
    data = r.json()
    items = data.get("_embedded", {}).get("items", [])
    return items, None


def find_asset_by_code(http: AkeneoHttp, family_code: str, asset_code: str) -> Tuple[Optional[Dict], Optional[str]]:
    search = {"code": [{"operator": "IN", "value": [asset_code]}]}
    params = {"search": json.dumps(search)}
    r = http.get(f"/api/rest/v1/asset-families/{family_code}/assets", params=params)
    if r.status_code != 200:
        return None, f"Asset search failed ({r.status_code}): {r.text}"
    data = r.json()
    items = data.get("_embedded", {}).get("items", [])
    return (items[0] if items else None), None


def download_image(url: str, timeout: int = 20, retries: int = 2) -> Tuple[Optional[bytes], Optional[str]]:
    last_err = None
    for _ in range(retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as resp:
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code} downloading image"
                    continue
                chunks = []
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)
                return b"".join(chunks), None
        except Exception as e:
            last_err = str(e)
    return None, last_err or "Unknown download error"


def upload_asset_media(http: AkeneoHttp, file_bytes: bytes, filename: str) -> Tuple[Optional[str], Optional[str]]:
    print(f"Uploading media for Asset derived from '{filename}'")
    guessed_type, _ = mimetypes.guess_type(filename)
    content_type = guessed_type or 'application/octet-stream'
    files = {"file": (filename, file_bytes, content_type)}
    r = http.post("/api/rest/v1/asset-media-files", files=files, headers={"Accept": "*/*"})
    # Debug limited request headers if failure to help diagnose Content-Type issues
    if r.status_code not in [201, 202]:
        try:
            sent_ct = r.request.headers.get('Content-Type')
            sent_acc = r.request.headers.get('Accept')
            ua = r.request.headers.get('User-Agent')
            print(f"Debug upload headers -> Content-Type: {sent_ct}; Accept: {sent_acc}; User-Agent: {ua}")
        except Exception:
            pass
        return None, f"Media upload failed ({r.status_code}): {r.text}"
    media_code = r.headers.get('asset-media-file-code')
    if not media_code:
        return None, "Missing asset-media-file-code in response headers"
    print(f"Success: (id:{media_code})")
    return media_code, None


def upsert_asset(http: AkeneoHttp, family_code: str, asset_code: str, label: str, media_file_code: str) -> Optional[str]:
    print(f"Creating Asset {asset_code}")
    payload = {
        "code": asset_code,
        "values": {
            "label": [
                {"locale": DEFAULT_LABEL_LOCALE, "channel": None, "data": label}
            ],
            "media": [
                {"locale": None, "channel": None, "data": media_file_code}
            ],
        },
    }
    r = http.patch(f"/api/rest/v1/asset-families/{family_code}/assets/{asset_code}", json_data=payload)
    if r.status_code not in [201, 204]:
        return f"Asset upsert failed ({r.status_code}): {r.text}"
    print(f"Success: (id:{asset_code})")
    return None


def update_product_assets(http: AkeneoHttp, sku: str, main_code: Optional[str], other_codes: List[str]) -> Optional[str]:
    print(f"Updating Product {sku}")
    values = {}
    if main_code:
        values[MAIN_ATTRIBUTE] = [{"locale": None, "scope": None, "data": [main_code]}]
    values[OTHER_ATTRIBUTE] = [{"locale": None, "scope": None, "data": other_codes}]
    payload = {"values": values}
    r = http.patch(f"/api/rest/v1/products/{sku}", json_data=payload)
    if r.status_code not in [201, 204]:
        return f"Product update failed ({r.status_code}): {r.text}"
    print(f"Success: (id:{sku})")
    return None


def process_product(http: AkeneoHttp, product_entry: Dict, stats: Dict) -> None:
    sku = product_entry.get("sku")
    images = product_entry.get("images", []) or []
    if not sku:
        stats["other"].append((None, "Missing sku"))
        return
    items, err = find_product_by_sku(http, sku)
    if err:
        logger.error(f"SKU {sku}: {err}")
        stats["other"].append((sku, err))
        return
    if len(items) == 0:
        logger.error(f"SKU {sku}: not found")
        stats["not_found"].append(sku)
        print(f"Skipping Product {sku}: not found by SKU")
        return
    if len(items) > 1:
        logger.error(f"SKU {sku}: multiple products found")
        stats["ambiguous"].append(sku)
        print(f"Skipping Product {sku}: multiple products found for SKU")
        return

    # Sort images by position ascending; position==1 is main
    valid_images = []
    for img in images:
        if not img or not img.get("url") or not img.get("seo_filename"):
            stats["other"].append((sku, "Invalid image entry"))
            continue
        valid_images.append(img)
    valid_images.sort(key=lambda x: x.get("position", 9999))
    if not valid_images:
        logger.info(f"SKU {sku}: no valid images to process")
        stats["updated"].append(sku)  # nothing to link but processed
        print(f"Skipping Product {sku}: no valid images to process")
        return

    asset_codes_in_order: List[str] = []
    for img in valid_images:
        filename = img["seo_filename"]
        asset_code = sanitize_asset_code(filename)
        existing, search_err = find_asset_by_code(http, ASSET_FAMILY_CODE, asset_code)
        if existing:
            logger.info(f"Asset exists: {asset_code}")
            print(f"Skipping Asset {asset_code}: already exists")
            asset_codes_in_order.append(asset_code)
            continue
        if search_err:
            logger.error(f"SKU {sku}: {search_err}")
            stats["other"].append((sku, search_err))
            print(f"Skipping Asset {asset_code}: search error: {search_err}")
            continue
        # Need to create asset
        file_bytes, dl_err = download_image(img["url"])
        if dl_err:
            logger.error(f"SKU {sku}: download error for {img['url']}: {dl_err}")
            stats["other"].append((sku, f"download error: {dl_err}"))
            print(f"Skipping Asset {asset_code}: download error: {dl_err}")
            continue
        media_file_code, upload_err = upload_asset_media(http, file_bytes, filename)
        if upload_err:
            logger.error(f"SKU {sku}: media upload error: {upload_err}")
            stats["other"].append((sku, f"media upload error: {upload_err}"))
            print(f"Fail: media upload for {asset_code}: {upload_err}")
            continue
        upsert_err = upsert_asset(http, ASSET_FAMILY_CODE, asset_code, filename, media_file_code)
        if upsert_err:
            logger.error(f"SKU {sku}: asset upsert error: {upsert_err}")
            stats["other"].append((sku, f"asset upsert error: {upsert_err}"))
            print(f"Fail: creating Asset {asset_code}: {upsert_err}")
            continue
        logger.info(f"Created asset: {asset_code}")
        asset_codes_in_order.append(asset_code)

    if not asset_codes_in_order:
        logger.error(f"SKU {sku}: no assets created or found; skipping product update")
        print(f"Skipping Product {sku}: no assets to link")
        stats["other"].append((sku, "no assets to link"))
        return

    main_code: Optional[str] = None
    other_codes: List[str] = []
    # Map by position semantics: first item after sorting has smallest position
    if valid_images and valid_images[0].get("position") == 1:
        main_code = asset_codes_in_order[0]
        other_codes = asset_codes_in_order[1:]
    else:
        other_codes = asset_codes_in_order

    err2 = update_product_assets(http, sku, main_code, other_codes)
    if err2:
        logger.error(f"SKU {sku}: product update error: {err2}")
        stats["other"].append((sku, f"product update error: {err2}"))
        return

    stats["updated"].append(sku)


def print_summary(stats: Dict):
    total = stats.get("total", 0)
    updated = len(stats.get("updated", []))
    not_found = len(stats.get("not_found", [])) + len(stats.get("ambiguous", []))
    other = len(stats.get("other", []))
    print(f"total products processed: {total}; Successfully added assets to {updated} products; Products Not found: {not_found}; Other Issues: {other}")
    if stats.get("not_found"):
        print("Not found SKUs:")
        for sku in stats["not_found"]:
            print(f" - {sku}")
    if stats.get("ambiguous"):
        print("Ambiguous SKUs:")
        for sku in stats["ambiguous"]:
            print(f" - {sku}")
    if stats.get("other"):
        print("Other issues:")
        for sku, reason in stats["other"]:
            print(f" - {sku}: {reason}")


def main():
    input_path = os.environ.get("INPUT_CSV_PATH") or os.path.join(os.path.dirname(__file__), "export_assets_to_akeneo.csv")

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET") or os.environ.get("AKENEO_CLIENT_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

    missing = [k for k, v in {
        'AKENEO_CLIENT_ID': AKENEO_CLIENT_ID,
        'AKENEO_SECRET': AKENEO_SECRET,
        'AKENEO_USERNAME': AKENEO_USERNAME,
        'AKENEO_PASSWORD': AKENEO_PASSWORD,
        'AKENEO_BASE_URL': AKENEO_BASE_URL,
    }.items() if not v]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        sys.exit(2)

    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID, AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)
    http = AkeneoHttp(akeneo, AKENEO_BASE_URL)

    try:
        products = load_input_csv(input_path)
    except Exception as e:
        print(f"Failed to load input file {input_path}: {e}")
        sys.exit(2)

    stats = {"total": len(products), "updated": [], "not_found": [], "ambiguous": [], "other": []}
    for entry in products:
        try:
            process_product(http, entry, stats)
        except Exception as e:
            sku = entry.get('sku') if isinstance(entry, dict) else None
            logger.exception("Unhandled error")
            stats["other"].append((sku, f"Unhandled: {e}"))

    print_summary(stats)


if __name__ == "__main__":
    main()


