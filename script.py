#!/opt/homebrew/bin/python3
import requests
from datetime import datetime
import pandas as pd
from urllib.parse import quote
import os
from dotenv import load_dotenv

load_dotenv()

# === Configuration ===
INPUT_CSV = './data/input_product_list.csv'
OUTPUT_CSV = './data/procurement_report.csv'

# Base URL structure (masked)
BASE_DOMAIN = "https://api.vendor-portal.com"
TOKEN_URL = f"{BASE_DOMAIN}/identity/connect/token"
PRICE_API_URL = f"{BASE_DOMAIN}/api/v1/realtimepricing"
INVENTORY_API_URL = f"{BASE_DOMAIN}/api/v1/realtimeinventory"
CATALOG_API_URL = f"{BASE_DOMAIN}/api/v1/catalogPages"
PRODUCT_API_URL = f"{BASE_DOMAIN}/api/v2/products"

CLIENT_AUTH = os.getenv("VENDOR_API_TOKEN")
USERNAME = os.getenv("VENDOR_USERNAME")
PASSWORD = os.getenv("VENDOR_PASSWORD")

# === Step 1: Load Targets ===
try:
    # Read the input CSV
    input_df = pd.read_csv(INPUT_CSV)

    # Check if the required column exists (e.g., "Product Link")
    if 'Product Link' not in input_df.columns:
        raise ValueError("Input CSV must contain a 'Product Link' column.")

    # Convert that column to a list to loop through
    # dropna() ensures we don't try to scan empty rows
    product_paths = input_df['Product Link'].dropna().tolist()

    print(f"Loaded {len(product_paths)} products from {INPUT_CSV}")

except FileNotFoundError:
    print(f"Error: Could not find input file at {INPUT_CSV}")
    exit()
except Exception as e:
    print(f"Error reading input CSV: {e}")
    exit()

# === Step 2: Loop over paths ===
all_rows = []

# === Step 1: Authentication ===
# Demonstrates handling OAuth2 / Token-based auth
token_data = {
    "grant_type": "password",
    "username": USERNAME,
    "password": PASSWORD,
    "scope": "api_access offline_access"
}
token_headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": CLIENT_AUTH
}

try:
    tokens = requests.post(TOKEN_URL, data=token_data, headers=token_headers).json()
    access_token = tokens["access_token"]
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()

headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

all_rows = []

# === Step 3: ETL Process (Extract, Transform, Load) ===
print(f"Processing {len(product_paths)} items...")

for path in product_paths:
    clean_path = path.split('?')[0]

    # Logic to clean URL prefixes if necessary
    if clean_path.startswith(BASE_DOMAIN):
        clean_path = clean_path[len(BASE_DOMAIN):]

    full_url = f"{CATALOG_API_URL}?path={quote(clean_path, safe='/:?=')}"

    # 1. Fetch Catalog Metadata
    catalog_resp = requests.get(full_url, headers=headers)
    if catalog_resp.status_code != 200:
        continue

    catalog_data = catalog_resp.json()
    product_id = catalog_data.get("productId")
    product_name = catalog_data.get("productName")

    if not product_id: continue

    # 2. Fetch Manufacturer SKU
    product_url = f"{PRODUCT_API_URL}?productNumbers={product_name}"
    product_resp = requests.get(product_url, headers=headers)
    vendorSKU = None
    if product_resp.status_code == 200:
        try:
            vendorSKU = product_resp.json().get("products", [])[0].get("manufacturerItem")
        except IndexError:
            pass

    # 3. Fetch Real-time Pricing
    price_payload = {"productPriceParameters": [{"productId": product_id, "qtyOrdered": 1}]}
    price_resp = requests.post(PRICE_API_URL, headers=headers, json=price_payload)
    unit_list_price = None
    if price_resp.status_code == 200:
        results = price_resp.json().get("realTimePricingResults", [])
        if results:
            unit_list_price = results[0].get("unitListPriceDisplay")

    # 4. Fetch Real-time Inventory
    inv_payload = {"productIds": [product_id]}
    inv_resp = requests.post(INVENTORY_API_URL, headers=headers, json=inv_payload)
    stock_status = "Unknown"
    if inv_resp.status_code == 200:
        results = inv_resp.json().get("realTimeInventoryResults", [])
        if results:
            stock_status = results[0].get("additionalResults", {}).get("subMessageType")

    # Aggregate Data
    row = {
        "Date Scanned": datetime.today().strftime("%Y-%m-%d"),
        "Product Name": product_name,
        "Vendor SKU": vendorSKU,
        "List Price": unit_list_price,
        "Stock Status": stock_status,
        "Source Link": f"{BASE_DOMAIN}{clean_path}"
    }
    all_rows.append(row)

# === Step 4: Export Data ===
df = pd.DataFrame(all_rows)

if os.path.exists(OUTPUT_CSV):
    # Logic to handle appending vs overwriting
    df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
    print(f"Appended data to {OUTPUT_CSV}")
else:
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Created new report at {OUTPUT_CSV}")