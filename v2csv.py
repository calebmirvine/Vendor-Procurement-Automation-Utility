#!/opt/homebrew/bin/python3
import asyncio
import httpx
from datetime import datetime
import pandas as pd
from urllib.parse import quote
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === Config ===
if getattr(sys, 'frozen', False):
    # If the script is run as a bundled executable (e.g., via PyInstaller)
    SCRIPT_DIR = os.path.dirname(sys.executable)
else:
    # If run as a normal Python script
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

CSV_PATH = os.path.join(SCRIPT_DIR, 'export.csv')
PRODUCTS_TXT_PATH = os.path.join(SCRIPT_DIR, 'products.txt')

REMOTE_BASE_URL = os.getenv("REMOTE_BASE_URL")
TOKEN_URL = REMOTE_BASE_URL + os.getenv("REMOTE_TOKEN_PATH", "/identity/connect/token")
PRICE_API_URL = REMOTE_BASE_URL + os.getenv("REMOTE_PRICE_API_PATH", "/api/v1/realtimepricing")
INVENTORY_API_URL = REMOTE_BASE_URL + os.getenv("REMOTE_INVENTORY_API_PATH", "/api/v1/realtimeinventory")
CATALOG_API_URL = REMOTE_BASE_URL + os.getenv("REMOTE_CATALOG_API_PATH", "/api/v1/catalogPages")
PRODUCT_API_URL = REMOTE_BASE_URL + os.getenv("REMOTE_PRODUCT_API_PATH", "/api/v2/products")

# Credentials from environment variables
CLIENT_AUTH = os.getenv("CLIENT_AUTH")
USERNAME = os.getenv("REMOTE_USERNAME")
PASSWORD = os.getenv("REMOTE_PASSWORD")

CONCURRENCY_LIMIT = 15 # Adjust based on API limits

async def get_access_token(client):
    token_data = {
        "grant_type": "password",
        "username": USERNAME,
        "password": PASSWORD,
        "scope": "iscapi offline_access"
    }
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": CLIENT_AUTH
    }
    resp = await client.post(TOKEN_URL, data=token_data, headers=token_headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

async def fetch_product_data(client, path, headers, semaphore, current_date, existing_pairs=None):
    async with semaphore:
        path = path.strip()
        if not path:
            return None
            
        clean_path = path.split('?')[0]
        if clean_path.startswith(REMOTE_BASE_URL):
            clean_path = clean_path[len(REMOTE_BASE_URL):]

        full_url = f"{CATALOG_API_URL}?path={quote(clean_path, safe='/:?=')}"
        
        try:
            # 1. Catalog API (essential for product_id and name)
            catalog_resp = await client.get(full_url, headers=headers)
            if catalog_resp.status_code != 200:
                print(f"Warning: catalog API failed for path {path}, status {catalog_resp.status_code}")
                return None

            catalog_data = catalog_resp.json()
            product_id = catalog_data.get("productId")
            product_name = catalog_data.get("productName") # This is the "Product Code"
            title = catalog_data.get("title")

            if not product_id:
                print(f"Warning: no product ID found for path {path}")
                return None

            # === Deduplication Check by (Product Code, Date) ===
            if existing_pairs is not None:
                if (product_name, current_date) in existing_pairs:
                    return {"_skip": True, "Product Code": product_name}

            # Extract categories from breadCrumbs
            bread_crumbs = catalog_data.get("breadCrumbs", [])
            category_cols = {}
            if len(bread_crumbs) > 2:
                middle_crumbs = bread_crumbs[1:-1]
                for idx, bc in enumerate(middle_crumbs, start=1):
                    category_cols[f"category_{idx}"] = bc.get("text", "")

            # 2. Product API for Vendor SKU (manufacturerItem)
            vendorSKU = None
            product_url = f"{PRODUCT_API_URL}?productNumbers={product_name}"
            product_resp = await client.get(product_url, headers=headers)
            if product_resp.status_code == 200:
                product_data = product_resp.json()
                product_results = product_data.get("products", [])
                if product_results:
                    vendorSKU = product_results[0].get("manufacturerItem")

            # 3. Price API
            unit_list_price_display = None
            per_units = None
            price_payload = {"productPriceParameters": [{"productId": product_id, "unitOfMeasure": "", "qtyOrdered": 1}]}
            price_resp = await client.post(PRICE_API_URL, headers=headers, json=price_payload)
            if price_resp.status_code == 200:
                price_data = price_resp.json()
                price_results = price_data.get("realTimePricingResults", [])
                if price_results:
                    result = price_results[0]
                    unit_list_price_display = result.get("unitListPriceDisplay")
                    additional_price = result.get("additionalResults") or {}
                    per_units = additional_price.get("unitOfMeasure")

            # 4. Inventory API
            status = None
            inv_payload = {"productIds": [product_id]}
            inv_resp = await client.post(INVENTORY_API_URL, headers=headers, json=inv_payload)
            if inv_resp.status_code == 200:
                inv_data = inv_resp.json()
                inv_results = inv_data.get("realTimeInventoryResults", [])
                if inv_results:
                    result = inv_results[0]
                    additional_inv = result.get("additionalResults") or {}
                    status = additional_inv.get("subMessageType")

            # Final Row
            product_link = f"{REMOTE_BASE_URL}{clean_path}"
            
            row = {
                "Date Listed": current_date,
                "Link": product_link,
                "Hidden ID": product_id,
                "Product Code": product_name,
                "Vendor SKU": vendorSKU,
                "Title": title,
                "List Price": unit_list_price_display,
                "Per": per_units,
                "Status": status,
            }
            row.update(category_cols)
            return row

        except Exception as e:
            print(f"Error processing path {path}: {e}")
            return None

async def main():
    current_date = datetime.today().strftime("%m/%d/%Y")

    # Verify environment variables
    if not REMOTE_BASE_URL or not CLIENT_AUTH or not USERNAME or not PASSWORD:
        print("Error: Required environment variables (REMOTE_BASE_URL, CLIENT_AUTH, REMOTE_USERNAME, REMOTE_PASSWORD) are not set in .env")
        return

    # === Step 0: User Prompt at the Beginning ===
    choice = 'O'
    existing_df = None
    existing_pairs = None # Set of (Product Code, Date)

    if os.path.exists(CSV_PATH):
        print(f"\nThe file '{CSV_PATH}' already exists.")
        while True:
            choice = input("Do you want to (A)ppend to existing file or (O)verwrite it? [A/O]: ").strip().upper()
            if choice in ['A', 'O']:
                break
            print("Invalid choice. Please enter 'A' for append or 'O' for overwrite.")
        
        if choice == 'A':
            try:
                existing_df = pd.read_csv(CSV_PATH)
                if 'Product Code' in existing_df.columns and 'Date Listed' in existing_df.columns:
                    # Create a set of (Product Code, Date Listed) to detect duplicates for the SAME day
                    existing_pairs = set(zip(existing_df['Product Code'], existing_df['Date Listed']))
                    print(f"Loaded existing data. Will skip products already entered for {current_date}.")
                else:
                    print("Warning: CSV missing required columns for deduplication. Skipping pre-check.")
            except Exception as e:
                print(f"Error reading existing CSV: {e}. Defaulting to Overwrite.")
                choice = 'O'

    # === Step 1: Load Paths ===
    if not os.path.exists(PRODUCTS_TXT_PATH):
        print(f"Error: {PRODUCTS_TXT_PATH} not found.")
        return

    with open(PRODUCTS_TXT_PATH, 'r') as f:
        product_paths = [line.strip() for line in f if line.strip()]

    if not product_paths:
        print("No paths found in products.txt")
        return

    print(f"Loaded {len(product_paths)} product paths. Starting async processing...")

    # === Step 2: Fetch Data ===
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            access_token = await get_access_token(client)
        except Exception as e:
            print(f"Error getting access token: {e}")
            return

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [fetch_product_data(client, path, headers, semaphore, current_date, existing_pairs) for path in product_paths]
        
        results = await asyncio.gather(*tasks)
        
        # Filter out skipped or failed results
        new_data = [r for r in results if r is not None and not r.get("_skip")]
        skipped_count = len([r for r in results if r is not None and r.get("_skip")])
        
        if skipped_count:
            print(f"Skipped {skipped_count} products already present in the CSV for today's date ({current_date}).")

    if not new_data and choice == 'A':
        print("No new products found to add for today.")
        return
    elif not new_data and choice == 'O':
        print("No data collected to write.")
        return

    # === Step 3: Write CSV ===
    df_new = pd.DataFrame(new_data)

    if choice == 'A':
        df_final = pd.concat([existing_df, df_new], ignore_index=True)
        # Final safety: drop duplicates by Product Code and Date
        df_final.drop_duplicates(subset=['Product Code', 'Date Listed'], keep='last', inplace=True)
        print(f"Adding {len(df_new)} new products to existing CSV...")
    else:
        # For overwrite, still drop duplicates within the newly fetched data
        df_final = df_new.drop_duplicates(subset=['Product Code', 'Date Listed'], keep='last')
        print(f"Writing {len(df_final)} unique products to CSV...")

    df_final.to_csv(CSV_PATH, index=False)
    print(f"API data written to {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())