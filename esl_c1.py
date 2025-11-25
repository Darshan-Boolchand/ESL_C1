from flask import Flask, request, jsonify, send_file
import pandas as pd
import requests
import traceback
from requests.auth import HTTPBasicAuth
import urllib3
import os
import re

app = Flask(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Hanshow API Configuration ===
API_BASE = "https://boolchand.slscanada.ca:9001"
USERNAME = "Vp6697S3ydmGo4t5gE"
PASSWORD = "zHyHN8jtABzWHQ68%v"
CUSTOMER_CODE = "boolchand"
STORE_CODE = "C1"  # ✅ Curaçao store code

# === Tax configuration based on Product Class ===
NINE_PERCENT_CLASSES = [
    "APPLE IPHONES",
    "OTHER PHONES",
    "SAMSUNG PHONES",
    "GAMING TITLES"
]

# === Clean Excel string helper ===
_illegal_unichrs = [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F), (0x7F, 0x9F)]
_illegal_ranges = ["%s-%s" % (chr(low), chr(high)) for (low, high) in _illegal_unichrs]
_illegal_re = re.compile(u'[%s]' % u''.join(_illegal_ranges))

def clean_excel_string(value):
    if isinstance(value, str):
        return _illegal_re.sub("", value)
    return value

# === Get bearer token from Hanshow ===
def get_token():
    response = requests.post(
        f"{API_BASE}/proxy/token",
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        verify=False
    )
    response.raise_for_status()
    return response.json()["access_token"]

# === Send items in batches of 1000, with token retry ===
def update_esl(items):
    def send_request(batch, token):
        url = f"{API_BASE}/proxy/integration/{CUSTOMER_CODE}/{STORE_CODE}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "customerStoreCode": CUSTOMER_CODE,
            "storeCode": STORE_CODE,
            "batchNo": f"batch-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}",
            "items": batch
        }
        return requests.post(url, headers=headers, json=payload, verify=False)

    responses = []
    token = get_token()
    batch_size = 1000

    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        print(f"📦 Sending batch {i//batch_size + 1} with {len(batch)} items")

        response = send_request(batch, token)

        # === Retry once on 401 Unauthorized ===
        if response.status_code == 401:
            print("🔁 Token may have expired, retrying with new token...")
            token = get_token()
            response = send_request(batch, token)

        try:
            res_json = response.json()
        except:
            res_json = {"error": "Failed to decode JSON", "text": response.text}

        print("📡 API Response:", response.status_code, response.text)
        responses.append({
            "batch": i//batch_size + 1,
            "status": response.status_code,
            "response": res_json
        })

    return 200, {"batches_sent": len(responses), "results": responses}

@app.route('/')
def home():
    return '✅ Curaçao ESL Update Service is Running'

@app.route('/convert', methods=['POST'])
def convert_excel():
    try:
        if 'file' not in request.files:
            return "No file uploaded", 400

        file = request.files['file']
        if file.filename == '':
            return "Empty filename", 400

        # === Support .xls or .xlsx, skip first line ===
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext in ['.xls', '.xlsx']:
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'
            df = pd.read_excel(file, skiprows=1, dtype=str, engine=engine)
        else:
            return "Unsupported file format. Please upload .xls or .xlsx", 400

        print("🧾 Columns in uploaded Excel:", df.columns.tolist())

        items = []
        for _, row in df.iterrows():
            try:
                sku = str(row['Product ID']).strip()
                short_name = str(row['Product Code']).strip()
                name = str(row['Description']).strip()
                brand = str(row['Brand Name']).strip()
                retail = float(row['Current Retail'])

                # === Determine TAX RATE (based on Product Class) ===
                product_class = str(row.get('Product Class', '')).strip().upper()
                if product_class in NINE_PERCENT_CLASSES:
                    tax_rate = 0.09
                else:
                    tax_rate = 0.06

                # === PRICE CALCULATIONS ===
                price1 = int(round(retail * (1 + tax_rate)))  # with tax
                price2 = round(price1 / 1.8)

                # === MSRP → price3 ===
                try:
                    msrp_raw = row['MSRP']
                    if pd.notna(msrp_raw):
                        msrp_value = float(msrp_raw)
                        if not msrp_value.is_integer():
                            price3 = 0
                        else:
                            msrp_int = int(msrp_value)
                            if msrp_int == price1 or msrp_int < price1:
                                price3 = 0
                            else:
                                price3 = msrp_int
                    else:
                        price3 = 0
                except:
                    price3 = 0

                # === STOCK ===
                stock_column = next(
                    (col for col in row.index if col.strip().lower().replace(" ", "") in [
                        "qtyonhand", "quantityonhand", "onhand", "stock"
                    ]),
                    None
                )
                stock = int(float(row[stock_column])) if stock_column and pd.notna(row[stock_column]) else 0

                item = {
                    "IIS_COMMAND": "UPDATE",
                    "sku": sku,
                    "itemShortName": short_name,
                    "itemName": name,
                    "manufacturer": brand,
                    "price1": price1,
                    "price2": price2,
                    "price3": price3,
                    "inventory": stock
                }

                cleaned_item = {k: clean_excel_string(v) for k, v in item.items()}
                items.append(cleaned_item)
            except Exception as row_error:
                print(f"⚠️ Skipping row: {row_error}")

        if not items:
            return "No valid items found.", 400

        # === Save to mapped.xlsx (cleaned) ===
        df_mapped = pd.DataFrame(items)
        df_mapped = df_mapped.applymap(clean_excel_string)
        df_mapped.to_excel("mapped.xlsx", index=False)
        print("💾 mapped.xlsx saved locally")

        # === Send to Hanshow API ===
        status, result = update_esl(items)
        return jsonify({
            "status": status,
            "total_items": len(items),
            "result": result,
            "local_file_url": "/download_last_xlsx"
        })

    except Exception as e:
        print("❌ ERROR IN /convert")
        traceback.print_exc()
        return f"Error: {str(e)}", 500

@app.route('/download_last_xlsx', methods=['GET'])
def download_last_xlsx():
    file_path = "mapped.xlsx"
    if not os.path.exists(file_path):
        return "mapped.xlsx not found", 404
    return send_file(
        file_path,
        download_name="mapped.xlsx",
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    app.run()


