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

# === Hanshow API Configuration (set as Render environment variables) ===
API_BASE = "https://boolchand.slscanada.ca:9001"
USERNAME = os.environ["HANSHOW_USER"]
PASSWORD = os.environ["HANSHOW_PASS"]
CUSTOMER_CODE = "boolchand"
STORE_CODE = "C1"  # Curacao store code

# === Curacao Google Sheet (Apps Script web app) ===
SHEET_URL = os.environ["APPS_SCRIPT_URL"]
SHEET_TOKEN = os.environ["SHEET_TOKEN"]

# === Tax configuration based on Product Class (Curacao) ===
NINE_PERCENT_CLASSES = [
    "APPLE IPHONES",
    "OTHER PHONES",
    "SAMSUNG PHONES",
    "GAMING TITLES",
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
        verify=False,
    )
    response.raise_for_status()
    return response.json()["access_token"]


# === Google Sheet helpers (never stop the label update) ===
def _sheet_json(r):
    try:
        return r.json()
    except Exception:
        print(f"SHEET ERROR: not JSON. HTTP {r.status_code}. Starts with: {r.text[:300]!r}")
        return None


def sheet_sync(items):
    """Send today's export to the sheet: updates price/stock in Master, adds new SKUs."""
    try:
        r = requests.post(SHEET_URL, json={"token": SHEET_TOKEN, "items": items}, timeout=120)
        data = _sheet_json(r)
        if data is None:
            return {"ok": False, "error": "sheet did not return JSON, see Render logs"}
        print("SHEET SYNC:", data)
        return data
    except Exception as e:
        print("SHEET SYNC FAILED:", e)
        return {"ok": False, "error": str(e)}


def sheet_master():
    """Read the Master tab (all Hanshow fields keyed by field name). Returns {} if unreachable."""
    try:
        r = requests.get(SHEET_URL, params={"token": SHEET_TOKEN, "action": "master"}, timeout=120)
        data = _sheet_json(r)
        if not data or "items" not in data:
            print("SHEET MASTER ERROR:", data)
            return {}
        return {str(i["sku"]): i for i in data["items"] if i.get("sku")}
    except Exception as e:
        print("SHEET MASTER FAILED:", e)
        return {}


# === Send items in batches of 1000, with token retry ===
def update_esl(items):
    def send_request(batch, token):
        url = f"{API_BASE}/proxy/integration/{CUSTOMER_CODE}/{STORE_CODE}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "customerStoreCode": CUSTOMER_CODE,
            "storeCode": STORE_CODE,
            "batchNo": f"batch-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}",
            "items": batch,
        }
        return requests.post(url, headers=headers, json=payload, verify=False)

    responses = []
    token = get_token()
    batch_size = 1000

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        print(f"Sending batch {i // batch_size + 1} with {len(batch)} items")

        response = send_request(batch, token)

        if response.status_code == 401:
            print("Token may have expired, retrying with new token...")
            token = get_token()
            response = send_request(batch, token)

        try:
            res_json = response.json()
        except Exception:
            res_json = {"error": "Failed to decode JSON", "text": response.text}

        print("API Response:", response.status_code, response.text)
        responses.append({
            "batch": i // batch_size + 1,
            "status": response.status_code,
            "response": res_json,
        })

    return 200, {"batches_sent": len(responses), "results": responses}


@app.route('/')
def home():
    return 'Curacao ESL Update Service is Running'


@app.route('/convert', methods=['POST'])
def convert_excel():
    try:
        if 'file' not in request.files:
            return "No file uploaded", 400

        file = request.files['file']
        if file.filename == '':
            return "Empty filename", 400

        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext in ['.xls', '.xlsx']:
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'
            df = pd.read_excel(file, skiprows=1, dtype=str, engine=engine)
        else:
            return "Unsupported file format", 400

        print("Columns in uploaded Excel:", df.columns.tolist())

        export_items = []
        for _, row in df.iterrows():
            try:
                sku = str(row['Product ID']).strip()
                short_name = str(row['Product Code']).strip()
                name = str(row['Description']).strip()
                brand = str(row['Brand Name']).strip()
                retail = float(row['Current Retail'])

                product_class = str(row.get('Product Class', '')).strip().upper()
                tax_rate = 0.09 if product_class in NINE_PERCENT_CLASSES else 0.06

                price1 = int(round(retail * (1 + tax_rate)))
                price2 = round(price1 / 1.8)

                # === MSRP -> price3 ===
                # price3 = 1  -> sentinel: template shows red label, no "before" price
                # price3 = 0  -> no MSRP / no special styling
                # price3 = N  -> genuine "before" price (only when N > price1)
                try:
                    msrp_raw = row.get('MSRP', None)
                    if pd.notna(msrp_raw):
                        msrp_value = float(msrp_raw)
                        if msrp_value == 1:
                            price3 = 1
                        elif not msrp_value.is_integer():
                            price3 = 0
                        else:
                            msrp_int = int(msrp_value)
                            price3 = 0 if msrp_int <= price1 else msrp_int
                    else:
                        price3 = 0
                except Exception:
                    price3 = 0

                stock_column = next(
                    (col for col in row.index if col.strip().lower().replace(" ", "") in [
                        "qtyonhand", "quantityonhand", "onhand", "stock"
                    ]),
                    None,
                )
                stock = int(float(row[stock_column])) if stock_column and pd.notna(row[stock_column]) else 0

                item = {
                    "sku": sku,
                    "itemShortName": short_name,
                    "itemName": name,
                    "manufacturer": brand,
                    "price1": price1,
                    "price2": price2,
                    "price3": price3,
                    "inventory": stock,
                }
                export_items.append({k: clean_excel_string(v) for k, v in item.items()})
            except Exception as row_error:
                print(f"Skipping row: {row_error}")

        if not export_items:
            return "No valid items found.", 400

        # 1) Update the Google Sheet (Master prices/stock, new SKUs). Failure does not stop labels.
        sheet_result = sheet_sync(export_items)

        # 2) Read Master and merge descriptive fields (category, specification, rsrvTxt...)
        master = sheet_master()
        items = []
        for it in export_items:
            base = {k: v for k, v in master.get(it["sku"], {}).items()
                    if v not in ("", "nan", "None")}
            base.update(it)  # today's price/stock always wins
            base["IIS_COMMAND"] = "UPDATE"
            items.append(base)

        pd.DataFrame(items).to_excel("mapped.xlsx", index=False)

        # 3) Push to Hanshow
        status, result = update_esl(items)
        return jsonify({
            "status": status,
            "total_items": len(items),
            "sheet": sheet_result,
            "master_rows_loaded": len(master),
            "result": result,
            "local_file_url": "/download_last_xlsx",
        })

    except Exception as e:
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/download_last_xlsx', methods=['GET'])
def download_last_xlsx():
    if not os.path.exists("mapped.xlsx"):
        return "mapped.xlsx not found", 404
    return send_file(
        "mapped.xlsx",
        download_name="mapped.xlsx",
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


if __name__ == '__main__':
    app.run()
