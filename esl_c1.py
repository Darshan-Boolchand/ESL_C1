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

# === Hanshow API Configuration (credentials come from Render env vars) ===
API_BASE = "https://boolchand.slscanada.ca:9001"
USERNAME = os.environ.get("HANSHOW_USER", "")
PASSWORD = os.environ.get("HANSHOW_PASS", "")
CUSTOMER_CODE = "boolchand"
STORE_CODE = "C1"  # Curaçao store code

# === Google Sheet (Apps Script web app) ===
SHEET_WEB_URL = os.environ.get("SHEET_WEB_URL") or os.environ.get("APPS_SCRIPT_URL", "")
SHEET_WEB_SECRET = os.environ.get("SHEET_WEB_SECRET") or os.environ.get("SHEET_TOKEN", "")
SYNC_KEY = os.environ.get("SYNC_KEY", "")

# === Tax configuration based on Product Class ===
NINE_PERCENT_CLASSES = ["APPLE IPHONES", "OTHER PHONES", "SAMSUNG PHONES", "GAMING TITLES"]

# === Clean Excel string helper ===
_illegal_unichrs = [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F), (0x7F, 0x9F)]
_illegal_ranges = ["%s-%s" % (chr(low), chr(high)) for (low, high) in _illegal_unichrs]
_illegal_re = re.compile(u'[%s]' % u''.join(_illegal_ranges))

def clean_excel_string(value):
    if isinstance(value, str):
        return _illegal_re.sub("", value)
    return value

# === Clean text for ESL templates (quotes/backslashes/line breaks break the label file) ===
def clean_label_text(value):
    if not isinstance(value, str):
        return value
    s = value
    s = re.sub(r'(\d)\s*"', r'\1 IN ', s)   # 55" -> 55 IN
    s = s.replace('"', '')
    s = s.replace('\\', '/')
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# Fields Hanshow requires as strings
STRING_FIELDS = {"sku", "itemName", "itemShortName", "brand", "manufacturer", "specification", "ean",
                 "rsrvTxt1", "rsrvTxt2", "rsrvTxt3", "rsrvTxt4", "rsrvTxt5", "rsrvTxt11",
                 "rsrvTxt12", "rsrvTxt13", "rsrvTxt14", "rsrvTxt15", "level1CategoryName",
                 "level2CategoryName", "level4CategoryName", "level5CategoryName"}

def as_text(v):
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v)

# === Hanshow token ===
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
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "customerStoreCode": CUSTOMER_CODE,
            "storeCode": STORE_CODE,
            "batchNo": f"batch-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}",
            "items": batch
        }
        return requests.post(url, headers=headers, json=payload, verify=False)

    for it in items:
        for k in STRING_FIELDS:
            if k in it and it[k] is not None and not isinstance(it[k], str):
                it[k] = as_text(it[k])

    responses = []
    token = get_token()
    batch_size = 1000

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        print(f"📦 Sending batch {i // batch_size + 1} with {len(batch)} items")
        response = send_request(batch, token)

        if response.status_code == 401:
            print("🔁 Token may have expired, retrying with new token...")
            token = get_token()
            response = send_request(batch, token)

        try:
            res_json = response.json()
        except Exception:
            res_json = {"error": "Failed to decode JSON", "text": response.text}

        print("📡 API Response:", response.status_code, response.text)
        responses.append({"batch": i // batch_size + 1, "status": response.status_code, "response": res_json})

    return 200, {"batches_sent": len(responses), "results": responses}

# === Google Sheet helpers ===
def clean_sheet_items(raw_items):
    items = []
    for it in raw_items:
        item = {"IIS_COMMAND": "UPDATE"}
        for k, v in it.items():
            if k in STRING_FIELDS and v is not None and not isinstance(v, str):
                v = as_text(v)
            item[k] = clean_label_text(v) if isinstance(v, str) else v
        items.append({k: clean_excel_string(v) for k, v in item.items()})
    return items

def _sheet_data(r):
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Sheet endpoint did not return JSON (HTTP {r.status_code}). "
                           f"Starts with: {r.text[:200]!r} -> redeploy the web app as Execute as Me / Anyone")
    if "items" not in data:
        raise RuntimeError(f"Sheet endpoint error: {data}")
    return data

def build_items_from_sheet():
    if not SHEET_WEB_URL or not SHEET_WEB_SECRET:
        raise RuntimeError("SHEET_WEB_URL / SHEET_WEB_SECRET are not set")
    r = requests.get(SHEET_WEB_URL, params={"secret": SHEET_WEB_SECRET}, timeout=180)
    r.raise_for_status()
    return clean_sheet_items(_sheet_data(r)["items"])

def merge_with_sheet(a1_items):
    """Send today's C1 rows to the sheet (updates price/old price/stock, adds new SKUs),
    then return the full Master as push-ready items."""
    slim = [{k: it.get(k) for k in ("sku", "itemShortName", "itemName", "manufacturer",
                                    "price1", "price2", "price3", "inventory")} for it in a1_items]
    r = requests.post(SHEET_WEB_URL, json={"secret": SHEET_WEB_SECRET, "a1_items": slim}, timeout=300)
    r.raise_for_status()
    data = _sheet_data(r)
    print(f"📗 Sheet merge: updated={data.get('updated')} added={data.get('added')} textFixed={data.get('textFixed')}")
    return clean_sheet_items(data["items"])

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

        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext in ['.xls', '.xlsx']:
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'
            df = pd.read_excel(file, skiprows=1, dtype=str, engine=engine)
        else:
            return "Unsupported file format", 400

        print("🧾 Columns in uploaded Excel:", df.columns.tolist())

        items = []
        for _, row in df.iterrows():
            try:
                sku = str(row['Product ID']).strip()
                short_name = clean_label_text(str(row['Product Code']).strip())
                name = clean_label_text(str(row['Description']).strip())
                brand = clean_label_text(str(row['Brand Name']).strip())
                retail = float(row['Current Retail'])

                product_class = str(row.get('Product Class', '')).strip().upper()
                tax_rate = 0.09 if product_class in NINE_PERCENT_CLASSES else 0.06

                price1 = int(round(retail * (1 + tax_rate)))
                price2 = float(f"{price1 / 1.8:.2f}")

                # price3: 1 = red label sentinel, 0 = none, N > price1 = "before" price
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
                items.append({k: clean_excel_string(v) for k, v in item.items()})
            except Exception as row_error:
                print(f"⚠️ Skipping row: {row_error}")

        if not items:
            return "No valid items found.", 400

        # === Merge with the Google Sheet Master (specs, categories, rsrvTxt...) ===
        sheet_status = "not configured"
        if SHEET_WEB_URL and SHEET_WEB_SECRET:
            try:
                items = merge_with_sheet(items)
                sheet_status = "ok"
                print(f"📗 Using Master from Google Sheet: {len(items)} items")
            except Exception as sheet_err:
                sheet_status = f"FAILED: {sheet_err}"
                print(f"❌ SHEET ERROR (pushing plain C1 rows instead): {sheet_err}")

        df_mapped = pd.DataFrame(items)
        df_mapped = df_mapped.map(clean_excel_string)
        df_mapped.to_excel("mapped.xlsx", index=False)
        print("💾 mapped.xlsx saved locally")

        status, result = update_esl(items)
        return jsonify({
            "status": status,
            "total_items": len(items),
            "sheet": sheet_status,
            "result": result,
            "local_file_url": "/download_last_xlsx"
        })

    except Exception as e:
        print("❌ ERROR IN /convert")
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
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/sync_from_sheet', methods=['GET', 'POST'])
def sync_from_sheet():
    try:
        if SYNC_KEY and request.args.get("key") != SYNC_KEY:
            return "forbidden", 403
        items = build_items_from_sheet()
        if not items:
            return "No items in sheet", 400
        status, result = update_esl(items)
        return jsonify({"status": status, "total_items": len(items), "result": result})
    except Exception as e:
        print("❌ ERROR IN /sync_from_sheet")
        traceback.print_exc()
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    app.run()
