# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ed2d451f-7951-4e5c-83a4-2615447a7a78",
# META       "default_lakehouse_name": "lh_bank",
# META       "default_lakehouse_workspace_id": "e8e3a7f2-9a9b-4ab5-b74b-bc0933da2da8",
# META       "known_lakehouses": [
# META         {
# META           "id": "ed2d451f-7951-4e5c-83a4-2615447a7a78"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Parameters: the pipeline can override these at run time
start_year = 2019          # first year of quarterly data to load

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pull FDIC BankFind data into bronze as raw JSON files.
# Financials: one file per quarter; quarters already in bronze are skipped,
# so re-runs only fetch new quarters (incremental + idempotent).
import requests, json, os, time, datetime

BASE   = "https://banks.data.fdic.gov/api"
BRONZE = "/lakehouse/default/Files/bronze/fdic"   # local mount of lh_bank
FIN_FIELDS = "CERT,REPDTE,STALP,ASSET,DEP,LNLSNET,EQ,NETINC,ROA,ROE,NIMY,NCLNLS"

def get(endpoint, params):
    """One API call -> (rows, total). FDIC wraps each row as {'data': {...}}."""
    r = requests.get(f"{BASE}/{endpoint}", params={**params, "format": "json"}, timeout=120)
    r.raise_for_status()
    body = r.json()
    return [d["data"] for d in body["data"]], body["meta"]["total"]

def get_all(endpoint, params, page=10000):
    """Page through an endpoint with limit/offset until every row is read."""
    rows, offset = [], 0
    while True:
        batch, total = get(endpoint, {**params, "limit": page, "offset": offset})
        rows += batch
        offset += page
        if offset >= total or not batch:
            return rows, total
        time.sleep(0.5)                      # be polite to the API

def save(rows, path):
    """Write rows as JSON Lines (one record per line), which Spark reads natively."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

# 1) Quarter-end dates from start_year up to the last finished quarter
today = datetime.date.today()
quarters = [f"{y}{md}" for y in range(start_year, today.year + 1)
            for md in ("0331", "0630", "0930", "1231")
            if datetime.date(y, int(md[:2]), int(md[2:])) < today]

# 2) Financials: one file per quarter, skipping files we already have
for q in quarters:
    path = f"{BRONZE}/financials/repdte={q}/financials_{q}.json"
    if os.path.exists(path):
        continue                                   # incremental: already loaded
    rows, total = get_all("financials", {"filters": f"REPDTE:{q}", "fields": FIN_FIELDS})
    if rows:                                       # newest quarter may not be published yet
        save(rows, path)
        print(f"financials {q}: {len(rows)} rows (API total {total})")

# 3) Reference data: full refresh each run, saved under today's date
run = today.isoformat()
for ep in ["institutions", "failures"]:
    rows, total = get_all(ep, {})
    save(rows, f"{BRONZE}/{ep}/load_date={run}/{ep}.json")
    print(f"{ep}: {len(rows)} rows (API total {total})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
