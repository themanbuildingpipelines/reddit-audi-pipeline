"""
Reddit Audi pipeline, rebuilt locally (no Azure).

Same medallion flow as the original repo, with DuckDB standing in for Azure SQL:
  bronze  : every backup CSV unioned as-is, one row per post id (latest scrape wins)
  silver  : title + selftext merged, timestamps converted, issues categorised,
            model / year / mileage extracted, duplicate texts removed
  gold    : categorised / uncategorised views plus the analysis tables the
            dashboard reads (exported to gold/*.csv)

Usage: python pipeline.py <raw_csv_dir> <out_dir>
"""
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

RAW_DIR = Path(sys.argv[1] if len(sys.argv) > 1 else "raw")
OUT_DIR = Path(sys.argv[2] if len(sys.argv) > 2 else "out")
OUT_DIR.mkdir(parents=True, exist_ok=True)
(OUT_DIR / "gold").mkdir(exist_ok=True)

con = duckdb.connect(str(OUT_DIR / "audi.duckdb"))
for s in ("bronze", "silver", "gold"):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")

# ---------------------------------------------------------------- BRONZE
frames = []
for f in sorted(RAW_DIR.glob("*.csv")):
    df = pd.read_csv(f, dtype=str)
    df.columns = [c.lower().strip() for c in df.columns]
    df["source_file"] = f.name
    frames.append(df)
raw = pd.concat(frames, ignore_index=True)
con.register("raw_df", raw)
con.execute("""
CREATE OR REPLACE TABLE bronze.audi_rdata AS
SELECT * EXCLUDE (rn) FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id
                              ORDER BY TRY_CAST(num_comments AS INT) DESC NULLS LAST,
                                       source_file DESC) AS rn
    FROM raw_df
    WHERE id IS NOT NULL
) WHERE rn = 1
""")

# ---------------------------------------------------------------- SILVER
# Keyword lists from the original proc_load.py. Matching now uses word
# boundaries: the old substring match tagged "back" as HVAC ("ac"),
# "chip"/"hp" inside other words, and so on.
CATEGORIES = {
    "Air and Fuel Delivery": ["fuel pump", "injectors", "injector", "throttle", "carb", "air intake",
        "fuel line", "fuel filter", "gas cap", "maf sensor", "maf", "fuel rail", "fuel tank",
        "fuel system", "intake manifold"],
    "Audi Accessories": ["floor mats", "roof rack", "bike rack", "keychain", "phone holder",
        "seat covers", "dash cam", "dashcam", "mud flaps", "trunk liner", "cargo net", "sunshade"],
    "Mods": ["tuning", "tuned", "remap", "flash", "tune", "apr", "jb4", "unitronic", "034",
        "piggyback", "downpipe", "cold air intake", "blow off valve", "turbo upgrade",
        "coilovers", "widebody", "wrap", "wrapped", "spoiler", "diffuser", "custom exhaust", "stage 1",
        "stage 2"],
    "Body": ["bumper", "fender", "hood", "trunk", "grille", "grill", "mirror", "door panel",
        "body kit", "side skirt", "roof", "windshield", "paint", "dent", "scratch", "rust"],
    "Brake": ["brake", "brakes", "pads", "rotors", "calipers", "brake lines", "abs", "brembo",
        "ebc", "brake fluid", "master cylinder"],
    "Cooling": ["radiator", "coolant", "intercooler", "water pump", "thermostat", "overheating",
        "overheat", "heat exchanger", "antifreeze"],
    "Electrical & Lighting": ["ecu", "alternator", "starter", "wiring", "harness",
        "fuse", "relay", "headlights", "headlight", "taillights", "led", "hid", "drl", "turn signal",
        "fog lights", "mmi", "carplay", "android auto", "sensor", "sensors", "dash cluster",
        "virtual cockpit", "battery"],
    "Emission Control": ["catalytic converter", "o2 sensor", "egr", "dpf", "emissions",
        "exhaust gas", "smog", "nox", "check engine light", "cel", "p0420"],
    "Engine": ["v6", "v8", "v10", "turbo", "tfsi", "crankshaft", "pistons", "valves", "camshaft",
        "timing chain", "head gasket", "misfire", "compression", "oil pump", "engine",
        "oil consumption", "carbon buildup", "pcv", "boost"],
    "Exhaust": ["muffler", "midpipe", "catback", "cat back", "resonator", "headers",
        "straight pipe", "exhaust", "drone", "pops and bangs", "exhaust tips"],
    "HVAC": ["ac", "a/c", "air conditioning", "heater", "blower motor", "vents", "cabin filter",
        "ac compressor", "condenser", "evaporator", "climate control", "refrigerant"],
    "Hybrid/Electric": ["ev", "hybrid", "plug-in", "e-tron", "etron", "charging", "charger",
        "regen", "inverter", "phev"],
    "Suspension and Steering": ["shocks", "struts", "springs", "sway bar", "control arms",
        "control arm", "bushings", "tie rods", "steering rack", "power steering", "alignment",
        "camber", "suspension", "air suspension"],
    "Tire and Wheel": ["rims", "tires", "tyres", "wheels", "lug nuts", "spacers", "tread",
        "winter tires", "summer tires", "all-season", "michelin", "pirelli", "stance"],
    "Tools and Equipment": ["jack", "torque wrench", "obd2", "obd scanner", "diagnostic tool",
        "impact gun", "multimeter", "vcds", "obdeleven", "obd11"],
    "Transmission and Driveline": ["gearbox", "clutch", "dsg", "s-tronic", "s tronic",
        "tiptronic", "manual", "shifter", "torque converter", "driveshaft", "differential",
        "axle", "axles", "cv joint", "transmission", "quattro", "launch control", "mechatronic"],
}
CAT_RX = {c: re.compile(r"(?<![\w])(" + "|".join(re.escape(k) for k in kws) + r")(?![\w])", re.I)
          for c, kws in CATEGORIES.items()}

def classify(text: str) -> str:
    hits = [c for c, rx in CAT_RX.items() if rx.search(text)]
    return " | ".join(hits) if hits else "Uncategorized"

# Longest names first so "RS e-tron GT" wins over "e-tron", "RS3" over "S3", "TT RS" over "TT".
MODELS = sorted([
    "A1", "A3", "A4", "A5", "A6", "A7", "A8", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8",
    "S3", "S4", "S5", "S6", "S7", "S8", "SQ5", "SQ7", "SQ8", "RS3", "RS4", "RS5", "RS6", "RS7",
    "RSQ3", "RSQ8", "TT", "TTS", "TT RS", "R8", "e-tron", "e-tron GT", "Q4 e-tron", "RS e-tron GT",
    "Allroad",
], key=len, reverse=True)
MODEL_RX = [(m, re.compile(rf"(?<![\w-]){re.escape(m)}(?![\w-])", re.I)) for m in MODELS]
SUB_MODEL = {"audia4": "A4", "audis4": "S4", "audis5": "S5", "audiq6": "Q6", "audiq5": "Q5",
             "audia6": "A6", "audir8": "R8", "audiq7": "Q7", "audiq3": "Q3", "audia3": "A3"}

def extract_model(text, subreddit):
    # First model mentioned in the post wins; on a tie at one position, the longer name.
    hits = [(mt.start(), -len(m), m) for m, rx in MODEL_RX for mt in [rx.search(text)] if mt]
    if hits:
        return min(hits)[2], "text"
    m = SUB_MODEL.get(str(subreddit).lower())
    return (m, "subreddit") if m else (None, None)

YEAR_RX = re.compile(r"(?<!\d)(199\d|20[0-2]\d)(?!\d)")
MILES_RX = re.compile(r"(?<![\d.])(\d{1,3}(?:,\d{3})|\d{1,6})\s?(k|miles|mi|km)\b", re.I)

def extract_year(text):
    m = YEAR_RX.search(text)
    return int(m.group(1)) if m else None

def extract_miles(text):
    m = MILES_RX.search(text)
    if not m:
        return None
    n = int(m.group(1).replace(",", ""))
    unit = m.group(2).lower()
    if unit == "k":
        n *= 1000
    if unit == "km":
        n = round(n * 0.621371)
    return n if 100 <= n <= 500_000 else None

b = con.execute("SELECT * FROM bronze.audi_rdata").df()
b["user_issue"] = (b["title"].fillna("") + " " + b["selftext"].fillna("")).str.strip()
b["issue_raise_date"] = pd.to_datetime(pd.to_numeric(b["created_utc"], errors="coerce"), unit="s")
b["issue_category"] = b["user_issue"].map(classify)
mm = [extract_model(t, s) for t, s in zip(b["user_issue"], b["subreddit"])]
b["car_model"] = [m for m, _ in mm]
b["model_source"] = [s for _, s in mm]
b["year_model"] = b["user_issue"].map(extract_year)
b["mileage"] = b["user_issue"].map(extract_miles)
b["num_of_comments"] = pd.to_numeric(b["num_comments"], errors="coerce").fillna(0).astype(int)
b["score"] = pd.to_numeric(b["score"], errors="coerce").fillna(0).astype(int)
base = b[["id", "subreddit", "author", "title", "num_of_comments", "score", "permalink",
          "user_issue", "issue_raise_date", "issue_category", "car_model", "model_source",
          "year_model", "mileage"]]
con.register("base_df", base)
con.execute("CREATE OR REPLACE TABLE silver.audi_data_base AS SELECT * FROM base_df")

con.execute("""
CREATE OR REPLACE TABLE silver.audi_data_enriched AS
SELECT * EXCLUDE (rn) FROM (
    SELECT
        id, subreddit,
        CASE WHEN author IN ('[deleted]', '[Deleted]') OR author IS NULL THEN 'n/a' ELSE author END AS author,
        title, num_of_comments, score, permalink AS url, user_issue, issue_raise_date,
        issue_category, car_model, model_source, year_model, mileage,
        ROW_NUMBER() OVER (PARTITION BY lower(trim(user_issue)) ORDER BY num_of_comments DESC, id) AS rn
    FROM silver.audi_data_base
    WHERE length(trim(user_issue)) > 0
) WHERE rn = 1
""")

# ---------------------------------------------------------------- GOLD
con.execute("""CREATE OR REPLACE VIEW gold.audi_data_categorized AS
    SELECT * FROM silver.audi_data_enriched WHERE issue_category <> 'Uncategorized'""")
con.execute("""CREATE OR REPLACE VIEW gold.audi_data_uncategorized AS
    SELECT * FROM silver.audi_data_enriched WHERE issue_category = 'Uncategorized'""")
con.execute("""CREATE OR REPLACE VIEW gold.split_issues AS
    SELECT id, issue_raise_date, num_of_comments, score, car_model,
           trim(unnest(string_split(issue_category, '|'))) AS issue
    FROM gold.audi_data_categorized""")

gold = {
    "kpis": """
        SELECT
          COUNT(*)                                                        AS posts,
          COUNT(*) FILTER (WHERE issue_category <> 'Uncategorized')       AS categorized_posts,
          COUNT(*) FILTER (WHERE car_model IS NOT NULL)                   AS posts_with_model,
          SUM(num_of_comments)                                            AS total_comments,
          COUNT(DISTINCT subreddit)                                       AS subreddits,
          strftime(MIN(issue_raise_date), '%Y-%m-%d')                     AS first_post,
          strftime(MAX(issue_raise_date), '%Y-%m-%d')                     AS last_post
        FROM silver.audi_data_enriched""",
    "top_issues": """
        SELECT issue, COUNT(DISTINCT id) AS mentions,
               SUM(num_of_comments) AS total_comments,
               ROUND(AVG(num_of_comments), 1) AS avg_comments,
               ROUND(MEDIAN(num_of_comments), 1) AS median_comments,
               ROUND(AVG(score), 1) AS avg_score
        FROM gold.split_issues GROUP BY issue ORDER BY mentions DESC""",
    "top_models": """
        SELECT car_model,
               COUNT(*) AS mentions,
               COUNT(*) FILTER (WHERE model_source = 'text') AS named_in_post,
               COUNT(*) FILTER (WHERE model_source = 'subreddit') AS from_subreddit,
               SUM(num_of_comments) AS total_comments,
               ROUND(AVG(num_of_comments), 1) AS avg_comments
        FROM silver.audi_data_enriched WHERE car_model IS NOT NULL
        GROUP BY car_model ORDER BY mentions DESC""",
    # The API returns at most ~1,000 posts per listing, so the backups are a
    # snapshot weighted to Aug-Sep 2025. Weekly share of posts keeps volume
    # swings from the collection window out of the issue mix.
    "issue_mix_by_week": """
        WITH wk AS (
            SELECT strftime(date_trunc('week', issue_raise_date), '%Y-%m-%d') AS week, id
            FROM silver.audi_data_enriched WHERE issue_raise_date >= DATE '2025-08-11'),
        tot AS (SELECT week, COUNT(*) AS posts FROM wk GROUP BY week)
        SELECT tot.week, s.issue, COUNT(DISTINCT s.id) AS mentions, tot.posts,
               ROUND(COUNT(DISTINCT s.id) / tot.posts, 4) AS share_of_posts
        FROM gold.split_issues s
        JOIN wk ON wk.id = s.id
        JOIN tot ON tot.week = wk.week
        GROUP BY tot.week, s.issue, tot.posts ORDER BY tot.week, s.issue""",
    "posts_by_day": """
        SELECT strftime(issue_raise_date, '%Y-%m-%d') AS day, COUNT(*) AS posts,
               COUNT(*) FILTER (WHERE issue_category <> 'Uncategorized') AS categorized_posts
        FROM silver.audi_data_enriched
        WHERE issue_raise_date >= DATE '2025-08-11'
        GROUP BY ALL ORDER BY day""",
    "subreddits": """
        SELECT 'r/' || subreddit AS subreddit, COUNT(*) AS posts, SUM(num_of_comments) AS comments
        FROM silver.audi_data_enriched GROUP BY ALL ORDER BY posts DESC""",
    "top_posts": """
        SELECT id, title, 'r/' || subreddit AS subreddit, car_model, issue_category,
               num_of_comments AS comments, score,
               strftime(issue_raise_date, '%Y-%m-%d') AS posted, url
        FROM silver.audi_data_enriched
        ORDER BY num_of_comments DESC LIMIT 150""",
}
for name, sql in gold.items():
    con.execute(f"CREATE OR REPLACE TABLE gold.{name} AS {sql}")
    con.execute(f"COPY gold.{name} TO '{OUT_DIR / 'gold' / (name + '.csv')}' (HEADER)")
    print(f"gold.{name}: {con.execute(f'SELECT COUNT(*) FROM gold.{name}').fetchone()[0]} rows")

print("bronze rows:", con.execute("SELECT COUNT(*) FROM bronze.audi_rdata").fetchone()[0])
print("silver enriched rows:", con.execute("SELECT COUNT(*) FROM silver.audi_data_enriched").fetchone()[0])
