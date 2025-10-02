import pandas as pd
import re
from cleaning_loading import load_data_to_sql, get_engine

# -----------------------
# 1. Connect to DB
# -----------------------
engine = get_engine()

# -----------------------
# 2. Read from Bronze
# -----------------------
df = pd.read_sql_table("audi_rdata", con=engine, schema="bronze")
print("Bronze sample:", df.head())

# -----------------------
# 3. Clean + Enrich
# -----------------------

# Merge title + selftext into "issue"
if 'title' in df.columns and 'selftext' in df.columns:
    df['issue'] = df['title'].fillna('') + ' ' + df['selftext'].fillna('')
else:
    df['issue'] = ''

# Convert UTC timestamp → datetime
if 'created_utc' in df.columns:
    df['created_datetime'] = pd.to_datetime(df['created_utc'], unit='s')
else:
    df['created_datetime'] = pd.NaT

# Drop unwanted columns
df.drop(
    columns=[
        'title', 'selftext', 'subreddit', 'upvote_ratio',
        'subreddit_subscribers', 'permalink', 'source',
        'link_flair_text', 'created_utc'
    ],
    inplace=True,
    errors="ignore"
)

# -----------------------
# 3a. Categorization
# -----------------------
def classify(text):
    text = text.lower()
    
    categories = {
        "Air and Fuel Delivery": [
            "fuel pump", "injectors", "throttle", "carb", "air intake", "fuel line",
            "fuel filter", "gas cap", "maf sensor", "fuel rail", "tank",
            "fuel system", "intake manifold"
        ],
        "Audi Accessories": [
            "floor mats", "roof rack", "bike rack", "keychain", "phone holder",
            "seat covers", "dash cam", "mud flaps", "trunk liner", "cargo net", "sunshade"
        ],
        "Mods": [
            "tuning", "ecu", "remap", "chip", "flash", "tune", "apr", "jb4", "unitronic",
            "piggyback", "downpipe", "cold air intake", "blow off valve", "turbo upgrade",
            "supercharger", "coilovers", "widebody", "wrap", "spoiler", "diffuser",
            "custom exhaust"
        ],
        "Body": [
            "bumper", "fender", "hood", "trunk", "grille", "spoiler", "mirror", "door panel",
            "body kit", "side skirt", "roof", "windshield", "paint", "wrap", "dent", "scratch", "rust"
        ],
        "Brake": [
            "pads", "rotors", "calipers", "discs", "brake lines", "abs", "brembo", "ebc",
            "ceramic pads", "stopping power", "squeak", "brake fluid", "master cylinder"
        ],
        "Cooling": [
            "radiator", "coolant", "intercooler", "water pump", "thermostat", "fans", "hoses",
            "overheating", "heat exchanger", "reservoir", "antifreeze"
        ],
        "Electrical, Lighting, Telematics": [
            "ecu", "alternator", "starter", "wiring", "harness", "fuse", "relay", "headlights",
            "taillights", "led", "hid", "drl", "turn signals", "fog lights", "mmi", "carplay",
            "sensors", "dash cluster"
        ],
        "Emission Control": [
            "catalytic converter", "o2 sensor", "egr", "dpf", "emissions", "exhaust gas", "smog",
            "nox", "check engine light", "cel", "p0420"
        ],
        "Engine": [
            "v6", "v8", "turbo", "tfsi", "crankshaft", "pistons", "valves", "camshaft",
            "timing chain", "head gasket", "misfire", "compression", "oil pump", "block",
            "boost", "hp", "torque"
        ],
        "Exhaust": [
            "muffler", "downpipe", "midpipe", "catback", "resonator", "headers", "straight pipe",
            "backpressure", "sound", "drone", "pops & bangs", "exhaust tips"
        ],
        "HVAC": [
            "ac", "air conditioning", "heater", "blower motor", "vents", "cabin filter", "compressor",
            "condenser", "evaporator", "climate control", "refrigerant"
        ],
        "Hybrid/Electric": [
            "battery", "ev", "hybrid", "plug-in", "e-tron", "charging", "range", "regen",
            "inverter", "motor", "phev", "supercharger station"
        ],
        "Suspension and Steering": [
            "shocks", "struts", "springs", "coilovers", "sway bar", "control arms", "bushings",
            "tie rods", "steering rack", "power steering", "alignment", "camber", "caster",
            "toe", "handling"
        ],
        "Tire and Wheel": [
            "rims", "alloys", "tires", "wheels", "hub", "lug nuts", "spacers", "offset", "tread",
            "winter tires", "summer tires", "all-season", "michelin", "pirelli", "continental",
            "18”, 19”, 20”", "stance"
        ],
        "Tools and Equipment": [
            "jack", "lift", "wrench", "socket", "torque wrench", "obd2 scanner", "diagnostic tool",
            "impact gun", "ratchet", "gauge", "multimeter", "vcds", "obdeleven"
        ],
        "Transmission and Driveline": [
            "gearbox", "clutch", "dsg", "s-tronic", "manual", "auto", "shifter", "torque converter",
            "driveshaft", "differential", "axles", "cv joint", "gear oil", "transmission fluid",
            "slip", "launch control"
        ],
    }
    
    matches = []
    for category, keywords in categories.items():
        if any(word in text for word in keywords):
            matches.append(category)
    
    return matches if matches else ["Uncategorized"]

df['categories'] = df['issue'].apply(classify)
df['categories'] = df['categories'].apply(lambda x: ", ".join(x))

# -----------------------
# 3b. Extract car model, year, mileage
# -----------------------
audi_models = [
    "A1","A2","A3","A4","A5","A6","A7","A8",
    "Q2","Q3","Q4","Q5","Q7","Q8",
    "S1","S3","S4","S5","S6","S7","S8",
    "RS3","RS4","RS5","RS6","RS7","RSQ3","RSQ8",
    "TT","TTS","TT RS",
    "R8","e-tron","Q4 e-tron","RS e-tron GT"
]

def extract_model(text):
    for model in audi_models:
        if re.search(rf"\b{model}\b", text, re.IGNORECASE):
            return model
    return None

def extract_year(text):
    match = re.search(r"(19[8-9]\d|20[0-2]\d)", text)
    return int(match.group(0)) if match else None

def extract_miles(text):
    match = re.search(r"(\d{1,3}[,]?\d{0,3})\s?(miles|mi|k)", text, re.IGNORECASE)
    if match:
        val = match.group(1).replace(",", "")
        num = int(val)
        if "k" in match.group(0).lower():
            num *= 1000
        return num
    return None

df["car_model"] = df["issue"].apply(extract_model)
df["year_model"] = df["issue"].apply(extract_year)
df["mileage"] = df["issue"].apply(extract_miles)

# -----------------------
# 4. Load to Silver
# -----------------------
load_data_to_sql(df, "audi_data_base", schema="silver", if_exists="replace")

print("Loaded enriched Silver table: silver.audi_data_base")
