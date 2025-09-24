import pandas as pd
from ETL_audi.cleaning_loading import load_data_to_sql, get_engine

engine = get_engine()

# 1. Read Bronze just to make sure everything runs nicely
df = pd.read_sql_table("audi_rdata", con=engine, schema="bronze")
print("Bronze sample:")

#2. Merge title and selftext, and store it as new_df and the column as issue

df['issue'] = df['title'].fillna('') + ' ' + df['selftext'].fillna('')

#3. Convert UTC to a datetime to make it easy to work with in SQL
df['created_datetime'] = pd.to_datetime(df['created_utc'], unit='s')

#4. Drop unwanted columns
df.drop(columns=['title', 'selftext', 'subreddit', 'upvote_ratio', 'subreddit_subscribers', 'permalink', 'source', 'link_flair_text', 'created_utc'], inplace=True, errors="ignore")

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

#Do the classification
df['categories'] = df['issue'].apply(classify)

# Flatten list into comma-separated string to make it readable in sql
df['categories'] = df['categories'].apply(lambda x: ", ".join(x))


# load to Silver.audi_data_base
load_data_to_sql(df, "audi_data_base", schema="silver", if_exists="replace")
