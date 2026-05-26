#!/usr/bin/env python3
"""
Verify Kuta compatibility between Aditya and Anuradha charts.
Pulls kuta_profile from DB and manually computes Ashta Kuta scores.
"""
import json, os, sys
import psycopg2, psycopg2.extras

conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"],
    connect_timeout=10,
)

cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, name, chart_data FROM saved_charts ORDER BY name")
rows = cur.fetchall()

# Print all charts so user can identify Aditya and Anuradha
print("=== ALL SAVED CHARTS ===")
chart_map = {}
for row in rows:
    cd = json.loads(row["chart_data"])
    kp = cd.get("kuta_profile", {})
    moon = next((p for p in cd.get("planets", []) if p["name"] == "Moon"), {})
    birth = cd.get("birth", {})
    print(f"\nID={row['id']} | Name={row['name']}")
    print(f"  Birth: {birth.get('date')} | {birth.get('place')}")
    print(f"  Moon: {kp.get('moon_sign')} / {kp.get('moon_nakshatra')}")
    print(f"  Lord: {kp.get('moon_sign_lord')} | Gana: {kp.get('gana')} | Nadi: {kp.get('nadi')}")
    print(f"  Yoni: {kp.get('yoni')}({kp.get('yoni_gender')}) | Varna: {kp.get('varna')} | Vashya: {kp.get('vashya')}")
    chart_map[row['id']] = {"label": row["name"], "kp": kp, "moon": moon, "cd": cd}

cur.close()
conn.close()

# ── Ashta Kuta manual scoring ──────────────────────────────────────────────
NAKSHATRAS = [
    "Ashwini","Bharani","Krittika","Rohini","Mrigashira","Ardra",
    "Punarvasu","Pushya","Ashlesha","Magha","Purva Phalguni","Uttara Phalguni",
    "Hasta","Chitra","Swati","Vishakha","Anuradha","Jyeshtha",
    "Mula","Purva Ashadha","Uttara Ashadha","Shravana","Dhanishta","Shatabhisha",
    "Purva Bhadrapada","Uttara Bhadrapada","Revati"
]
SIGNS = ["Aries","Taurus","Gemini","Cancer","Leo","Virgo",
         "Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]

NAKSHATRA_GANA = [
    "Deva","Manava","Rakshasa","Manava","Deva","Manava","Deva","Deva","Rakshasa",
    "Rakshasa","Manava","Manava","Deva","Rakshasa","Deva","Rakshasa","Deva","Rakshasa",
    "Rakshasa","Manava","Manava","Deva","Rakshasa","Manava","Manava","Deva","Deva"
]
NAKSHATRA_NADI = [
    "Adya","Madhya","Antya","Adya","Madhya","Antya","Adya","Madhya","Antya",
    "Adya","Madhya","Antya","Adya","Madhya","Antya","Adya","Madhya","Antya",
    "Adya","Madhya","Antya","Adya","Madhya","Antya","Adya","Madhya","Antya"
]
NAKSHATRA_YONI = [
    "Horse","Elephant","Sheep","Serpent","Serpent","Dog","Cat","Sheep","Cat",
    "Rat","Rat","Cow","Buffalo","Tiger","Buffalo","Tiger","Deer","Deer",
    "Dog","Monkey","Mongoose","Monkey","Lion","Horse","Lion","Cow","Elephant"
]
NAKSHATRA_YONI_GENDER = [
    "M","M","F","M","F","F","F","M","M","M",
    "F","M","F","F","M","M","F","M","M","M",
    "M","F","F","F","M","F","F"
]
SIGN_VARNA = [
    "Kshatriya","Vaishya","Shudra","Brahmin","Kshatriya","Vaishya",
    "Shudra","Brahmin","Kshatriya","Vaishya","Shudra","Brahmin"
]
SIGN_VASHYA = [
    "Chatushpada","Chatushpada","Dwipada","Jalachara","Vanachara","Dwipada",
    "Dwipada","Keeta","Chatushpada","Chatushpada","Dwipada","Jalachara"
]
SIGN_LORDS = [
    "Mars","Venus","Mercury","Moon","Sun","Mercury",
    "Venus","Mars","Jupiter","Saturn","Saturn","Jupiter"
]

# Yoni friendship pairs
YONI_FRIENDS = {
    "Horse": "Horse", "Elephant": "Elephant", "Sheep": "Sheep",
    "Serpent": "Serpent", "Dog": "Dog", "Cat": "Cat",
    "Rat": "Rat", "Cow": "Cow", "Buffalo": "Buffalo",
    "Tiger": "Tiger", "Deer": "Deer", "Monkey": "Monkey",
    "Mongoose": "Mongoose", "Lion": "Lion"
}
YONI_ENEMIES = {
    ("Horse", "Buffalo"), ("Elephant", "Lion"), ("Sheep", "Monkey"),
    ("Serpent", "Mongoose"), ("Dog", "Deer"), ("Cat", "Rat"),
    ("Cow", "Tiger"),
}

# Planetary friendship table (natural)
# F=friend, N=neutral, E=enemy
PLANET_FRIENDSHIP = {
    "Sun":     {"Moon":"F","Mars":"F","Jupiter":"F","Mercury":"N","Venus":"E","Saturn":"E"},
    "Moon":    {"Sun":"F","Mercury":"F","Mars":"N","Jupiter":"N","Venus":"N","Saturn":"N"},
    "Mars":    {"Sun":"F","Moon":"F","Jupiter":"F","Mercury":"E","Venus":"N","Saturn":"N"},
    "Mercury": {"Sun":"F","Venus":"F","Mars":"N","Jupiter":"N","Saturn":"N","Moon":"E"},
    "Jupiter": {"Sun":"F","Moon":"F","Mars":"F","Saturn":"N","Mercury":"E","Venus":"E"},
    "Venus":   {"Mercury":"F","Saturn":"F","Mars":"N","Jupiter":"N","Sun":"E","Moon":"E"},
    "Saturn":  {"Mercury":"F","Venus":"F","Jupiter":"N","Sun":"E","Moon":"E","Mars":"E"},
    "Rahu":    {}, "Ketu": {}
}

# Tara categories (1-based index mod 9)
TARA_NAMES = {1:"Janma",2:"Sampat",3:"Vipat",4:"Kshema",5:"Pratyak",
               6:"Sadhaka",7:"Vadha",8:"Mitra",9:"Param Mitra"}
TARA_GOOD = {2,4,6,8,9}  # auspicious taras

def get_sign_idx(kp):
    sign = kp.get("moon_sign","")
    return SIGNS.index(sign) if sign in SIGNS else -1

def get_nak_idx(kp):
    nak = kp.get("moon_nakshatra","")
    return NAKSHATRAS.index(nak) if nak in NAKSHATRAS else -1

def score_varna(kp1, kp2):
    # Groom >= Bride in varna hierarchy: Brahmin>Kshatriya>Vaishya>Shudra
    order = {"Brahmin":4,"Kshatriya":3,"Vaishya":2,"Shudra":1}
    v1 = order.get(kp1.get("varna"),0)
    v2 = order.get(kp2.get("varna"),0)
    return 1 if v1 >= v2 else 0  # 1 if boy >= girl, else 0

def score_vashya(kp1, kp2):
    # Simple: same Vashya group = 2, compatible pairs = 1, else 0
    VASHYA_COMPAT = {
        ("Dwipada","Chatushpada"):1, ("Dwipada","Jalachara"):1,
        ("Jalachara","Dwipada"):1, ("Chatushpada","Dwipada"):1,
    }
    v1, v2 = kp1.get("vashya",""), kp2.get("vashya","")
    if v1 == v2: return 2
    return VASHYA_COMPAT.get((v1,v2), 0)

def score_tara(kp1, kp2):
    # Count from person1's nak to person2's nak (inclusive), divide by 9, take remainder
    n1, n2 = get_nak_idx(kp1), get_nak_idx(kp2)
    if n1 < 0 or n2 < 0: return 0
    forward = ((n2 - n1) % 27) + 1
    tara_1to2 = ((forward - 1) % 9) + 1
    backward = ((n1 - n2) % 27) + 1
    tara_2to1 = ((backward - 1) % 9) + 1
    good1 = tara_1to2 in TARA_GOOD
    good2 = tara_2to1 in TARA_GOOD
    t1name = TARA_NAMES.get(tara_1to2,"?")
    t2name = TARA_NAMES.get(tara_2to1,"?")
    print(f"    Tara A→B: count={forward} → Tara#{tara_1to2} ({t1name}) {'✓' if good1 else '✗'}")
    print(f"    Tara B→A: count={backward} → Tara#{tara_2to1} ({t2name}) {'✓' if good2 else '✗'}")
    if good1 and good2: return 3
    if good1 or good2: return 1.5
    return 0

def score_yoni(kp1, kp2):
    y1, g1 = kp1.get("yoni",""), kp1.get("yoni_gender","")
    y2, g2 = kp2.get("yoni",""), kp2.get("yoni_gender","")
    pair = tuple(sorted([y1,y2]))
    is_enemy = pair in {tuple(sorted(e)) for e in YONI_ENEMIES}
    if is_enemy: return 0
    if y1 == y2:
        return 4 if g1 != g2 else 3  # opposite gender same yoni = 4
    return 2  # friendly/neutral non-enemy

def score_graha_maitri(kp1, kp2):
    lord1 = kp1.get("moon_sign_lord","")
    lord2 = kp2.get("moon_sign_lord","")
    r12 = PLANET_FRIENDSHIP.get(lord1, {}).get(lord2, "N")
    r21 = PLANET_FRIENDSHIP.get(lord2, {}).get(lord1, "N")
    print(f"    {lord1}→{lord2}: {r12}  |  {lord2}→{lord1}: {r21}")
    score_map = {
        ("F","F"):5, ("F","N"):4, ("N","F"):4,
        ("N","N"):3, ("F","E"):1, ("E","F"):1,
        ("N","E"):0.5, ("E","N"):0.5, ("E","E"):0
    }
    return score_map.get((r12,r21), 1)

def score_gana(kp1, kp2):
    g1, g2 = kp1.get("gana",""), kp2.get("gana","")
    if g1 == g2: return 6
    if (g1=="Deva" and g2=="Manava") or (g1=="Manava" and g2=="Deva"): return 5
    if (g1=="Manava" and g2=="Rakshasa") or (g1=="Rakshasa" and g2=="Manava"): return 1
    # Deva-Rakshasa
    return 0

def score_bhakut(kp1, kp2):
    s1, s2 = get_sign_idx(kp1), get_sign_idx(kp2)
    if s1 < 0 or s2 < 0: return 0
    rel12 = (s2 - s1) % 12 + 1  # position of s2 from s1
    rel21 = (s1 - s2) % 12 + 1
    print(f"    Moon A in {kp1.get('moon_sign')} (sign {s1+1}), Moon B in {kp2.get('moon_sign')} (sign {s2+1})")
    print(f"    Relative positions: A→B={rel12}, B→A={rel21}")
    # Auspicious: 1-1(same), 3-11, 5-9, 7-7
    good_pairs = {(1,1),(3,11),(11,3),(5,9),(9,5),(7,7)}
    if (rel12, rel21) in good_pairs: return 7
    # 2-12 and 6-8 are specifically inauspicious = 0
    return 0

def score_nadi(kp1, kp2):
    n1, n2 = kp1.get("nadi",""), kp2.get("nadi","")
    return 0 if n1 == n2 else 8  # same nadi = dosha = 0, different = 8

def compute_ashta_kuta(label1, kp1, label2, kp2):
    print(f"\n{'='*60}")
    print(f"Ashta Kuta: {label1}  vs  {label2}")
    print(f"  A: Moon={kp1.get('moon_sign')}/{kp1.get('moon_nakshatra')} | Gana={kp1.get('gana')} Nadi={kp1.get('nadi')} lord={kp1.get('moon_sign_lord')}")
    print(f"  B: Moon={kp2.get('moon_sign')}/{kp2.get('moon_nakshatra')} | Gana={kp2.get('gana')} Nadi={kp2.get('nadi')} lord={kp2.get('moon_sign_lord')}")
    print()

    scores = {}
    scores["Varna  (1)"] = score_varna(kp1, kp2)
    scores["Vashya (2)"] = score_vashya(kp1, kp2)
    print("  [Tara]")
    scores["Tara   (3)"] = score_tara(kp1, kp2)
    scores["Yoni   (4)"] = score_yoni(kp1, kp2)
    print("  [Graha Maitri]")
    scores["G.Mait.(5)"] = score_graha_maitri(kp1, kp2)
    scores["Gana   (6)"] = score_gana(kp1, kp2)
    print("  [Bhakut]")
    scores["Bhakut (7)"] = score_bhakut(kp1, kp2)
    scores["Nadi   (8)"] = score_nadi(kp1, kp2)

    print()
    maxes = {"Varna  (1)":1,"Vashya (2)":2,"Tara   (3)":3,"Yoni   (4)":4,
             "G.Mait.(5)":5,"Gana   (6)":6,"Bhakut (7)":7,"Nadi   (8)":8}
    total = 0
    for k, s in scores.items():
        mx = maxes[k]
        print(f"  {k}: {s}/{mx}")
        total += s
    print(f"\n  TOTAL: {total}/36")
    return total

# Find charts labeled Aditya and Anuradha
aditya = None
anuradha = None
for cid, c in chart_map.items():
    lbl = (c["label"] or "").lower()
    if "aditya" in lbl and aditya is None:
        aditya = c
    if ("anuradha" in lbl or "anur" in lbl) and anuradha is None:
        anuradha = c

if aditya and anuradha:
    compute_ashta_kuta(aditya["label"], aditya["kp"], anuradha["label"], anuradha["kp"])
else:
    print(f"\nCould not auto-identify: aditya={aditya is not None}, anuradha={anuradha is not None}")
    print("Names found:", [c["label"] for c in chart_map.values()])
