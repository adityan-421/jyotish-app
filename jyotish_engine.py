#!/usr/bin/env python3
"""
Vedic Jyotish (Astrology) Engine — Parameterized, returns dicts.
Refactored from vedic_jyotish.py to support arbitrary birth data.
"""

import swisseph as swe
from datetime import datetime, timedelta
from collections import defaultdict

# ── Constants ───────────────────────────────────────────────────────────────
SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

SIGN_LORDS = [
    "Mars", "Venus", "Mercury", "Moon", "Sun", "Mercury",
    "Venus", "Mars", "Jupiter", "Saturn", "Saturn", "Jupiter"
]

NAKSHATRAS = [
    "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashira", "Ardra",
    "Punarvasu", "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni",
    "Hasta", "Chitra", "Swati", "Vishakha", "Anuradha", "Jyeshtha",
    "Mula", "Purva Ashadha", "Uttara Ashadha", "Shravana", "Dhanishta", "Shatabhisha",
    "Purva Bhadrapada", "Uttara Bhadrapada", "Revati"
]

NAKSHATRA_LORDS = [
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu",
    "Jupiter", "Saturn", "Mercury", "Ketu", "Venus", "Sun",
    "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu",
    "Jupiter", "Saturn", "Mercury"
]

# ── Kuta Compatibility Tables (indexed by nakshatra 0-26 or sign 0-11) ──────

# Gana: temperament of each nakshatra
NAKSHATRA_GANA = [
    "Deva",     # 0  Ashwini
    "Manava",   # 1  Bharani
    "Rakshasa", # 2  Krittika
    "Manava",   # 3  Rohini
    "Deva",     # 4  Mrigashira
    "Manava",   # 5  Ardra
    "Deva",     # 6  Punarvasu
    "Deva",     # 7  Pushya
    "Rakshasa", # 8  Ashlesha
    "Rakshasa", # 9  Magha
    "Manava",   # 10 Purva Phalguni
    "Manava",   # 11 Uttara Phalguni
    "Deva",     # 12 Hasta
    "Rakshasa", # 13 Chitra
    "Deva",     # 14 Swati
    "Rakshasa", # 15 Vishakha
    "Deva",     # 16 Anuradha
    "Rakshasa", # 17 Jyeshtha
    "Rakshasa", # 18 Mula
    "Manava",   # 19 Purva Ashadha
    "Manava",   # 20 Uttara Ashadha
    "Deva",     # 21 Shravana
    "Rakshasa", # 22 Dhanishta
    "Manava",   # 23 Shatabhisha
    "Manava",   # 24 Purva Bhadrapada
    "Deva",     # 25 Uttara Bhadrapada
    "Deva",     # 26 Revati
]

# Nadi: Adya (Vata) / Madhya (Pitta) / Antya (Kapha) — sequential cycle of 3
NAKSHATRA_NADI = [
    "Adya",   # 0  Ashwini
    "Madhya", # 1  Bharani
    "Antya",  # 2  Krittika
    "Adya",   # 3  Rohini
    "Madhya", # 4  Mrigashira
    "Antya",  # 5  Ardra
    "Adya",   # 6  Punarvasu
    "Madhya", # 7  Pushya
    "Antya",  # 8  Ashlesha
    "Adya",   # 9  Magha
    "Madhya", # 10 Purva Phalguni
    "Antya",  # 11 Uttara Phalguni
    "Adya",   # 12 Hasta
    "Madhya", # 13 Chitra
    "Antya",  # 14 Swati
    "Adya",   # 15 Vishakha
    "Madhya", # 16 Anuradha
    "Antya",  # 17 Jyeshtha
    "Adya",   # 18 Mula
    "Madhya", # 19 Purva Ashadha
    "Antya",  # 20 Uttara Ashadha
    "Adya",   # 21 Shravana
    "Madhya", # 22 Dhanishta
    "Antya",  # 23 Shatabhisha
    "Adya",   # 24 Purva Bhadrapada
    "Madhya", # 25 Uttara Bhadrapada
    "Antya",  # 26 Revati
]

# Yoni: animal symbol and gender for each nakshatra
NAKSHATRA_YONI = [
    "Horse",    # 0  Ashwini     (M)
    "Elephant", # 1  Bharani     (M)
    "Sheep",    # 2  Krittika    (F)
    "Serpent",  # 3  Rohini      (M)
    "Serpent",  # 4  Mrigashira  (F)
    "Dog",      # 5  Ardra       (F)
    "Cat",      # 6  Punarvasu   (F)
    "Sheep",    # 7  Pushya      (M)
    "Cat",      # 8  Ashlesha    (M)
    "Rat",      # 9  Magha       (M)
    "Rat",      # 10 Purva Phalguni  (F)
    "Cow",      # 11 Uttara Phalguni (M)
    "Buffalo",  # 12 Hasta       (F)
    "Tiger",    # 13 Chitra      (F)
    "Buffalo",  # 14 Swati       (M)
    "Tiger",    # 15 Vishakha    (M)
    "Deer",     # 16 Anuradha    (F)
    "Deer",     # 17 Jyeshtha    (M)
    "Dog",      # 18 Mula        (M)
    "Monkey",   # 19 Purva Ashadha  (M)
    "Mongoose", # 20 Uttara Ashadha (M)
    "Monkey",   # 21 Shravana    (F)
    "Lion",     # 22 Dhanishta   (F)
    "Horse",    # 23 Shatabhisha (F)
    "Lion",     # 24 Purva Bhadrapada (M)
    "Cow",      # 25 Uttara Bhadrapada (F)
    "Elephant", # 26 Revati      (F)
]

NAKSHATRA_YONI_GENDER = [
    "M","M","F","M","F","F","F","M","M","M",
    "F","M","F","F","M","M","F","M","M","M",
    "M","F","F","F","M","F","F",
]

# Varna: spiritual caste by Moon sign (0=Aries … 11=Pisces)
SIGN_VARNA = [
    "Kshatriya", # 0  Aries
    "Vaishya",   # 1  Taurus
    "Shudra",    # 2  Gemini
    "Brahmin",   # 3  Cancer
    "Kshatriya", # 4  Leo
    "Vaishya",   # 5  Virgo
    "Shudra",    # 6  Libra
    "Brahmin",   # 7  Scorpio
    "Kshatriya", # 8  Sagittarius
    "Vaishya",   # 9  Capricorn
    "Shudra",    # 10 Aquarius
    "Brahmin",   # 11 Pisces
]

# Vashya: attraction category by Moon sign
SIGN_VASHYA = [
    "Chatushpada", # 0  Aries
    "Chatushpada", # 1  Taurus
    "Dwipada",     # 2  Gemini
    "Jalachara",   # 3  Cancer
    "Vanachara",   # 4  Leo
    "Dwipada",     # 5  Virgo
    "Dwipada",     # 6  Libra
    "Keeta",       # 7  Scorpio
    "Chatushpada", # 8  Sagittarius
    "Chatushpada", # 9  Capricorn
    "Dwipada",     # 10 Aquarius
    "Jalachara",   # 11 Pisces
]

DASHA_YEARS = {
    "Ketu": 7, "Venus": 20, "Sun": 6, "Moon": 10, "Mars": 7,
    "Rahu": 18, "Jupiter": 16, "Saturn": 19, "Mercury": 17
}

DASHA_SEQUENCE = ["Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury"]

PLANETS = {
    "Sun": swe.SUN, "Moon": swe.MOON, "Mars": swe.MARS,
    "Mercury": swe.MERCURY, "Jupiter": swe.JUPITER, "Venus": swe.VENUS,
    "Saturn": swe.SATURN,
}

PLANET_ORDER = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]

# Panchang constants
TITHIS = [
    "Pratipada", "Dwitiya", "Tritiya", "Chaturthi", "Panchami",
    "Shashthi", "Saptami", "Ashtami", "Navami", "Dashami",
    "Ekadashi", "Dwadashi", "Trayodashi", "Chaturdashi", "Purnima/Amavasya"
]
PAKSHA = ["Shukla", "Krishna"]
VARAS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
VARA_LORDS = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
YOGAS_PANCHANG = [
    "Vishkambha", "Priti", "Ayushman", "Saubhagya", "Shobhana",
    "Atiganda", "Sukarma", "Dhriti", "Shula", "Ganda",
    "Vriddhi", "Dhruva", "Vyaghata", "Harshana", "Vajra",
    "Siddhi", "Vyatipata", "Variyan", "Parigha", "Shiva",
    "Siddha", "Sadhya", "Shubha", "Shukla", "Brahma",
    "Indra", "Vaidhriti"
]
KARANAS = [
    "Bava", "Balava", "Kaulava", "Taitila", "Garija", "Vanija", "Vishti",
    "Shakuni", "Chatushpada", "Naga", "Kimstughna"
]

# Chara Karaka names (8-karaka scheme including Rahu)
KARAKA_NAMES = [
    "Atmakaraka", "Amatyakaraka", "Bhratrikaraka", "Matrikaraka",
    "Putrakaraka", "Gnatikaraka", "Darakaraka"
]
KARAKA_ABBR = ["AK", "AmK", "BK", "MK", "PuK", "GK", "DK"]
KARAKA_SIGNIFIES = [
    "Soul, Self", "Mind, Career", "Siblings, Courage",
    "Mother, Happiness", "Children, Intelligence",
    "Enemies, Obstacles", "Spouse, Partnership"
]

ABBR = {"Sun": "Su", "Moon": "Mo", "Mars": "Ma", "Mercury": "Me",
        "Jupiter": "Ju", "Venus": "Ve", "Saturn": "Sa", "Rahu": "Ra", "Ketu": "Ke"}

# Dignity tables
EXALTATION = {"Sun": 0, "Moon": 1, "Mars": 9, "Mercury": 5, "Jupiter": 3,
              "Venus": 11, "Saturn": 6, "Rahu": 1, "Ketu": 7}
DEBILITATION = {"Sun": 6, "Moon": 7, "Mars": 3, "Mercury": 11, "Jupiter": 9,
                "Venus": 5, "Saturn": 0, "Rahu": 7, "Ketu": 1}
# Moolatrikona: (sign_idx, from_deg, to_deg) — degree range within the sign
MOOLATRIKONA_RANGES = {
    "Sun":     (4,   0, 20),   # Leo 0°–20°
    "Moon":    (1,   4, 20),   # Taurus 4°–20° (0°–3° Taurus = exalted)
    "Mars":    (0,   0, 12),   # Aries 0°–12°
    "Mercury": (5,  16, 20),   # Virgo 16°–20° (0°–15° = exalted)
    "Jupiter": (8,   0, 10),   # Sagittarius 0°–10°
    "Venus":   (6,   0, 15),   # Libra 0°–15°
    "Saturn":  (10,  0, 20),   # Aquarius 0°–20°
}
OWN_SIGNS = {
    "Sun": [4], "Moon": [3], "Mars": [0, 7], "Mercury": [2, 5],
    "Jupiter": [8, 11], "Venus": [1, 6], "Saturn": [9, 10]
}
# MKS houses (from lagna)
MKS_HOUSES = {"Sun": 12, "Moon": 8, "Mars": 7, "Mercury": 7,
              "Jupiter": 3, "Venus": 6, "Saturn": 1}

# Combustion thresholds (degrees from Sun)
COMBUSTION_DEGREES = {
    "Moon": 12, "Mars": 17, "Mercury": 14,
    "Jupiter": 11, "Venus": 10, "Saturn": 15,
}

# Natural planetary friendships (Naisargika Maitri)
FRIENDS = {
    "Sun":     {"Moon", "Mars", "Jupiter"},
    "Moon":    {"Sun", "Mercury"},
    "Mars":    {"Sun", "Moon", "Jupiter"},
    "Mercury": {"Sun", "Venus"},
    "Jupiter": {"Sun", "Moon", "Mars"},
    "Venus":   {"Mercury", "Saturn"},
    "Saturn":  {"Mercury", "Venus"},
}
ENEMIES = {
    "Sun":     {"Venus", "Saturn"},
    "Moon":    set(),
    "Mars":    {"Mercury"},
    "Mercury": {"Moon"},
    "Jupiter": {"Mercury", "Venus"},
    "Venus":   {"Sun", "Moon"},
    "Saturn":  {"Sun", "Moon", "Mars"},
}


def get_house_relation(planet_name, sign_idx):
    """Return 'own', 'friend', 'neutral', or 'enemy' based on sign lord relationship."""
    if planet_name in ("Rahu", "Ketu"):
        return None
    lord = SIGN_LORDS[sign_idx]
    if lord == planet_name:
        return "own"
    if planet_name in OWN_SIGNS and sign_idx in OWN_SIGNS[planet_name]:
        return "own"
    friends = FRIENDS.get(planet_name, set())
    enemies = ENEMIES.get(planet_name, set())
    if lord in friends:
        return "friendly"
    if lord in enemies:
        return "enemy"
    return "neutral"


# ── Helper Functions ────────────────────────────────────────────────────────

def get_sidereal_pos(jd, planet_id):
    flags = swe.FLG_SWIEPH | swe.FLG_SIDEREAL | swe.FLG_SPEED
    result = swe.calc_ut(jd, planet_id, flags)
    lon = result[0][0]
    lat = result[0][1]
    speed = result[0][3]
    return lon % 360, lat, speed


def get_rahu_ketu(jd):
    flags = swe.FLG_SWIEPH | swe.FLG_SIDEREAL
    result = swe.calc_ut(jd, swe.MEAN_NODE, flags)
    rahu = result[0][0] % 360
    ketu = (rahu + 180) % 360
    return rahu, ketu


def lon_to_sign(lon):
    return int(lon / 30)


def lon_to_deg_in_sign(lon):
    return lon % 30


def lon_to_nakshatra(lon):
    nak_span = 360 / 27
    nak_idx = int(lon / nak_span)
    pada = int((lon % nak_span) / (nak_span / 4)) + 1
    return nak_idx, pada


def format_dms(deg):
    d = int(deg)
    m = int((deg - d) * 60)
    s = int(((deg - d) * 60 - m) * 60)
    return f"{d}\u00b0{m:02d}'{s:02d}\""


def get_house(planet_lon, lagna_lon):
    planet_sign = lon_to_sign(planet_lon)
    lagna_sign = lon_to_sign(lagna_lon)
    return ((planet_sign - lagna_sign) % 12) + 1


def divisional_sign(lon, division):
    sign_idx = lon_to_sign(lon)
    deg_in_sign = lon_to_deg_in_sign(lon)

    if division == 1:
        return sign_idx
    elif division == 2:
        part = int(deg_in_sign / 15)
        if sign_idx % 2 == 0:
            return 3 if part == 0 else 4
        else:
            return 4 if part == 0 else 3
    elif division == 3:
        part = int(deg_in_sign / 10)
        return (sign_idx + part * 4) % 12
    elif division == 7:
        part = int(deg_in_sign / (30 / 7))
        if sign_idx % 2 == 0:
            return (sign_idx + part) % 12
        else:
            return (sign_idx + 6 + part) % 12
    elif division == 9:
        part = int(deg_in_sign / (30 / 9))
        element = sign_idx % 4
        element_start = [0, 9, 6, 3][element]
        return (element_start + part) % 12
    elif division == 10:
        part = int(deg_in_sign / 3)
        if sign_idx % 2 == 0:
            return (sign_idx + part) % 12
        else:
            return (sign_idx + 8 + part) % 12
    elif division == 12:
        part = int(deg_in_sign / 2.5)
        return (sign_idx + part) % 12
    elif division == 20:
        part = int(deg_in_sign / 1.5)  # 30° / 20 = 1.5° per division
        # Vimshamsha: element-based starting signs (like D9) to ensure
        # Rahu/Ketu (always 180° apart) land in opposite D20 signs.
        # Fire→Aries, Earth→Capricorn, Air→Libra, Water→Cancer
        element = sign_idx % 4
        element_start = [0, 9, 6, 3][element]
        return (element_start + part) % 12
    elif division == 60:
        part = int(deg_in_sign / 0.5)  # 30° / 60 = 0.5° per division
        # Parashari D60: odd signs from Aries, even from Libra.
        # Add sign_idx offset so opposite signs (same parity) differ by 6.
        if sign_idx % 2 == 0:  # odd sign (0-indexed even = 1st, 3rd, etc.)
            return (sign_idx + part) % 12
        else:  # even sign
            return (sign_idx + 6 + part) % 12
    return sign_idx


# ── Dignity detection ──────────────────────────────────────────────────────

def get_dignity(planet_name, sign_idx, house, deg_in_sign=0, is_divisional=False):
    """Return dignity string or None.

    deg_in_sign: degree within the sign (0–30) for degree-range checks.
    is_divisional: True for D9, D10 etc. — skips degree-based checks (moolatrikona)
                   since divisional charts don't have meaningful degree positions.
    """
    # Moon: Moolatrikona (Taurus 4°–20°) takes precedence over exaltation
    if not is_divisional and planet_name == "Moon" and sign_idx == 1:
        if 4 <= deg_in_sign <= 20:
            return "moolatrikona"
        # 0°–3° Taurus → exalted (falls through below)

    # Exaltation (sign-based)
    if planet_name in EXALTATION and EXALTATION[planet_name] == sign_idx:
        return "exalted"

    # Debilitation (sign-based)
    if planet_name in DEBILITATION and DEBILITATION[planet_name] == sign_idx:
        return "debilitated"

    # Moolatrikona (degree-range-based) — only for D1 where degrees are meaningful
    if not is_divisional and planet_name in MOOLATRIKONA_RANGES:
        mt_sign, mt_from, mt_to = MOOLATRIKONA_RANGES[planet_name]
        if sign_idx == mt_sign and mt_from <= deg_in_sign <= mt_to:
            return "moolatrikona"

    # Maranakarak Sthana
    if planet_name in MKS_HOUSES and MKS_HOUSES[planet_name] == house:
        return "mks"

    # Own sign
    if planet_name in OWN_SIGNS and sign_idx in OWN_SIGNS[planet_name]:
        return "own"

    return None


def dignity_arrow(dignity):
    if dignity == "exalted":
        return "\U0001F31F"   # 🌟
    elif dignity == "debilitated":
        return "\U0001F494"   # 💔
    elif dignity == "moolatrikona":
        return "\U0001F48E"   # 💎
    elif dignity == "mks":
        return "\u26A0\uFE0F" # ⚠️
    return ""


# ── Core calculation ───────────────────────────────────────────────────────

def calculate_all(jd, lat, lon, ayanamsa):
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    houses_result = swe.houses(jd, lat, lon, b'E')
    asc_tropical = houses_result[1][0]
    asc_sidereal = (asc_tropical - ayanamsa) % 360

    data = {"lagna": asc_sidereal, "planets": {}}

    for name, pid in PLANETS.items():
        planet_lon, lat, speed = get_sidereal_pos(jd, pid)
        data["planets"][name] = {"lon": planet_lon, "lat": lat, "speed": speed, "retro": speed < 0}

    rahu, ketu = get_rahu_ketu(jd)
    data["planets"]["Rahu"] = {"lon": rahu, "lat": 0.0, "speed": 0, "retro": True}
    data["planets"]["Ketu"] = {"lon": ketu, "lat": 0.0, "speed": 0, "retro": True}

    return data


# ── Build chart houses dict ────────────────────────────────────────────────

def build_chart_houses(data, division):
    """Build {house_number: [abbr+arrow, ...]} for a divisional chart."""
    lagna_lon = data["lagna"]
    if division == 1:
        lagna_sign = lon_to_sign(lagna_lon)
    else:
        lagna_sign = divisional_sign(lagna_lon, division)

    # Collect planets with their degree for sorting
    house_entries = {}  # house -> [(degree_in_sign, label), ...]
    retro_planets = []
    planet_degs = {}   # abbr -> degree string for display
    for name in PLANET_ORDER:
        p = data["planets"][name]
        if division == 1:
            sign_idx = lon_to_sign(p["lon"])
        else:
            sign_idx = divisional_sign(p["lon"], division)

        house = ((sign_idx - lagna_sign) % 12) + 1
        deg_in_sign = p["lon"] % 30  # degree within the sign (0-30)

        # Compute dignity in this chart (pass deg_in_sign for D1; others use sign only)
        d1_deg = deg_in_sign if division == 1 else 0
        dig = get_dignity(name, sign_idx, house, d1_deg)
        arrow = dignity_arrow(dig)
        label = ABBR[name] + arrow

        house_entries.setdefault(house, [])
        house_entries[house].append((deg_in_sign, label))

        # Track retrograde planets (exclude Rahu/Ketu as they are always retrograde)
        if p["retro"] and name not in ("Rahu", "Ketu"):
            retro_planets.append(ABBR[name])

        # Degree within sign for D1 display
        if division == 1:
            planet_degs[ABBR[name]] = str(int(deg_in_sign))

    # Sort planets within each house by degree and extract labels
    houses = {}
    for h, entries in house_entries.items():
        entries.sort(key=lambda x: x[0])
        houses[h] = [label for _, label in entries]

    result = {"lagna_sign": lagna_sign + 1, "houses": houses, "retro": retro_planets}
    if division == 1:
        result["planet_degs"] = planet_degs
    return result


# ── Arudha Lagna ───────────────────────────────────────────────────────────

def calculate_arudha_lagna(data):
    lagna_sign = lon_to_sign(data["lagna"])
    lagna_lord = SIGN_LORDS[lagna_sign]
    lord_sign = lon_to_sign(data["planets"][lagna_lord]["lon"])
    dist = (lord_sign - lagna_sign) % 12
    al_sign = (lord_sign + dist) % 12
    al_house = ((al_sign - lagna_sign) % 12) + 1
    return al_house


# ── Dasha calculation ─────────────────────────────────────────────────────

def calculate_dasha(data, birth_dt):
    moon_lon = data["planets"]["Moon"]["lon"]
    nak_idx, pada = lon_to_nakshatra(moon_lon)
    nak_lord = NAKSHATRA_LORDS[nak_idx]

    nak_span = 360 / 27
    pos_in_nak = moon_lon % nak_span
    fraction_remaining = 1 - (pos_in_nak / nak_span)

    start_idx = DASHA_SEQUENCE.index(nak_lord)

    # Maha Dasha periods
    maha = []
    current_start = birth_dt
    first_years = DASHA_YEARS[nak_lord] * fraction_remaining
    first_end = current_start + timedelta(days=first_years * 365.25)
    maha.append({"lord": nak_lord, "start": current_start, "end": first_end, "years": round(first_years, 2)})
    current_start = first_end

    for i in range(1, 10):
        idx = (start_idx + i) % 9
        lord = DASHA_SEQUENCE[idx]
        years = DASHA_YEARS[lord]
        end = current_start + timedelta(days=years * 365.25)
        maha.append({"lord": lord, "start": current_start, "end": end, "years": years})
        current_start = end

    def fmt_dt(dt):
        return dt.strftime("%d-%b-%Y")

    # Antardasha for all maha dashas
    antar = {}
    for md in maha:
        md_lord = md["lord"]
        md_lord_idx = DASHA_SEQUENCE.index(md_lord)
        ad_list = []
        ad_start = md["start"]
        for j in range(9):
            ad_idx = (md_lord_idx + j) % 9
            ad_lord = DASHA_SEQUENCE[ad_idx]
            ad_years = DASHA_YEARS[md_lord] * DASHA_YEARS[ad_lord] / 120
            ad_end = ad_start + timedelta(days=ad_years * 365.25)
            ad_list.append({"lord": ad_lord, "start": fmt_dt(ad_start), "end": fmt_dt(ad_end)})
            ad_start = ad_end
        antar[md_lord] = ad_list

    # Pratyantardasha for each antardasha
    pratyantar = {}
    for md_lord, ad_list in antar.items():
        for ad in ad_list:
            ad_lord = ad["lord"]
            key = f"{md_lord}/{ad_lord}"
            pd_list = []
            ad_lord_idx = DASHA_SEQUENCE.index(ad_lord)
            md_years = DASHA_YEARS[md_lord]
            ad_years_total = md_years * DASHA_YEARS[ad_lord] / 120
            pad_start = datetime.strptime(ad["start"], "%d-%b-%Y")
            for k in range(9):
                pad_idx = (ad_lord_idx + k) % 9
                pad_lord = DASHA_SEQUENCE[pad_idx]
                pad_years = ad_years_total * DASHA_YEARS[pad_lord] / 120
                pad_end = pad_start + timedelta(days=pad_years * 365.25)
                pd_list.append({"lord": pad_lord, "start": fmt_dt(pad_start), "end": fmt_dt(pad_end)})
                pad_start = pad_end
            pratyantar[key] = pd_list

    # Format maha dasha dates
    maha_fmt = [{"lord": m["lord"], "start": fmt_dt(m["start"]), "end": fmt_dt(m["end"]),
                 "years": m["years"]} for m in maha]

    return {
        "moon_nakshatra": NAKSHATRAS[nak_idx],
        "moon_pada": pada,
        "dasha_lord": nak_lord,
        "maha": maha_fmt,
        "antar": antar,
        "pratyantar": pratyantar
    }


# ── Sade Sati & Dhaiya ───────────────────────────────────────────────────

def calculate_sadesati(moon_lon, jd_birth):
    """Calculate Sade Sati and Dhaiya periods across the native's lifetime.

    Sade Sati: Saturn transiting 12th, 1st, 2nd signs from natal Moon.
    Dhaiya (Kantaka/Ashtama Shani): Saturn in 4th or 8th from Moon.
    """
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    moon_sign = lon_to_sign(moon_lon)  # 0-11

    # Signs that trigger Sade Sati (12th, 1st, 2nd from Moon)
    ss_signs = [(moon_sign - 1) % 12, moon_sign, (moon_sign + 1) % 12]
    ss_phase_names = ["Rising", "Peak", "Setting"]

    # Signs that trigger Dhaiya (4th, 8th from Moon)
    dh_signs = [(moon_sign + 3) % 12, (moon_sign + 7) % 12]
    dh_labels = {(moon_sign + 3) % 12: "4th from Moon", (moon_sign + 7) % 12: "8th from Moon"}

    def saturn_sidereal_sign(jd):
        """Get Saturn's sidereal sign index (0-11) at a given Julian day."""
        pos, _ = swe.calc_ut(jd, swe.SATURN)
        ayanamsa = swe.get_ayanamsa_ut(jd)
        sid_lon = (pos[0] - ayanamsa) % 360
        return int(sid_lon / 30)

    def fmt_jd(jd):
        """Convert Julian day to dd-Mon-YYYY string."""
        y, m, d, h = swe.revjul(jd)
        dt = datetime(int(y), int(m), int(d))
        return dt.strftime("%d-%b-%Y")

    # Coarse scan: step through 90 years in 15-day increments
    scan_years = 90
    step_days = 15.0
    total_steps = int(scan_years * 365.25 / step_days)

    # Build list of (jd, sign) samples
    transitions = []  # list of (jd_crossing, old_sign, new_sign)
    prev_sign = saturn_sidereal_sign(jd_birth)

    for i in range(1, total_steps + 1):
        jd_now = jd_birth + i * step_days
        cur_sign = saturn_sidereal_sign(jd_now)
        if cur_sign != prev_sign:
            # Binary search for exact crossing point
            lo = jd_now - step_days
            hi = jd_now
            for _ in range(25):
                mid = (lo + hi) / 2
                mid_sign = saturn_sidereal_sign(mid)
                if mid_sign == prev_sign:
                    lo = mid
                else:
                    hi = mid
            transitions.append((hi, prev_sign, cur_sign))
            prev_sign = cur_sign

    # Build sign-occupancy intervals
    intervals = []  # list of (start_jd, end_jd, sign)
    if transitions:
        # First interval: birth to first transition
        intervals.append((jd_birth, transitions[0][0], saturn_sidereal_sign(jd_birth)))
        for i in range(len(transitions) - 1):
            intervals.append((transitions[i][0], transitions[i + 1][0], transitions[i][2]))
        # Last interval: last transition to end of scan
        jd_end = jd_birth + scan_years * 365.25
        intervals.append((transitions[-1][0], jd_end, transitions[-1][2]))

    # Filter for Sade Sati phases
    ss_intervals = []  # (start_jd, end_jd, sign, phase_name)
    for start, end, sign in intervals:
        if sign in ss_signs:
            phase_idx = ss_signs.index(sign)
            ss_intervals.append((start, end, sign, ss_phase_names[phase_idx]))

    # Filter for Dhaiya periods
    dh_intervals = []  # (start_jd, end_jd, sign, position_label)
    for start, end, sign in intervals:
        if sign in dh_signs:
            dh_intervals.append((start, end, sign, dh_labels[sign]))

    # Group Sade Sati intervals into cycles
    # Adjacent SS intervals with gap < 365 days belong to the same cycle
    # (Saturn retrograde can create gaps of several months)
    cycles = []
    current_cycle_phases = []
    for iv in ss_intervals:
        if current_cycle_phases:
            gap = iv[0] - current_cycle_phases[-1][1]
            if gap > 365:
                # Start new cycle
                cycles.append(current_cycle_phases)
                current_cycle_phases = []
        current_cycle_phases.append(iv)
    if current_cycle_phases:
        cycles.append(current_cycle_phases)

    # Format cycles
    formatted_cycles = []
    for i, phases in enumerate(cycles):
        cycle_start = phases[0][0]
        cycle_end = phases[-1][1]
        dur_years = (cycle_end - cycle_start) / 365.25
        formatted_phases = []
        for start, end, sign, phase_name in phases:
            formatted_phases.append({
                "sign": SIGNS[sign],
                "phase": phase_name,
                "start": fmt_jd(start),
                "end": fmt_jd(end)
            })
        formatted_cycles.append({
            "cycle_number": i + 1,
            "start": fmt_jd(cycle_start),
            "end": fmt_jd(cycle_end),
            "duration_years": round(dur_years, 1),
            "phases": formatted_phases
        })

    # Format Dhaiya periods
    formatted_dhaiya = []
    for start, end, sign, pos_label in dh_intervals:
        formatted_dhaiya.append({
            "sign": SIGNS[sign],
            "position": pos_label,
            "start": fmt_jd(start),
            "end": fmt_jd(end)
        })

    # Determine current status
    now_jd = swe.julday(
        datetime.utcnow().year, datetime.utcnow().month, datetime.utcnow().day,
        datetime.utcnow().hour + datetime.utcnow().minute / 60.0
    )
    current_saturn_sign = saturn_sidereal_sign(now_jd)

    current_status = {"active": False, "type": None, "phase": None, "sign": SIGNS[current_saturn_sign]}
    if current_saturn_sign in ss_signs:
        phase_idx = ss_signs.index(current_saturn_sign)
        current_status = {
            "active": True,
            "type": "sadesati",
            "phase": ss_phase_names[phase_idx],
            "sign": SIGNS[current_saturn_sign]
        }
    elif current_saturn_sign in dh_signs:
        current_status = {
            "active": True,
            "type": "dhaiya",
            "phase": dh_labels[current_saturn_sign],
            "sign": SIGNS[current_saturn_sign]
        }

    return {
        "moon_sign": SIGNS[moon_sign],
        "sadesati_signs": [SIGNS[s] for s in ss_signs],
        "dhaiya_signs": [SIGNS[s] for s in dh_signs],
        "cycles": formatted_cycles,
        "dhaiya": formatted_dhaiya,
        "current_status": current_status
    }


# ── Ashtakavarga ──────────────────────────────────────────────────────────

def calculate_ashtakavarga(data):
    planet_lons = {}
    for name in ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]:
        planet_lons[name] = data["planets"][name]["lon"]

    lagna = data["lagna"]
    lagna_sign = lon_to_sign(lagna)

    benefic_houses = {
        "Sun":     [1, 2, 4, 7, 8, 9, 10, 11],
        "Moon":    [3, 6, 7, 8, 10, 11],
        "Mars":    [3, 5, 6, 10, 11],
        "Mercury": [1, 3, 5, 6, 9, 10, 11, 12],
        "Jupiter": [1, 2, 3, 4, 7, 8, 10, 11],
        "Venus":   [1, 2, 3, 4, 5, 8, 9, 11, 12],
        "Saturn":  [3, 5, 6, 11],
        "Lagna":   [3, 6, 10, 11],
    }

    total_points = [0] * 12

    for contributor_name, houses in benefic_houses.items():
        if contributor_name == "Lagna":
            ref_sign = lagna_sign
        else:
            ref_sign = lon_to_sign(planet_lons[contributor_name])

        for h in houses:
            target_sign = (ref_sign + h - 1) % 12
            total_points[target_sign] += 1

    return [{"sign": SIGNS[i], "points": total_points[i]} for i in range(12)]


# ── Yoga detection ────────────────────────────────────────────────────────

def detect_yogas(data):
    lagna = data["lagna"]
    lagna_sign = lon_to_sign(lagna)
    yogas_found = []

    positions = {}
    for name, p in data["planets"].items():
        sign = lon_to_sign(p["lon"])
        house = get_house(p["lon"], lagna)
        positions[name] = {"sign": sign, "house": house, "lon": p["lon"]}

    def same_sign(p1, p2):
        return positions[p1]["sign"] == positions[p2]["sign"]

    def in_kendra(planet):
        return positions[planet]["house"] in [1, 4, 7, 10]

    def in_trikona(planet):
        return positions[planet]["house"] in [1, 5, 9]

    def signs_apart(p1, p2):
        return (positions[p2]["sign"] - positions[p1]["sign"]) % 12

    def is_combust(planet):
        if planet not in COMBUSTION_DEGREES:
            return False
        diff = abs(positions[planet]["lon"] - positions["Sun"]["lon"])
        if diff > 180:
            diff = 360 - diff
        return diff <= COMBUSTION_DEGREES[planet]

    def is_debilitated(planet):
        return planet in DEBILITATION and positions[planet]["sign"] == DEBILITATION[planet]

    def is_own_or_exalt(planet):
        p_sign = positions[planet]["sign"]
        return (p_sign in OWN_SIGNS.get(planet, [])) or (EXALTATION.get(planet) == p_sign)

    moon_sign = positions["Moon"]["sign"]

    # 1. Gajakesari — Jupiter in kendra from Moon; skip if Jupiter combust or debilitated
    jup_from_moon = (positions["Jupiter"]["sign"] - moon_sign) % 12
    if jup_from_moon in [0, 3, 6, 9]:
        if not is_combust("Jupiter") and not is_debilitated("Jupiter"):
            pos_label = "conjunct Moon" if jup_from_moon == 0 else "in kendra from Moon"
            yogas_found.append({"name": "Gajakesari Yoga",
                                "description": f"Jupiter {pos_label} \u2014 wisdom, fame, good fortune",
                                "type": "positive"})

    # 2. Budhaditya — Sun-Mercury conjunct; skip if Mercury cazimi (<3\u00b0); partial if combust (3-14\u00b0)
    if same_sign("Sun", "Mercury"):
        merc_diff = abs(positions["Mercury"]["lon"] - positions["Sun"]["lon"])
        if merc_diff > 180:
            merc_diff = 360 - merc_diff
        if merc_diff >= 3:
            if merc_diff <= 14:
                yogas_found.append({"name": "Budhaditya Yoga",
                                    "description": f"Sun-Mercury conjunction ({merc_diff:.1f}\u00b0 apart, Mercury combust) \u2014 intelligence present but expression suppressed",
                                    "type": "neutral"})
            else:
                yogas_found.append({"name": "Budhaditya Yoga",
                                    "description": "Sun-Mercury conjunction \u2014 sharp intellect, communication skills, analytical mind",
                                    "type": "positive"})

    # 3. Chandra-Mangala — Moon-Mars conjunction or opposition; note Mars debilitation
    mars_from_moon = signs_apart("Moon", "Mars")
    if mars_from_moon == 0 or mars_from_moon == 6:
        pos_label = "conjunct Moon" if mars_from_moon == 0 else "opposing Moon"
        mars_note = " (Mars debilitated in Cancer \u2014 drive frustrated)" if is_debilitated("Mars") else ""
        yogas_found.append({"name": "Chandra-Mangala Yoga",
                            "description": f"Moon-Mars {pos_label} \u2014 wealth through enterprise and bold action{mars_note}",
                            "type": "positive"})

    # 4. Amala Yoga — benefic in 10th; skip if that benefic is debilitated or combust
    benefics = ["Jupiter", "Venus", "Mercury"]
    for b in benefics:
        if is_debilitated(b) or is_combust(b):
            continue
        if positions[b]["house"] == 10:
            yogas_found.append({"name": "Amala Yoga",
                                "description": f"{b} in 10th house \u2014 virtuous deeds, good public reputation",
                                "type": "positive"})
    for b in benefics:
        if is_debilitated(b) or is_combust(b):
            continue
        h_from_moon = (positions[b]["sign"] - moon_sign) % 12
        if h_from_moon == 9:
            yogas_found.append({"name": "Amala Yoga (from Moon)",
                                "description": f"{b} in 10th from Moon \u2014 good reputation, virtuous conduct",
                                "type": "positive"})

    # 5. Pancha Mahapurusha — planet in kendra in own/exalt sign; flag combust as partial
    mahapurusha = {"Mars": "Ruchaka", "Mercury": "Bhadra", "Jupiter": "Hamsa",
                   "Venus": "Malavya", "Saturn": "Shasha"}
    mp_own_signs = {
        "Mars": [0, 7], "Mercury": [2, 5], "Jupiter": [8, 11],
        "Venus": [1, 6], "Saturn": [9, 10]
    }
    mp_exalt_signs = {"Mars": 9, "Mercury": 5, "Jupiter": 3, "Venus": 11, "Saturn": 6}
    for planet, yoga_name in mahapurusha.items():
        p_sign = positions[planet]["sign"]
        if in_kendra(planet) and (p_sign in mp_own_signs[planet] or p_sign == mp_exalt_signs[planet]):
            sign_name = SIGNS[p_sign]
            combust_note = " (partial \u2014 planet combust)" if is_combust(planet) else ""
            yogas_found.append({"name": f"{yoga_name} Yoga (Pancha Mahapurusha)",
                                "description": f"{planet} in kendra in {sign_name}{combust_note} \u2014 power, status, exemplary qualities",
                                "type": "positive"})

    # 6. Raja Yoga — kendra lord + trikona lord conjunct; deduplicate pairs; skip if combust
    kendra_houses = [1, 4, 7, 10]
    trikona_houses = [1, 5, 9]
    kendra_lords = set()
    trikona_lords = set()
    for h in kendra_houses:
        sign_of_house = (lagna_sign + h - 1) % 12
        kendra_lords.add(SIGN_LORDS[sign_of_house])
    for h in trikona_houses:
        sign_of_house = (lagna_sign + h - 1) % 12
        trikona_lords.add(SIGN_LORDS[sign_of_house])
    _seen_raja = set()
    for kl in kendra_lords:
        for tl in trikona_lords:
            if kl == tl or kl not in positions or tl not in positions:
                continue
            if not same_sign(kl, tl):
                continue
            pair = frozenset([kl, tl])
            if pair in _seen_raja:
                continue
            _seen_raja.add(pair)
            if is_combust(kl) or is_combust(tl):
                continue  # combusted lord \u2014 yoga doesn\u2019t manifest
            placement = positions[kl]["house"]
            dusthana_note = f" (placed in H{placement}, dusthana \u2014 delayed)" if placement in [6, 8, 12] else ""
            yogas_found.append({"name": "Raja Yoga",
                                "description": f"{kl} (kendra lord) + {tl} (trikona lord) conjunct{dusthana_note} \u2014 power and authority",
                                "type": "positive"})

    # 7. Dhana Yoga — 2nd/11th lord in kendra/trikona; mark partial if debilitated/combust
    for h in [2, 11]:
        sign_of_house = (lagna_sign + h - 1) % 12
        lord = SIGN_LORDS[sign_of_house]
        if lord in positions and (in_kendra(lord) or in_trikona(lord)):
            if is_debilitated(lord) or is_combust(lord):
                yogas_found.append({"name": "Dhana Yoga (partial)",
                                    "description": f"{lord} (lord of H{h}) in kendra/trikona but debilitated/combust \u2014 wealth potential reduced",
                                    "type": "neutral"})
            else:
                yogas_found.append({"name": "Dhana Yoga",
                                    "description": f"{lord} (lord of H{h}) in kendra/trikona \u2014 wealth and financial prosperity",
                                    "type": "positive"})

    # 8. Vipareeta Raja Yoga — dusthana lord in dusthana; deduplicate per planet
    dusthana = [6, 8, 12]
    _seen_vipareeta = set()
    for h in dusthana:
        sign_of_house = (lagna_sign + h - 1) % 12
        lord = SIGN_LORDS[sign_of_house]
        if lord in positions and positions[lord]["house"] in dusthana and lord not in _seen_vipareeta:
            _seen_vipareeta.add(lord)
            yogas_found.append({"name": "Vipareeta Raja Yoga",
                                "description": f"{lord} (lord of H{h}) in dusthana \u2014 rise after setbacks, gains through adversity",
                                "type": "positive"})

    # 9. Kemadruma Yoga — no planets in 2nd/12th from Moon; check classical cancellations
    check_planets = ["Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    sign_2_from_moon = (moon_sign + 1) % 12
    sign_12_from_moon = (moon_sign - 1) % 12
    has_planet_near_moon = any(
        positions[cp]["sign"] in [sign_2_from_moon, sign_12_from_moon]
        for cp in check_planets
    )
    if not has_planet_near_moon:
        moon_in_kendra = positions["Moon"]["house"] in [1, 4, 7, 10]
        moon_aspected_by_benefic = any(
            (positions[b]["sign"] + 6) % 12 == moon_sign for b in ["Jupiter", "Venus", "Mercury"]
        )
        planet_in_kendra_from_lagna = any(
            positions[cp]["house"] in [1, 4, 7, 10] for cp in check_planets
        )
        if moon_in_kendra or moon_aspected_by_benefic or planet_in_kendra_from_lagna:
            yogas_found.append({"name": "Kemadruma Yoga (cancelled)",
                                "description": "No planets in 2nd/12th from Moon \u2014 Kemadruma cancelled (Moon in kendra / benefic aspect / planet in kendra)",
                                "type": "neutral"})
        else:
            yogas_found.append({"name": "Kemadruma Yoga",
                                "description": "No planets in 2nd/12th from Moon \u2014 emotional isolation, difficulty finding support",
                                "type": "caution"})

    # 10. Vish Yoga — Moon + Saturn conjunct; reduced if Saturn in dignity
    if positions["Moon"]["sign"] == positions["Saturn"]["sign"]:
        if is_own_or_exalt("Saturn"):
            yogas_found.append({"name": "Vish Yoga (reduced)",
                                "description": "Moon-Saturn conjunct; Saturn in dignity \u2014 karmic discipline, emotional depth (toxicity reduced)",
                                "type": "neutral"})
        else:
            yogas_found.append({"name": "Vish Yoga",
                                "description": "Moon and Saturn conjunct \u2014 emotional heaviness, delays, karmic lessons",
                                "type": "caution"})

    # 11. Saraswati Yoga — Jupiter, Venus, Mercury all in good houses AND at least one in own/exalt
    good_houses = [1, 2, 4, 5, 7, 9, 10]
    if (all(positions[p]["house"] in good_houses for p in ["Jupiter", "Venus", "Mercury"]) and
            any(is_own_or_exalt(p) for p in ["Jupiter", "Venus", "Mercury"])):
        yogas_found.append({"name": "Saraswati Yoga",
                            "description": "Jupiter, Venus, Mercury well-placed (at least one in own/exaltation) \u2014 exceptional learning, arts, wisdom",
                            "type": "positive"})

    # 12. Parivartana Yoga (mutual sign exchange — D1 only)
    _PARIVARTAN_PLANETS = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    _seen_pairs = set()
    for _pa in _PARIVARTAN_PLANETS:
        for _pb in _PARIVARTAN_PLANETS:
            if _pa == _pb or frozenset([_pa, _pb]) in _seen_pairs:
                continue
            if (SIGN_LORDS[positions[_pa]["sign"]] == _pb and
                    SIGN_LORDS[positions[_pb]["sign"]] == _pa):
                _seen_pairs.add(frozenset([_pa, _pb]))
                _houses_a = sorted([((s - lagna_sign) % 12) + 1
                                    for s in range(12) if SIGN_LORDS[s] == _pa])
                _houses_b = sorted([((s - lagna_sign) % 12) + 1
                                    for s in range(12) if SIGN_LORDS[s] == _pb])
                _all_h = set(_houses_a + _houses_b)
                if _all_h & {6, 8, 12}:
                    _subtype, _ytype = "Dainya Parivartana", "caution"
                    _effect = "dusthana exchange \u2014 hardship with eventual release"
                elif _all_h <= {1, 4, 5, 7, 9, 10}:
                    _subtype, _ytype = "Maha Parivartana", "positive"
                    _effect = "powerful kendra/trikona exchange \u2014 mutual strength, rise in life"
                else:
                    _subtype, _ytype = "Kahala Parivartana", "neutral"
                    _effect = "mixed results \u2014 depends on dasha and planetary dignity"
                _ha_str = "/".join(str(h) for h in _houses_a)
                _hb_str = "/".join(str(h) for h in _houses_b)
                yogas_found.append({
                    "name": _subtype + " Yoga",
                    "description": (f"{_pa} (H{_ha_str}) \u2194 {_pb} (H{_hb_str}) "
                                    f"mutual sign exchange \u2014 {_effect}"),
                    "type": _ytype,
                })

    # 13. Graha Yuddha (planetary war — D1 only)
    _YUDDHA_PLANETS = ["Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    for _i, _pa in enumerate(_YUDDHA_PLANETS):
        for _pb in _YUDDHA_PLANETS[_i + 1:]:
            _diff = abs(positions[_pa]["lon"] - positions[_pb]["lon"])
            if _diff > 180:
                _diff = 360 - _diff
            if _diff <= 1.0:
                _lat_a = data["planets"][_pa].get("lat", 0)
                _lat_b = data["planets"][_pb].get("lat", 0)
                _winner = _pa if _lat_a >= _lat_b else _pb
                _loser  = _pb if _winner == _pa else _pa
                yogas_found.append({
                    "name": "Graha Yuddha",
                    "description": (f"{_pa}\u2013{_pb} planetary war ({_diff:.2f}\u00b0 apart) \u2014 "
                                    f"{_winner} wins, {_loser} loses strength; "
                                    f"{_loser}\u2019s significations weakened in this chart"),
                    "type": "caution",
                })

    # 14. Gita Yoga — Jupiter + Mercury in same sign
    if same_sign("Jupiter", "Mercury"):
        yogas_found.append({
            "name": "Gita Yoga",
            "description": "Jupiter-Mercury conjunction \u2014 philosophical intellect, gift for scripture and teaching, wisdom in speech",
            "type": "positive",
        })

    # 15. Guru Chandal Yoga — Jupiter + Rahu in same sign
    if same_sign("Jupiter", "Rahu"):
        yogas_found.append({
            "name": "Guru Chandal Yoga",
            "description": "Jupiter-Rahu conjunction \u2014 unconventional wisdom, foreign/outsider influences on dharma; can accelerate spiritual growth but also bring confusion",
            "type": "caution",
        })

    # 16. Paraspara Yoga — two planets in mutual aspect (both look at each other)
    # Every planet has 7th aspect. Mars also has 4th+8th, Jupiter 5th+9th, Saturn 3rd+10th.
    # The only mutual non-7th pair possible is Mars (4th) \u2194 Saturn (10th) when 4 signs apart.
    _PARA_PLANETS = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    _PARA_SPECIAL = {
        "Mars":    {3, 7},   # 4th (offset 3) and 8th (offset 7)
        "Jupiter": {4, 8},   # 5th and 9th
        "Saturn":  {2, 9},   # 3rd and 10th
    }
    _BENEFIC_SET = {"Jupiter", "Venus", "Mercury", "Moon"}

    def _aspects(p_from, p_to):
        offset = (positions[p_to]["sign"] - positions[p_from]["sign"]) % 12
        return offset == 6 or offset in _PARA_SPECIAL.get(p_from, set())

    _seen_para = set()
    for _i, _pa in enumerate(_PARA_PLANETS):
        for _pb in _PARA_PLANETS[_i + 1:]:
            pair = frozenset([_pa, _pb])
            if pair in _seen_para:
                continue
            if _aspects(_pa, _pb) and _aspects(_pb, _pa):
                _seen_para.add(pair)
                _ha = positions[_pa]["house"]
                _hb = positions[_pb]["house"]
                _offset = (positions[_pb]["sign"] - positions[_pa]["sign"]) % 12
                if _offset == 6:
                    _asp_label = "mutual 7th aspect"
                else:
                    _asp_label = f"mutual {_offset + 1}th/{(12 - _offset) + 1}th aspect"
                _both_b = _pa in _BENEFIC_SET and _pb in _BENEFIC_SET
                _both_m = _pa not in _BENEFIC_SET and _pb not in _BENEFIC_SET
                _ytype = "positive" if _both_b else ("caution" if _both_m else "neutral")
                yogas_found.append({
                    "name": "Paraspara Yoga",
                    "description": (f"{_pa} (H{_ha}) \u2194 {_pb} (H{_hb}) {_asp_label} \u2014 "
                                    f"each planet activates the other\u2019s significations; "
                                    f"effects depend on dignity and house lordship"),
                    "type": _ytype,
                })

    return yogas_found


# ── Aspects ───────────────────────────────────────────────────────────────

def calculate_aspects(data):
    lagna = data["lagna"]
    lagna_sign = lon_to_sign(lagna)
    positions = {}
    for name, p in data["planets"].items():
        positions[name] = lon_to_sign(p["lon"])

    special_aspects = {
        "Mars": [4, 8],
        "Jupiter": [5, 9],
        "Saturn": [3, 10],
    }

    all_planets = PLANET_ORDER
    planet_to_planet = []

    for planet in all_planets:
        p_sign = positions[planet]
        aspect_signs = [(p_sign + 6) % 12]
        if planet in special_aspects:
            for sp in special_aspects[planet]:
                aspect_signs.append((p_sign + sp - 1) % 12)

        for target_planet in all_planets:
            if target_planet == planet:
                continue
            t_sign = positions[target_planet]
            if t_sign in aspect_signs:
                dist = (t_sign - p_sign) % 12 + 1
                aspect_type = "7th" if dist == 7 else f"{dist}th (special)"
                planet_to_planet.append({"from": planet, "to": target_planet, "type": aspect_type})

    # Special house aspects
    special_house = []
    for planet in ["Mars", "Jupiter", "Saturn", "Rahu"]:
        p_sign = positions[planet]
        p_house = ((p_sign - lagna_sign) % 12) + 1
        aspected = [((p_sign + 6 - lagna_sign) % 12) + 1]
        asp_rules = special_aspects.get(planet, [])
        # Rahu gets Jupiter-like special aspects
        if planet == "Rahu":
            asp_rules = [5, 9]
        for sp in asp_rules:
            aspected.append(((p_sign + sp - 1 - lagna_sign) % 12) + 1)
        special_house.append({"planet": planet, "house": p_house, "aspects": aspected})

    return {"planet_to_planet": planet_to_planet, "special": special_house}


# ── Bhava chart ───────────────────────────────────────────────────────────

def build_bhava(data):
    lagna = data["lagna"]
    lagna_deg = lon_to_deg_in_sign(lagna)
    bhava = []

    for h in range(1, 13):
        cusp_start = (lagna + (h - 1) * 30) % 360
        cusp_end = (cusp_start + 30) % 360
        sign_start = lon_to_sign(cusp_start)
        sign_end = lon_to_sign(cusp_end)

        planets_in = []
        for name in PLANET_ORDER:
            p = data["planets"][name]
            if get_house(p["lon"], lagna) == h:
                planets_in.append(name)

        # Format cusp labels: "23°01' Gemini"
        deg_fmt = f"{int(lagna_deg)}\u00b0{int((lagna_deg - int(lagna_deg)) * 60):02d}'"
        bhava.append({
            "house": h,
            "start": f"{deg_fmt} {SIGNS[sign_start]}",
            "end": f"{deg_fmt} {SIGNS[sign_end]}",
            "planets": planets_in
        })

    return bhava


# ── Panchang ──────────────────────────────────────────────────────────────

def calculate_panchang(jd, data, birth_dt):
    """Calculate the five limbs of Panchang: Tithi, Vara, Nakshatra, Yoga, Karana."""
    # Get tropical Sun and Moon for tithi (need tropical longitudes)
    sun_trop = swe.calc_ut(jd, swe.SUN, swe.FLG_SWIEPH)[0][0] % 360
    moon_trop = swe.calc_ut(jd, swe.MOON, swe.FLG_SWIEPH)[0][0] % 360

    # Tithi: based on Moon-Sun elongation (each tithi = 12°)
    elongation = (moon_trop - sun_trop) % 360
    tithi_idx = int(elongation / 12)
    tithi_num = tithi_idx + 1
    paksha = PAKSHA[0] if tithi_idx < 15 else PAKSHA[1]
    tithi_in_paksha = (tithi_idx % 15) + 1
    tithi_name = TITHIS[tithi_idx % 15]

    # Vara (weekday) — from Julian day
    # int(JD + 0.5) % 7 gives: 0=Mon,1=Tue,2=Wed,3=Thu,4=Fri,5=Sat,6=Sun
    day_num = int(jd + 0.5) % 7  # 0=Mon
    # Remap to VARAS index: Sun=0,Mon=1,...,Sat=6
    vara_map = [1, 2, 3, 4, 5, 6, 0]  # Mon→1, Tue→2, ..., Sun→0
    vara_index = vara_map[day_num]
    vara_name = VARAS[vara_index]
    vara_lord = VARA_LORDS[vara_index]

    # Nakshatra (Moon's sidereal nakshatra — already available)
    moon_lon = data["planets"]["Moon"]["lon"]
    nak_idx, nak_pada = lon_to_nakshatra(moon_lon)

    # Yoga: (Sun_sid + Moon_sid) / 13°20'
    sun_sid = data["planets"]["Sun"]["lon"]
    moon_sid = moon_lon
    yoga_sum = (sun_sid + moon_sid) % 360
    yoga_idx = int(yoga_sum / (13 + 20 / 60))
    yoga_name = YOGAS_PANCHANG[yoga_idx % 27]

    # Karana: half-tithi (each karana = 6°)
    karana_idx = int(elongation / 6)
    # First karana of the cycle is Kimstughna (fixed), then rotating 7, then fixed ones
    # Karanas 0 = Kimstughna (fixed, for first half of Shukla Pratipada)
    # Then Bava, Balava, Kaulava, Taitila, Garija, Vanija, Vishti repeat
    # Last 4 karanas (57-60) are fixed: Shakuni, Chatushpada, Naga, Kimstughna
    if karana_idx == 0:
        karana_name = "Kimstughna"
    elif karana_idx <= 56:
        rotating_idx = (karana_idx - 1) % 7
        karana_name = KARANAS[rotating_idx]
    else:
        fixed_idx = karana_idx - 57
        karana_name = KARANAS[7 + fixed_idx]

    # Determine if Vishti (Bhadra) karana — considered inauspicious
    is_vishti = (karana_name == "Vishti")

    return {
        "tithi": {
            "number": tithi_num,
            "name": tithi_name,
            "paksha": paksha,
            "tithi_in_paksha": tithi_in_paksha,
            "display": f"{paksha} {tithi_name} ({tithi_in_paksha})"
        },
        "vara": {
            "name": vara_name,
            "lord": vara_lord
        },
        "nakshatra": {
            "name": NAKSHATRAS[nak_idx],
            "pada": nak_pada,
            "lord": NAKSHATRA_LORDS[nak_idx],
            "display": f"{NAKSHATRAS[nak_idx]} (Pada {nak_pada})"
        },
        "yoga": {
            "name": yoga_name,
            "index": yoga_idx + 1
        },
        "karana": {
            "name": karana_name,
            "is_vishti": is_vishti
        }
    }


# ── Chara Karakas (Jaimini) ──────────────────────────────────────────────

def calculate_karakas(data):
    """Calculate Chara Karakas based on degree in sign (7-karaka scheme).

    The planet with the highest degree in its sign = Atmakaraka.
    For Rahu, use (30 - degree) as per Jaimini convention.
    """
    karaka_planets = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]

    planet_degrees = []
    for name in karaka_planets:
        p = data["planets"][name]
        deg = lon_to_deg_in_sign(p["lon"])
        planet_degrees.append({"name": name, "abbr": ABBR[name], "deg": deg})

    # Sort by degree descending — highest degree = Atmakaraka
    planet_degrees.sort(key=lambda x: x["deg"], reverse=True)

    karakas = []
    for i, pd in enumerate(planet_degrees):
        if i < len(KARAKA_NAMES):
            karakas.append({
                "karaka": KARAKA_NAMES[i],
                "karaka_abbr": KARAKA_ABBR[i],
                "planet": pd["name"],
                "planet_abbr": pd["abbr"],
                "degree": format_dms(pd["deg"]),
                "signifies": KARAKA_SIGNIFIES[i]
            })

    return karakas


# ── Doshas ────────────────────────────────────────────────────────────────

def detect_doshas(data):
    """Detect major doshas (afflictions) in the chart."""
    lagna_sign = lon_to_sign(data["lagna"])
    doshas = []

    # Build positions lookup
    positions = {}
    for name in PLANET_ORDER:
        p = data["planets"][name]
        sign_idx = lon_to_sign(p["lon"])
        house = ((sign_idx - lagna_sign) % 12) + 1
        positions[name] = {"sign": sign_idx, "house": house, "lon": p["lon"], "retro": p["retro"]}

    moon_sign = positions["Moon"]["sign"]
    moon_house = positions["Moon"]["house"]

    # --- Mangal Dosha (Kuja Dosha) ---
    mars_house = positions["Mars"]["house"]
    mars_sign = positions["Mars"]["sign"]
    mangal_houses = {1, 2, 4, 7, 8, 12}
    # Check from Lagna, Moon, Venus
    refs = []
    if mars_house in mangal_houses:
        refs.append("Lagna")
    mars_from_moon = ((mars_sign - moon_sign) % 12) + 1
    if mars_from_moon in mangal_houses:
        refs.append("Moon")
    venus_sign = positions["Venus"]["sign"]
    mars_from_venus = ((mars_sign - venus_sign) % 12) + 1
    if mars_from_venus in mangal_houses:
        refs.append("Venus")
    if refs:
        cancellations = []
        # Mars in own/exalted sign
        if mars_sign in OWN_SIGNS.get("Mars", []) or mars_sign == EXALTATION.get("Mars"):
            cancellations.append("Mars in own/exalted sign")
        # Mars aspected by Jupiter
        ju_house = positions["Jupiter"]["house"]
        for asp in [5, 7, 9]:
            if ((ju_house - 1 + asp - 1) % 12) + 1 == mars_house:
                cancellations.append("Mars aspected by Jupiter")
                break
        severity = "severe" if len(refs) >= 3 else "moderate"
        # If all cancellations present, downgrade
        if cancellations:
            severity = "moderate" if severity == "severe" else "moderate"
        from_str = ", ".join(refs)
        doshas.append({
            "name": "Mangal Dosha",
            "description": f"Mars in house {mars_house} from Lagna \u2014 present from {from_str} ({len(refs)}/3 references)",
            "type": severity,
            "cancellations": cancellations
        })

    # --- Kaal Sarp Dosha ---
    rahu_lon = positions["Rahu"]["lon"]
    ketu_lon = positions["Ketu"]["lon"]
    # Check if all planets are on one side of Rahu-Ketu axis
    between_rahu_ketu = 0
    between_ketu_rahu = 0
    for name in ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]:
        p_lon = positions[name]["lon"]
        if rahu_lon > ketu_lon:
            if ketu_lon < p_lon < rahu_lon:
                between_ketu_rahu += 1
            else:
                between_rahu_ketu += 1
        else:
            if rahu_lon < p_lon < ketu_lon:
                between_rahu_ketu += 1
            else:
                between_ketu_rahu += 1
    total = between_rahu_ketu + between_ketu_rahu
    if between_rahu_ketu == total or between_ketu_rahu == total:
        doshas.append({
            "name": "Kaal Sarp Dosha",
            "description": "All planets hemmed between Rahu-Ketu axis",
            "type": "severe",
            "cancellations": []
        })
    elif min(between_rahu_ketu, between_ketu_rahu) <= 1:
        doshas.append({
            "name": "Partial Kaal Sarp Dosha",
            "description": "Nearly all planets hemmed between Rahu-Ketu axis (one planet escapes)",
            "type": "moderate",
            "cancellations": []
        })

    # --- Pitra Dosha ---
    sun_sign = positions["Sun"]["sign"]
    rahu_sign = positions["Rahu"]["sign"]
    if sun_sign == rahu_sign:
        doshas.append({
            "name": "Pitra Dosha",
            "description": "Ancestral affliction \u2014 Sun conjunct Rahu",
            "type": "moderate",
            "cancellations": []
        })
    # Sun in 9th with malefic aspect
    if positions["Sun"]["house"] == 9:
        ninth_sign = (lagna_sign + 8) % 12
        ninth_lord = SIGN_LORDS[ninth_sign]
        if ninth_lord in positions:
            nl_house = positions[ninth_lord]["house"]
            if nl_house in [6, 8, 12]:
                doshas.append({
                    "name": "Pitra Dosha",
                    "description": f"Sun in 9th house with 9th lord ({ninth_lord}) in dusthana (house {nl_house})",
                    "type": "moderate",
                    "cancellations": []
                })

    # --- Kemadruma Dosha ---
    check_planets = ["Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    sign_2_from_moon = (moon_sign + 1) % 12
    sign_12_from_moon = (moon_sign - 1) % 12
    has_planet_near = any(
        positions[cp]["sign"] in [sign_2_from_moon, sign_12_from_moon]
        for cp in check_planets
    )
    if not has_planet_near:
        cancellations = []
        # Cancellation: planets in kendra from Moon
        for cp in check_planets:
            cp_from_moon = ((positions[cp]["sign"] - moon_sign) % 12) + 1
            if cp_from_moon in [1, 4, 7, 10]:
                cancellations.append(f"{cp} in kendra from Moon")
                break
        doshas.append({
            "name": "Kemadruma Dosha",
            "description": "No planets in 2nd/12th from Moon \u2014 potential emotional/financial difficulties",
            "type": "moderate",
            "cancellations": cancellations
        })

    # --- Grahan Dosha (Eclipse Doshas) ---
    for node in ["Rahu", "Ketu"]:
        node_sign = positions[node]["sign"]
        for luminary in ["Sun", "Moon"]:
            if positions[luminary]["sign"] == node_sign:
                name = "Surya Grahan Dosha" if luminary == "Sun" else "Chandra Grahan Dosha"
                # Skip Sun-Rahu if already reported as Pitra Dosha
                if luminary == "Sun" and node == "Rahu":
                    doshas.append({
                        "name": name,
                        "description": f"{luminary} conjunct {node} \u2014 eclipse affliction on {luminary.lower()} significations",
                        "type": "severe",
                        "cancellations": []
                    })
                elif luminary == "Moon":
                    doshas.append({
                        "name": name,
                        "description": f"{luminary} conjunct {node} \u2014 eclipse affliction on {luminary.lower()} significations",
                        "type": "severe",
                        "cancellations": []
                    })

    # --- Shani Dosha (Saturn affliction) ---
    sat_house = positions["Saturn"]["house"]
    if sat_house in [1, 4, 7, 8, 10]:
        cancellations = []
        if positions["Saturn"]["sign"] in OWN_SIGNS.get("Saturn", []):
            cancellations.append("Saturn in own sign")
        if positions["Saturn"]["sign"] == EXALTATION.get("Saturn"):
            cancellations.append("Saturn exalted")
        # Check Jupiter aspect on Saturn
        for asp in [5, 7, 9]:
            if ((positions["Jupiter"]["house"] - 1 + asp - 1) % 12) + 1 == sat_house:
                cancellations.append("Jupiter aspects Saturn")
                break
        if not cancellations:
            doshas.append({
                "name": "Shani Dosha",
                "description": f"Saturn in house {sat_house} \u2014 karmic delays and restrictions",
                "type": "moderate",
                "cancellations": cancellations
            })

    return doshas


# ── Kuta profile builder ──────────────────────────────────────────────────

def compute_kuta_profile(moon_sign_idx, moon_nak_idx):
    """Return all Ashta Kuta attributes for one person's Moon position."""
    return {
        "moon_sign":           SIGNS[moon_sign_idx],
        "moon_sign_lord":      SIGN_LORDS[moon_sign_idx],
        "moon_nakshatra":      NAKSHATRAS[moon_nak_idx],
        "moon_nakshatra_lord": NAKSHATRA_LORDS[moon_nak_idx],
        "gana":                NAKSHATRA_GANA[moon_nak_idx],
        "nadi":                NAKSHATRA_NADI[moon_nak_idx],
        "yoni":                NAKSHATRA_YONI[moon_nak_idx],
        "yoni_gender":         NAKSHATRA_YONI_GENDER[moon_nak_idx],
        "varna":               SIGN_VARNA[moon_sign_idx],
        "vashya":              SIGN_VASHYA[moon_sign_idx],
    }


# Yoni enemy pairs (bi-directional)
_YONI_ENEMIES = frozenset([
    frozenset(["Horse", "Buffalo"]),
    frozenset(["Elephant", "Lion"]),
    frozenset(["Sheep", "Monkey"]),
    frozenset(["Serpent", "Mongoose"]),
    frozenset(["Dog", "Deer"]),
    frozenset(["Cat", "Rat"]),
    frozenset(["Cow", "Tiger"]),
])

# Vashya dominance pairs: key dominates value (gets 1 point, not full 2)
_VASHYA_DOMINATES = {
    ("Chatushpada", "Dwipada"), ("Chatushpada", "Vanachara"),
    ("Dwipada", "Chatushpada"), ("Jalachara", "Dwipada"),
    ("Vanachara", "Chatushpada"),
}

# Natural friendship lookup using existing FRIENDS / ENEMIES dicts
def _planet_relation(p1, p2):
    """Return 'F', 'N', or 'E' for how p1 views p2."""
    if p2 in FRIENDS.get(p1, set()):
        return "F"
    if p2 in ENEMIES.get(p1, set()):
        return "E"
    return "N"

_TARA_NAMES = ["Janma", "Sampat", "Vipat", "Kshema", "Pratyak",
               "Sadhaka", "Vadha", "Mitra", "Param Mitra"]
_TARA_GOOD  = {2, 4, 6, 8, 9}   # 1-based indices of auspicious Taras

_VARNA_ORDER = {"Brahmin": 4, "Kshatriya": 3, "Vaishya": 2, "Shudra": 1}

_GM_SCORE = {
    ("F", "F"): 5, ("F", "N"): 4, ("N", "F"): 4,
    ("N", "N"): 3, ("F", "E"): 1, ("E", "F"): 1,
    ("N", "E"): 0.5, ("E", "N"): 0.5, ("E", "E"): 0,
}


def compute_ashta_kuta_scores(kp_a, kp_b):
    """
    Compute all 8 Ashta Kuta compatibility scores from two kuta_profile dicts.
    Returns a dict with individual scores, max values, labels, notes, and total.
    kp_a / kp_b are the kuta_profile sub-dicts from compute_kuta_profile().
    """
    results = {}

    # ── 1. Varna (max 1) ──────────────────────────────────────────────────
    va = _VARNA_ORDER.get(kp_a["varna"], 0)
    vb = _VARNA_ORDER.get(kp_b["varna"], 0)
    results["varna"] = {
        "score": 1 if va >= vb else 0, "max": 1,
        "a": kp_a["varna"], "b": kp_b["varna"],
        "note": "Compatible" if va >= vb else "Incompatible (A's Varna is lower than B's)",
    }

    # ── 2. Vashya (max 2) ──────────────────────────────────────────────────
    va2, vb2 = kp_a["vashya"], kp_b["vashya"]
    if va2 == vb2:
        vashya_score, vashya_note = 2, "Same Vashya group"
    elif (va2, vb2) in _VASHYA_DOMINATES or (vb2, va2) in _VASHYA_DOMINATES:
        vashya_score, vashya_note = 1, "Partial — one dominates the other"
    else:
        vashya_score, vashya_note = 0, "Incompatible Vashya"
    results["vashya"] = {"score": vashya_score, "max": 2, "a": va2, "b": vb2, "note": vashya_note}

    # ── 3. Tara (max 3) ──────────────────────────────────────────────────
    na = NAKSHATRAS.index(kp_a["moon_nakshatra"])
    nb = NAKSHATRAS.index(kp_b["moon_nakshatra"])
    count_ab = (nb - na) % 27 + 1
    count_ba = (na - nb) % 27 + 1
    tara_ab = ((count_ab - 1) % 9) + 1   # 1-9
    tara_ba = ((count_ba - 1) % 9) + 1
    good_ab = tara_ab in _TARA_GOOD
    good_ba = tara_ba in _TARA_GOOD
    tara_score = 3 if (good_ab and good_ba) else (1.5 if (good_ab or good_ba) else 0)
    results["tara"] = {
        "score": tara_score, "max": 3,
        "a_to_b": _TARA_NAMES[tara_ab - 1], "b_to_a": _TARA_NAMES[tara_ba - 1],
        "a_to_b_good": good_ab, "b_to_a_good": good_ba,
        "note": f"A→B: {_TARA_NAMES[tara_ab-1]} ({'✓' if good_ab else '✗'}), B→A: {_TARA_NAMES[tara_ba-1]} ({'✓' if good_ba else '✗'})",
    }

    # ── 4. Yoni (max 4) ──────────────────────────────────────────────────
    ya, ga = kp_a["yoni"], kp_a["yoni_gender"]
    yb, gb = kp_b["yoni"], kp_b["yoni_gender"]
    pair = frozenset([ya, yb])
    if pair in _YONI_ENEMIES:
        yoni_score, yoni_note = 0, f"Enemy Yoni pair ({ya} vs {yb})"
    elif ya == yb:
        if ga != gb:
            yoni_score, yoni_note = 4, f"Same Yoni ({ya}), complementary genders"
        else:
            yoni_score, yoni_note = 3, f"Same Yoni ({ya}), same gender"
    else:
        yoni_score, yoni_note = 2, f"Friendly/neutral Yoni ({ya} vs {yb})"
    results["yoni"] = {"score": yoni_score, "max": 4, "a": f"{ya}({ga})", "b": f"{yb}({gb})", "note": yoni_note}

    # ── 5. Graha Maitri (max 5) ───────────────────────────────────────────
    lord_a = kp_a["moon_sign_lord"]
    lord_b = kp_b["moon_sign_lord"]
    rel_ab = _planet_relation(lord_a, lord_b)
    rel_ba = _planet_relation(lord_b, lord_a)
    gm_score = _GM_SCORE.get((rel_ab, rel_ba), 0)
    results["graha_maitri"] = {
        "score": gm_score, "max": 5,
        "lord_a": lord_a, "lord_b": lord_b,
        "a_views_b": rel_ab, "b_views_a": rel_ba,
        "note": f"{lord_a}→{lord_b}: {rel_ab}, {lord_b}→{lord_a}: {rel_ba}",
    }

    # ── 6. Gana (max 6) ──────────────────────────────────────────────────
    ga2, gb2 = kp_a["gana"], kp_b["gana"]
    if ga2 == gb2:
        gana_score = 6
    elif frozenset([ga2, gb2]) == frozenset(["Deva", "Manava"]):
        gana_score = 5
    elif frozenset([ga2, gb2]) == frozenset(["Manava", "Rakshasa"]):
        gana_score = 1
    else:  # Deva-Rakshasa
        gana_score = 0
    gana_note = ("Gana Dosha (Deva-Rakshasa) — fundamental temperament clash"
                 if gana_score == 0 else
                 "Partial Gana match" if gana_score < 6 else "Perfect Gana match")
    results["gana"] = {"score": gana_score, "max": 6, "a": ga2, "b": gb2, "note": gana_note}

    # ── 7. Bhakut (max 7) ─────────────────────────────────────────────────
    sa = SIGNS.index(kp_a["moon_sign"])
    sb = SIGNS.index(kp_b["moon_sign"])
    rel_ab7 = (sb - sa) % 12 + 1   # position of B from A
    rel_ba7 = (sa - sb) % 12 + 1
    bhakut_pair = tuple(sorted([rel_ab7, rel_ba7]))
    _BHAKUT_GOOD = {(1, 1), (3, 11), (5, 9), (7, 7)}
    if bhakut_pair in _BHAKUT_GOOD or (rel_ab7 == rel_ba7 == 1):
        bhakut_score = 7
        bhakut_note = f"Favorable Bhakut ({rel_ab7}-{rel_ba7})"
    else:
        bhakut_score = 0
        bhakut_label = {(2, 12): "2-12 Dosha", (6, 8): "6-8 Dosha"}.get(bhakut_pair, f"{rel_ab7}-{rel_ba7} Dosha")
        bhakut_note = f"Bhakut Dosha — {bhakut_label} Moon positions"
    results["bhakut"] = {
        "score": bhakut_score, "max": 7,
        "a_sign": kp_a["moon_sign"], "b_sign": kp_b["moon_sign"],
        "relation": f"{rel_ab7}-{rel_ba7}", "note": bhakut_note,
    }

    # ── 8. Nadi (max 8) ──────────────────────────────────────────────────
    na2, nb2 = kp_a["nadi"], kp_b["nadi"]
    if na2 == nb2:
        nadi_score, nadi_note = 0, f"Nadi Dosha — both {na2} Nadi (most serious dosha)"
    else:
        nadi_score, nadi_note = 8, f"Compatible ({na2} vs {nb2})"
    results["nadi"] = {"score": nadi_score, "max": 8, "a": na2, "b": nb2, "note": nadi_note}

    # ── Total ──────────────────────────────────────────────────────────────
    total = sum(v["score"] for v in results.values())
    return {"scores": results, "total": round(total, 1), "max": 36}


def format_kuta_scores_for_ai(kuta):
    """Format compute_ashta_kuta_scores() output as a readable string for AI prompts."""
    s = kuta["scores"]
    lines = [
        f"PRE-COMPUTED ASHTA KUTA SCORES (authoritative — do NOT recalculate):",
        f"  Varna   (1/1):  {s['varna']['score']}/1   — {s['varna']['a']} vs {s['varna']['b']} — {s['varna']['note']}",
        f"  Vashya  (2/2):  {s['vashya']['score']}/2   — {s['vashya']['a']} vs {s['vashya']['b']} — {s['vashya']['note']}",
        f"  Tara    (3/3):  {s['tara']['score']}/3   — {s['tara']['note']}",
        f"  Yoni    (4/4):  {s['yoni']['score']}/4   — {s['yoni']['a']} vs {s['yoni']['b']} — {s['yoni']['note']}",
        f"  G.Maitri(5/5): {s['graha_maitri']['score']}/5   — {s['graha_maitri']['note']}",
        f"  Gana    (6/6):  {s['gana']['score']}/6   — {s['gana']['a']} vs {s['gana']['b']} — {s['gana']['note']}",
        f"  Bhakut  (7/7):  {s['bhakut']['score']}/7   — {s['bhakut']['note']}",
        f"  Nadi    (8/8):  {s['nadi']['score']}/8   — {s['nadi']['note']}",
        f"  ─────────────────────────────────────",
        f"  TOTAL:          {kuta['total']}/36",
    ]
    return "\n".join(lines)


# ── Main entry point ──────────────────────────────────────────────────────

def compute_chart(year, month, day, hour, minute, lat, lon, tz_offset, place=""):
    """Returns a complete JSON-serializable dict with all chart data."""
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    birth_dt = datetime(year, month, day, hour, minute, 0)
    birth_utc = birth_dt - timedelta(hours=tz_offset)

    jd = swe.julday(
        birth_utc.year, birth_utc.month, birth_utc.day,
        birth_utc.hour + birth_utc.minute / 60.0 + birth_utc.second / 3600.0
    )
    ayanamsa = swe.get_ayanamsa_ut(jd)

    # Core positions
    data = calculate_all(jd, lat, lon, ayanamsa)
    lagna_lon = data["lagna"]
    lagna_sign = lon_to_sign(lagna_lon)
    lagna_deg = lon_to_deg_in_sign(lagna_lon)
    lagna_nak, lagna_pada = lon_to_nakshatra(lagna_lon)

    # Build planet list
    sun_lon = data["planets"]["Sun"]["lon"]
    planets_list = []
    combust_abbrs = []
    vargottama_abbrs = []
    for name in PLANET_ORDER:
        p = data["planets"][name]
        p_lon = p["lon"]
        sign_idx = lon_to_sign(p_lon)
        deg = lon_to_deg_in_sign(p_lon)
        nak_idx, pada = lon_to_nakshatra(p_lon)
        house = get_house(p_lon, lagna_lon)
        deg_in_sign = p_lon % 30
        dig = get_dignity(name, sign_idx, house, deg_in_sign)
        relation = get_house_relation(name, sign_idx)

        # Combust: planet within threshold degrees of Sun (not Sun, Rahu, Ketu)
        if name in COMBUSTION_DEGREES:
            diff = abs((p_lon - sun_lon + 180) % 360 - 180)
            combust = diff <= COMBUSTION_DEGREES[name]
        else:
            combust = False
        if combust:
            combust_abbrs.append(ABBR[name])

        # Vargottama: same sign in D1 and D9
        d9_sign = divisional_sign(p_lon, 9)
        vargottama = (sign_idx == d9_sign)
        if vargottama:
            vargottama_abbrs.append(ABBR[name])

        planets_list.append({
            "name": name,
            "abbr": ABBR[name],
            "lon": round(p_lon, 6),
            "sign": sign_idx + 1,
            "sign_name": SIGNS[sign_idx],
            "deg": format_dms(deg),
            "full_lon": format_dms(p_lon),
            "nakshatra": NAKSHATRAS[nak_idx],
            "pada": pada,
            "house": house,
            "retro": p["retro"],
            "speed": round(p["speed"], 4),
            "dignity": dig,
            "house_relation": relation,
            "combust": combust,
            "vargottama": vargottama
        })

    # Charts
    divisions = {"D1": 1, "D9": 9, "D2": 2, "D3": 3, "D7": 7, "D10": 10, "D12": 12, "D20": 20, "D60": 60}
    charts = {}
    dignities = {}
    for label, div in divisions.items():
        chart_data = build_chart_houses(data, div)
        charts[label] = chart_data

        # Compute dignities for this chart
        div_digs = {}
        if div == 1:
            chart_lagna_sign = lagna_sign
        else:
            chart_lagna_sign = divisional_sign(lagna_lon, div)
        for name in PLANET_ORDER:
            p = data["planets"][name]
            if div == 1:
                s = lon_to_sign(p["lon"])
            else:
                s = divisional_sign(p["lon"], div)
            h = ((s - chart_lagna_sign) % 12) + 1
            d_deg = p["lon"] % 30 if div == 1 else 0
            dig = get_dignity(name, s, h, d_deg, is_divisional=(div != 1))
            div_digs[ABBR[name]] = dig
        dignities[label] = div_digs

    # Arudha Lagna
    al_house = calculate_arudha_lagna(data)

    # Dasha
    dasha = calculate_dasha(data, birth_dt)

    # Sade Sati & Dhaiya
    sadesati = calculate_sadesati(data["planets"]["Moon"]["lon"], jd)

    # Ashtakavarga
    ashtak = calculate_ashtakavarga(data)

    # Yogas
    yogas = detect_yogas(data)

    # Doshas
    doshas = detect_doshas(data)

    # Aspects
    aspects = calculate_aspects(data)

    # Bhava
    bhava = build_bhava(data)

    # Panchang
    panchang = calculate_panchang(jd, data, birth_dt)

    # Chara Karakas
    karakas = calculate_karakas(data)

    # Kuta compatibility profile (derived from Moon's position)
    moon_lon = data["planets"]["Moon"]["lon"]
    moon_sign_idx = lon_to_sign(moon_lon)
    moon_nak_idx, _ = lon_to_nakshatra(moon_lon)
    kuta_profile = compute_kuta_profile(moon_sign_idx, moon_nak_idx)

    # Format timezone string
    tz_h = int(tz_offset)
    tz_m = int((tz_offset - tz_h) * 60)
    tz_str = f"UTC+{tz_h}:{tz_m:02d}" if tz_offset >= 0 else f"UTC{tz_h}:{abs(tz_m):02d}"

    return {
        "birth": {
            "date": birth_dt.strftime("%B %d, %Y"),
            "time": birth_dt.strftime("%H:%M"),
            "place": place,
            "lat": lat,
            "lon": lon,
            "tz": tz_offset,
            "tz_str": tz_str,
            "ayanamsa": format_dms(ayanamsa),
            "jd": round(jd, 6)
        },
        "lagna": {
            "sign": lagna_sign + 1,
            "sign_name": SIGNS[lagna_sign],
            "degree": format_dms(lagna_deg),
            "lon": round(lagna_lon, 6),
            "lon_fmt": format_dms(lagna_lon),
            "nakshatra": NAKSHATRAS[lagna_nak],
            "pada": lagna_pada,
            "nak_lord": NAKSHATRA_LORDS[lagna_nak],
            "sign_lord": SIGN_LORDS[lagna_sign]
        },
        "planets": planets_list,
        "combust_planets": combust_abbrs,
        "vargottama_planets": vargottama_abbrs,
        "charts": charts,
        "dignities": dignities,
        "arudha_lagna": {"house": al_house},
        "bhava": bhava,
        "dasha": dasha,
        "ashtakavarga": ashtak,
        "yogas": yogas,
        "doshas": doshas,
        "aspects": aspects,
        "panchang": panchang,
        "karakas": karakas,
        "sadesati": sadesati,
        "kuta_profile": kuta_profile
    }


# ── Birth Time Rectification (BTR) ──────────────────────────────────

# Life areas governed by each divisional chart
DIVISION_LIFE_AREAS = {
    1: "Overall life, personality, physical body",
    2: "Wealth, family resources",
    3: "Siblings, courage, short travels",
    7: "Children, progeny, creative expression",
    9: "Marriage, spouse, dharma, fortune",
    10: "Career, profession, public life",
    12: "Parents, ancestry, past life debts",
    60: "Past karma, overall destiny, subtle life patterns",
}


def _get_lagna_at_offset(jd, lat, lon, ayanamsa, offset_minutes):
    """Get sidereal Lagna longitude at jd + offset_minutes."""
    jd_offset = jd + (offset_minutes / 1440.0)
    houses_result = swe.houses(jd_offset, lat, lon, b'E')
    asc_tropical = houses_result[1][0]
    return (asc_tropical - ayanamsa) % 360


def _get_divisional_lagna(lagna_lon, division):
    """Get divisional Lagna sign index for a given Lagna longitude."""
    return divisional_sign(lagna_lon, division)


def _find_boundary(jd, lat, lon, ayanamsa, division, direction, max_minutes=120):
    """Search forward (+1) or backward (-1) to find when divisional Lagna changes.

    Phase 1: Coarse search in 1-minute steps.
    Phase 2: Binary search for sub-minute precision.
    Returns: (minutes_to_change, adjacent_sign_name) or (None, None) if not found.
    """
    base_lagna = _get_lagna_at_offset(jd, lat, lon, ayanamsa, 0)
    base_div_sign = _get_divisional_lagna(base_lagna, division)

    # Phase 1: coarse search in 1-minute steps
    last_matching = 0
    first_changed = None
    for step in range(1, max_minutes + 1):
        offset = step * direction
        test_lagna = _get_lagna_at_offset(jd, lat, lon, ayanamsa, offset)
        test_div_sign = _get_divisional_lagna(test_lagna, division)
        if test_div_sign != base_div_sign:
            first_changed = offset
            break
        last_matching = offset

    if first_changed is None:
        return None, None

    # Phase 2: binary search between last_matching and first_changed
    lo = last_matching
    hi = first_changed
    for _ in range(20):  # ~0.00006 minute precision
        mid = (lo + hi) / 2.0
        test_lagna = _get_lagna_at_offset(jd, lat, lon, ayanamsa, mid)
        test_div_sign = _get_divisional_lagna(test_lagna, division)
        if test_div_sign == base_div_sign:
            lo = mid
        else:
            hi = mid

    minutes_to_change = abs(hi)
    # Get the sign at the boundary
    boundary_lagna = _get_lagna_at_offset(jd, lat, lon, ayanamsa, hi)
    boundary_sign = _get_divisional_lagna(boundary_lagna, division)

    return round(minutes_to_change, 1), SIGNS[boundary_sign]


def compute_btr(year, month, day, hour, minute, lat, lon, tz_offset):
    """Compute birth time rectification boundary data for all key divisional charts."""
    swe.set_sid_mode(swe.SIDM_LAHIRI)

    birth_dt = datetime(year, month, day, hour, minute, 0)
    birth_utc = birth_dt - timedelta(hours=tz_offset)

    jd = swe.julday(
        birth_utc.year, birth_utc.month, birth_utc.day,
        birth_utc.hour + birth_utc.minute / 60.0 + birth_utc.second / 3600.0
    )
    ayanamsa = swe.get_ayanamsa_ut(jd)

    btr_divisions = [1, 2, 3, 7, 9, 10, 12, 20, 60]
    div_labels = {1: "D1", 2: "D2", 3: "D3", 7: "D7", 9: "D9", 10: "D10", 12: "D12", 20: "D20", 60: "D60"}

    boundaries = []
    critical_charts = []

    for div in btr_divisions:
        base_lagna = _get_lagna_at_offset(jd, lat, lon, ayanamsa, 0)
        current_sign_idx = _get_divisional_lagna(base_lagna, div)
        current_sign = SIGNS[current_sign_idx]

        mins_before, prev_sign = _find_boundary(jd, lat, lon, ayanamsa, div, -1)
        mins_after, next_sign = _find_boundary(jd, lat, lon, ayanamsa, div, +1)

        boundary = {
            "chart": div_labels[div],
            "division": div,
            "current_sign": current_sign,
            "prev_sign": prev_sign,
            "next_sign": next_sign,
            "mins_before": mins_before,
            "mins_after": mins_after,
        }
        boundaries.append(boundary)

        # Identify critical charts (boundary within 10 minutes)
        nearest_mins = None
        nearest_dir = None
        alternate_sign = None
        if mins_before is not None and mins_after is not None:
            if mins_before <= mins_after:
                nearest_mins = mins_before
                nearest_dir = "before"
                alternate_sign = prev_sign
            else:
                nearest_mins = mins_after
                nearest_dir = "after"
                alternate_sign = next_sign
        elif mins_before is not None:
            nearest_mins = mins_before
            nearest_dir = "before"
            alternate_sign = prev_sign
        elif mins_after is not None:
            nearest_mins = mins_after
            nearest_dir = "after"
            alternate_sign = next_sign

        if nearest_mins is not None and nearest_mins <= 10:
            life_area = DIVISION_LIFE_AREAS.get(div, "General")
            what_changes = (
                f"{div_labels[div]} Lagna would shift from {current_sign} to "
                f"{alternate_sign} — different characteristics for {life_area.lower()}"
            )
            critical_charts.append({
                "chart": div_labels[div],
                "mins_to_nearest_boundary": nearest_mins,
                "direction": nearest_dir,
                "current_sign": current_sign,
                "alternate_sign": alternate_sign,
                "life_area": life_area,
                "what_changes": what_changes,
            })

    # Planet sensitivity: check which planets change divisional sign within +/- 10 min
    planet_sensitivity = []
    data = calculate_all(jd, lat, lon, ayanamsa)
    for div in btr_divisions:
        for name in PLANET_ORDER:
            p_lon = data["planets"][name]["lon"]
            current_div_sign = divisional_sign(p_lon, div)

            for direction in [-1, +1]:
                for step_min in range(1, 11):
                    offset = step_min * direction
                    test_jd = jd + (offset / 1440.0)
                    if name in PLANETS:
                        test_lon, _, _s = get_sidereal_pos(test_jd, PLANETS[name])
                    elif name == "Rahu":
                        test_lon, _ = get_rahu_ketu(test_jd)
                    else:  # Ketu
                        _, test_lon = get_rahu_ketu(test_jd)
                    test_div_sign = divisional_sign(test_lon, div)
                    if test_div_sign != current_div_sign:
                        planet_sensitivity.append({
                            "planet": name,
                            "chart": div_labels[div],
                            "current_sign": SIGNS[current_div_sign],
                            "alternate_sign": SIGNS[test_div_sign],
                            "mins_to_change": step_min,
                            "direction": "after" if direction == 1 else "before",
                        })
                        break

    return {
        "boundaries": boundaries,
        "critical_charts": critical_charts,
        "planet_sensitivity": planet_sensitivity,
    }


# ── Landing page features ─────────────────────────────────────────────────────

def _find_tithi_boundary(jd_ref, tithi_idx, forward):
    """
    Find the JD when tithi_idx starts (forward=False) or ends (forward=True).
    Steps in 15-min increments from jd_ref, then bisects to ~4-sec accuracy.
    Returns JD or None.
    """
    swe.set_sid_mode(swe.SIDM_LAHIRI)
    step = (15.0 / 1440.0) * (1 if forward else -1)

    def get_tidx(jd):
        s, _, _ss = get_sidereal_pos(jd, swe.SUN)
        m, _, _ms = get_sidereal_pos(jd, swe.MOON)
        return int(((m - s) % 360) / 12)

    prev_jd = jd_ref
    for _ in range(120):  # 120 × 15 min = 30 hours max
        curr_jd = prev_jd + step
        if get_tidx(curr_jd) != tithi_idx:
            lo, hi = (prev_jd, curr_jd) if forward else (curr_jd, prev_jd)
            for _ in range(12):  # bisect → ~4-sec resolution
                mid = (lo + hi) / 2
                if get_tidx(mid) == tithi_idx:
                    lo = mid
                else:
                    hi = mid
            return hi
        prev_jd = curr_jd
    return None


def _get_tz_coords(tz_str):
    """Approximate lat/lon from IANA timezone string using UTC offset."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        tz = ZoneInfo(tz_str)
        offset_secs = _dt.now(tz).utcoffset().total_seconds()
        return 20.0, (offset_secs / 3600) * 15
    except Exception:
        return 20.0, 0.0


def compute_panchang(date_str, tz_str="UTC"):
    """
    Compute panchang for date_str ('YYYY-MM-DD') in the user's timezone.
    Returns dict with tithi, vara, nakshatra, yoga, karana, rahu_kaal, gulika_kaal,
    sunrise, sunset, moon_sign, sun_sign.
    """
    from zoneinfo import ZoneInfo
    from datetime import datetime as _dt

    swe.set_sid_mode(swe.SIDM_LAHIRI)

    try:
        tz = ZoneInfo(tz_str)
    except Exception:
        tz = ZoneInfo("UTC")

    year, month, day = map(int, date_str.split("-"))
    local_noon = _dt(year, month, day, 12, 0, 0, tzinfo=tz)
    utc_noon = local_noon.astimezone(ZoneInfo("UTC"))
    jd_noon = swe.julday(utc_noon.year, utc_noon.month, utc_noon.day,
                         utc_noon.hour + utc_noon.minute / 60.0)

    sun_lon, _, _ss = get_sidereal_pos(jd_noon, swe.SUN)
    moon_lon, _, _ms = get_sidereal_pos(jd_noon, swe.MOON)

    # Tithi
    diff = (moon_lon - sun_lon) % 360
    tithi_idx = int(diff / 12)          # 0-29
    tithi_num = tithi_idx % 15          # 0-14
    paksha_idx = 0 if tithi_idx < 15 else 1
    if tithi_num == 14:
        tithi_name = "Purnima" if paksha_idx == 0 else "Amavasya"
    else:
        tithi_name = TITHIS[tithi_num]

    # Tithi boundary JDs (converted to local time strings after jd_to_hhmm is defined)
    try:
        jd_tithi_start = _find_tithi_boundary(jd_noon, tithi_idx, forward=False)
        jd_tithi_end   = _find_tithi_boundary(jd_noon, tithi_idx, forward=True)
    except Exception:
        jd_tithi_start = jd_tithi_end = None

    # Vara
    weekday = _dt(year, month, day).weekday()   # Mon=0 … Sun=6
    jyotish_vara_idx = (weekday + 1) % 7        # Sun=0 … Sat=6
    vara = VARAS[jyotish_vara_idx]
    vara_lord = VARA_LORDS[jyotish_vara_idx]

    # Nakshatra
    nak_idx, pada = lon_to_nakshatra(moon_lon)

    # Yoga
    yoga_lon = (sun_lon + moon_lon) % 360
    yoga_idx = int(yoga_lon / (360.0 / 27))

    # Karana (half-tithi)
    karana_idx_raw = int(diff / 6)   # 0-59
    movable = ["Bava", "Balava", "Kaulava", "Taitila", "Garija", "Vanija", "Vishti"]
    fixed_end = ["Shakuni", "Chatushpada", "Naga", "Kimstughna"]
    if karana_idx_raw == 0:
        karana = "Kimstughna"
    elif karana_idx_raw >= 57:
        karana = fixed_end[min(karana_idx_raw - 57, 2)]
    else:
        karana = movable[(karana_idx_raw - 1) % 7]

    # Sunrise / Sunset / Rahu Kaal / Gulika Kaal
    approx_lat, approx_lon = _get_tz_coords(tz_str)
    geopos = (approx_lon, approx_lat, 0)

    # Search from local 4 AM UTC equivalent
    local_4am = _dt(year, month, day, 4, 0, 0, tzinfo=tz)
    utc_4am = local_4am.astimezone(ZoneInfo("UTC"))
    jd_search = swe.julday(utc_4am.year, utc_4am.month, utc_4am.day,
                            utc_4am.hour + utc_4am.minute / 60.0)

    def jd_to_hhmm(jd, tz):
        y2, m2, d2, h2 = swe.revjul(jd)
        hr = int(h2); mn = int((h2 - hr) * 60)
        utc_dt = _dt(y2, m2, d2, hr, mn, tzinfo=ZoneInfo("UTC"))
        loc_dt = utc_dt.astimezone(tz)
        return loc_dt.strftime("%-I:%M %p")

    tithi_start_str = jd_to_hhmm(jd_tithi_start, tz) if jd_tithi_start else "—"
    tithi_end_str   = jd_to_hhmm(jd_tithi_end,   tz) if jd_tithi_end   else "—"

    try:
        _, trise = swe.rise_trans(jd_search, swe.SUN, swe.CALC_RISE, geopos, 0.0, 0.0)
        _, tset  = swe.rise_trans(jd_search, swe.SUN, swe.CALC_SET,  geopos, 0.0, 0.0)
        jd_rise = trise[0]; jd_set = tset[0]
        day_dur = jd_set - jd_rise
        seg = day_dur / 8.0

        # Rahu Kaal segment (0-indexed): Mon=1,Tue=6,Wed=4,Thu=5,Fri=2,Sat=7,Sun=3
        rahu_map = {0: 1, 1: 6, 2: 4, 3: 5, 4: 2, 5: 7, 6: 3}
        rahu_seg = rahu_map[weekday]
        rk_start = jd_rise + rahu_seg * seg
        rk_end   = rk_start + seg

        # Gulika Kaal segment (0-indexed): Mon=5,Tue=4,Wed=3,Thu=2,Fri=1,Sat=0,Sun=6
        gulika_map = {0: 5, 1: 4, 2: 3, 3: 2, 4: 1, 5: 0, 6: 6}
        gulika_seg = gulika_map[weekday]
        gk_start = jd_rise + gulika_seg * seg
        gk_end   = gk_start + seg

        sunrise_str   = jd_to_hhmm(jd_rise, tz)
        sunset_str    = jd_to_hhmm(jd_set,  tz)
        rahu_kaal     = f"{jd_to_hhmm(rk_start, tz)} – {jd_to_hhmm(rk_end, tz)}"
        gulika_kaal   = f"{jd_to_hhmm(gk_start, tz)} – {jd_to_hhmm(gk_end, tz)}"
    except Exception:
        sunrise_str = sunset_str = rahu_kaal = gulika_kaal = "—"

    return {
        "date": date_str,
        "tithi": f"{PAKSHA[paksha_idx]} {tithi_name}",
        "tithi_num": tithi_idx + 1,
        "tithi_start": tithi_start_str,
        "tithi_end": tithi_end_str,
        "paksha": PAKSHA[paksha_idx],
        "vara": vara,
        "vara_lord": vara_lord,
        "nakshatra": NAKSHATRAS[nak_idx],
        "nakshatra_pada": pada,
        "nakshatra_lord": NAKSHATRA_LORDS[nak_idx],
        "yoga": YOGAS_PANCHANG[yoga_idx % 27],
        "karana": karana,
        "sunrise": sunrise_str,
        "sunset": sunset_str,
        "rahu_kaal": rahu_kaal,
        "gulika_kaal": gulika_kaal,
        "moon_sign": SIGNS[lon_to_sign(moon_lon)],
        "sun_sign": SIGNS[lon_to_sign(sun_lon)],
    }


# ── Event-based birth-time rectification ────────────────────────────────────
# Each event type maps to the houses it activates and its primary karaka.
BTR_EVENT_MAP = {
    "marriage":         {"houses": [7, 2, 11], "karaka": "Venus",   "label": "Marriage"},
    "first_job":        {"houses": [10, 6, 2], "karaka": "Saturn",  "label": "First job / career start"},
    "career_change":    {"houses": [10, 6],    "karaka": "Saturn",  "label": "Major career change"},
    "first_child":      {"houses": [5, 9],     "karaka": "Jupiter", "label": "Birth of first child"},
    "father_passing":   {"houses": [9, 4],     "karaka": "Sun",     "label": "Father's passing"},
    "mother_passing":   {"houses": [4, 11],    "karaka": "Moon",    "label": "Mother's passing"},
    "accident_surgery": {"houses": [6, 8],     "karaka": "Mars",    "label": "Major accident / surgery"},
    "moved_abroad":     {"houses": [12, 9, 3], "karaka": "Rahu",    "label": "Moved abroad"},
    "bought_home":      {"houses": [4],        "karaka": "Mars",    "label": "Bought a home"},
    "education":        {"houses": [4, 5, 9],  "karaka": "Mercury", "label": "Major education milestone"},
}
# Special graha drishti: extra houses (besides the universal 7th) a planet aspects.
_GRAHA_SPECIAL_ASPECTS = {"Mars": [4, 8], "Jupiter": [5, 9], "Saturn": [3, 10],
                          "Rahu": [5, 9], "Ketu": [5, 9]}


def _parse_btr_date(s):
    return datetime.strptime(s, "%d-%b-%Y")


def _active_dasha_lords(dasha, event_dt):
    """(maha_lord, antar_lord) active on event_dt, or (None, None)."""
    maha_lord = None
    for md in dasha.get("maha", []):
        try:
            if _parse_btr_date(md["start"]) <= event_dt < _parse_btr_date(md["end"]):
                maha_lord = md["lord"]; break
        except Exception:
            continue
    antar_lord = None
    if maha_lord:
        for ad in dasha.get("antar", {}).get(maha_lord, []):
            try:
                if _parse_btr_date(ad["start"]) <= event_dt < _parse_btr_date(ad["end"]):
                    antar_lord = ad["lord"]; break
            except Exception:
                continue
    return maha_lord, antar_lord


def rectify_by_events(year, month, day, hour, minute, lat, lon, tz_offset,
                      window_minutes, events, max_candidates=31):
    """Score candidate birth times across ±window_minutes against dated life
    events via Vimshottari dasha-lord activation (using equal/bhava houses from
    the Lagna degree, so it discriminates within a sign) plus major transits.
    Returns ranked candidates with structured facts for an LLM judge."""
    window_minutes = float(window_minutes or 15)
    base_dt = datetime(year, month, day, hour, minute)

    n = max(3, min(int(max_candidates), int(round(window_minutes * 2)) + 1))
    step = (2 * window_minutes) / (n - 1) if n > 1 else 0.0
    offsets = [round(-window_minutes + i * step, 2) for i in range(n)]

    resolved, transit_cache = [], {}
    for ev in events:
        m = BTR_EVENT_MAP.get(ev.get("type"), {})
        houses = [int(h) for h in (ev.get("houses") or m.get("houses") or [10])]
        karaka = ev.get("karaka") or m.get("karaka")
        label = ev.get("label") or m.get("label") or ev.get("text") or ev.get("type") or "Event"
        date_str = ev.get("date")
        try:
            edt = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            continue
        if date_str not in transit_cache:
            try:
                transit_cache[date_str] = compute_transits_for_date(date_str)
            except Exception:
                transit_cache[date_str] = []
        resolved.append({"label": label, "date": date_str, "edt": edt,
                         "houses": houses, "karaka": karaka})

    if not resolved:
        return {"error": "No valid dated events provided.", "candidates": []}

    candidates = []
    for off in offsets:
        cdt = base_dt + timedelta(minutes=off)
        chart = compute_chart(cdt.year, cdt.month, cdt.day, cdt.hour, cdt.minute, lat, lon, tz_offset)
        lagna_lon = chart["lagna"]["lon"]
        lagna_sign_idx = chart["lagna"]["sign"]
        dasha = chart.get("dasha", {})

        def house_of(l):
            return int(((l - lagna_lon) % 360) / 30) + 1

        planet_house = {p["name"]: house_of(p["lon"]) for p in chart["planets"]}

        def touches(planet, target_houses):
            t = set()
            for h in range(1, 13):           # rules
                if SIGN_LORDS[(lagna_sign_idx + h - 1) % 12] == planet:
                    t.add(h)
            ph = planet_house.get(planet)
            if ph:                            # occupies + aspects
                t.add(ph)
                for o in [7] + _GRAHA_SPECIAL_ASPECTS.get(planet, []):
                    t.add(((ph - 1 + o - 1) % 12) + 1)
            return t & set(target_houses)

        total, ev_facts = 0.0, []
        for rev in resolved:
            maha_lord, antar_lord = _active_dasha_lords(dasha, rev["edt"])
            houses, karaka = rev["houses"], rev["karaka"]
            sc, reasons = 0.0, []
            for lord, w, tag in [(maha_lord, 2.0, "Mahadasha"), (antar_lord, 1.5, "Antardasha")]:
                if not lord:
                    continue
                hit = touches(lord, houses)
                if hit:
                    sc += w
                    reasons.append(f"{tag} {lord} activates house(s) {sorted(hit)}")
                if karaka:
                    if lord == karaka:
                        sc += w * 0.5
                        reasons.append(f"{tag} lord is karaka {karaka}")
                    else:
                        kh = planet_house.get(karaka)
                        if kh and touches(lord, [kh]):
                            sc += w * 0.5
                            reasons.append(f"{tag} {lord} connects karaka {karaka} (house {kh})")
            for tp in transit_cache.get(rev["date"], []):
                if tp["planet"] not in ("Saturn", "Jupiter", "Rahu", "Ketu"):
                    continue
                th = house_of(tp["sign_idx"] * 30 + tp.get("deg_in_sign", 0))
                tset = {th}
                for o in [7] + _GRAHA_SPECIAL_ASPECTS.get(tp["planet"], []):
                    tset.add(((th - 1 + o - 1) % 12) + 1)
                hit = tset & set(houses)
                if hit:
                    sc += 0.5
                    reasons.append(f"transit {tp['planet']} on house(s) {sorted(hit)}")
            total += sc
            ev_facts.append({"event": rev["label"], "date": rev["date"],
                             "mahadasha": maha_lord, "antardasha": antar_lord,
                             "target_houses": houses, "karaka": karaka,
                             "score": round(sc, 2), "reasons": reasons})
        candidates.append({"offset_min": off, "time": cdt.strftime("%H:%M"),
                           "lagna": chart["lagna"]["sign_name"],
                           "navamsha_lagna": SIGNS[divisional_sign(lagna_lon, 9)],
                           "score": round(total, 2), "events": ev_facts})

    candidates.sort(key=lambda c: -c["score"])
    return {"window_minutes": window_minutes, "n_candidates": len(candidates),
            "events": [{"label": r["label"], "date": r["date"],
                        "houses": r["houses"], "karaka": r["karaka"]} for r in resolved],
            "candidates": candidates}


def compute_transits_for_date(date_str):
    """Return sidereal positions of all 9 grahas for a given date (noon UTC)."""
    swe.set_sid_mode(swe.SIDM_LAHIRI)
    year, month, day = map(int, date_str.split("-"))
    jd = swe.julday(year, month, day, 12.0)

    result = []
    for name, pid in PLANETS.items():
        lon, _, speed = get_sidereal_pos(jd, pid)
        sign_idx = lon_to_sign(lon)
        nak_idx, pada = lon_to_nakshatra(lon)
        result.append({
            "planet": name, "abbr": ABBR[name],
            "sign": SIGNS[sign_idx], "sign_idx": sign_idx,
            "deg_in_sign": round(lon % 30, 1),
            "nakshatra": NAKSHATRAS[nak_idx], "nakshatra_pada": pada,
            "retrograde": speed < 0,
        })

    rahu_lon, ketu_lon = get_rahu_ketu(jd)
    for name, lon in [("Rahu", rahu_lon), ("Ketu", ketu_lon)]:
        sign_idx = lon_to_sign(lon)
        nak_idx, pada = lon_to_nakshatra(lon)
        result.append({
            "planet": name, "abbr": ABBR[name],
            "sign": SIGNS[sign_idx], "sign_idx": sign_idx,
            "deg_in_sign": round(lon % 30, 1),
            "nakshatra": NAKSHATRAS[nak_idx], "nakshatra_pada": pada,
            "retrograde": True,
        })
    return result


def compute_transits():
    """Return current sidereal positions of all 9 grahas."""
    from zoneinfo import ZoneInfo
    from datetime import datetime as _dt

    swe.set_sid_mode(swe.SIDM_LAHIRI)
    now = _dt.now(ZoneInfo("UTC"))
    jd_now = swe.julday(now.year, now.month, now.day, now.hour + now.minute / 60.0)

    result = []
    for name, pid in PLANETS.items():
        lon, _, speed = get_sidereal_pos(jd_now, pid)
        sign_idx = lon_to_sign(lon)
        nak_idx, pada = lon_to_nakshatra(lon)
        result.append({
            "planet": name, "abbr": ABBR[name],
            "sign": SIGNS[sign_idx], "sign_idx": sign_idx,
            "deg_in_sign": round(lon % 30, 1),
            "nakshatra": NAKSHATRAS[nak_idx], "nakshatra_pada": pada,
            "retrograde": speed < 0,
        })

    rahu_lon, ketu_lon = get_rahu_ketu(jd_now)
    for name, lon in [("Rahu", rahu_lon), ("Ketu", ketu_lon)]:
        sign_idx = lon_to_sign(lon)
        nak_idx, pada = lon_to_nakshatra(lon)
        result.append({
            "planet": name, "abbr": ABBR[name],
            "sign": SIGNS[sign_idx], "sign_idx": sign_idx,
            "deg_in_sign": round(lon % 30, 1),
            "nakshatra": NAKSHATRAS[nak_idx], "nakshatra_pada": pada,
            "retrograde": True,
        })
    return result
