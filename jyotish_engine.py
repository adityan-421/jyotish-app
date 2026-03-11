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
    speed = result[0][3]
    return lon % 360, speed


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
        # Vimshamsha: movable signs start from Aries, fixed from Sagittarius, dual from Leo
        element = sign_idx % 3  # 0=movable, 1=fixed, 2=dual
        element_start = [0, 8, 4][element]
        return (element_start + part) % 12
    elif division == 60:
        part = int(deg_in_sign / 0.5)  # 30° / 60 = 0.5° per division
        # Parashari D60: odd signs count from Aries, even signs count from Libra
        if sign_idx % 2 == 0:  # odd sign (0-indexed even = 1st, 3rd, etc.)
            return part % 12
        else:  # even sign
            return (6 + part) % 12
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
        planet_lon, speed = get_sidereal_pos(jd, pid)
        data["planets"][name] = {"lon": planet_lon, "speed": speed, "retro": speed < 0}

    rahu, ketu = get_rahu_ketu(jd)
    data["planets"]["Rahu"] = {"lon": rahu, "speed": 0, "retro": True}
    data["planets"]["Ketu"] = {"lon": ketu, "speed": 0, "retro": True}

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

    moon_sign = positions["Moon"]["sign"]

    # 1. Gajakesari
    jup_from_moon = (positions["Jupiter"]["sign"] - moon_sign) % 12
    if jup_from_moon in [0, 3, 6, 9]:
        yogas_found.append({"name": "Gajakesari Yoga",
                            "description": "Jupiter in kendra from Moon \u2014 wisdom, fame, good fortune",
                            "type": "positive"})

    # 2. Budhaditya
    if same_sign("Sun", "Mercury"):
        yogas_found.append({"name": "Budhaditya Yoga",
                            "description": "Sun-Mercury conjunction \u2014 intelligence, communication skills",
                            "type": "positive"})

    # 3. Chandra-Mangala
    if same_sign("Moon", "Mars") or signs_apart("Moon", "Mars") == 6:
        yogas_found.append({"name": "Chandra-Mangala Yoga",
                            "description": "Moon-Mars conjunction/aspect \u2014 wealth through enterprise",
                            "type": "positive"})

    # 4. Amala Yoga
    benefics = ["Jupiter", "Venus", "Mercury"]
    for b in benefics:
        if positions[b]["house"] == 10:
            yogas_found.append({"name": "Amala Yoga",
                                "description": f"{b} in 10th house \u2014 virtuous deeds, good reputation",
                                "type": "positive"})
    for b in benefics:
        h_from_moon = (positions[b]["sign"] - moon_sign) % 12
        if h_from_moon == 9:
            yogas_found.append({"name": "Amala Yoga (from Moon)",
                                "description": f"{b} in 10th from Moon \u2014 good reputation",
                                "type": "positive"})

    # 5. Pancha Mahapurusha
    mahapurusha = {"Mars": "Ruchaka", "Mercury": "Bhadra", "Jupiter": "Hamsa",
                   "Venus": "Malavya", "Saturn": "Shasha"}
    own_signs = {
        "Mars": [0, 7], "Mercury": [2, 5], "Jupiter": [8, 11],
        "Venus": [1, 6], "Saturn": [9, 10]
    }
    exalt_signs = {"Mars": 9, "Mercury": 5, "Jupiter": 3, "Venus": 11, "Saturn": 6}
    for planet, yoga_name in mahapurusha.items():
        p_sign = positions[planet]["sign"]
        if in_kendra(planet) and (p_sign in own_signs[planet] or p_sign == exalt_signs[planet]):
            yogas_found.append({"name": f"{yoga_name} Yoga (Pancha Mahapurusha)",
                                "description": f"{planet} in kendra in own/exaltation sign \u2014 power and status",
                                "type": "positive"})

    # 6. Raja Yoga
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
    for kl in kendra_lords:
        for tl in trikona_lords:
            if kl != tl and kl in positions and tl in positions and same_sign(kl, tl):
                yogas_found.append({"name": "Raja Yoga",
                                    "description": f"{kl} (kendra lord) conjunct {tl} (trikona lord) \u2014 power, authority",
                                    "type": "positive"})

    # 7. Dhana Yoga
    for h in [2, 11]:
        sign_of_house = (lagna_sign + h - 1) % 12
        lord = SIGN_LORDS[sign_of_house]
        if lord in positions and (in_kendra(lord) or in_trikona(lord)):
            yogas_found.append({"name": "Dhana Yoga",
                                "description": f"{lord} (lord of house {h}) in kendra/trikona \u2014 wealth",
                                "type": "positive"})

    # 8. Vipareeta Raja Yoga
    dusthana = [6, 8, 12]
    for h in dusthana:
        sign_of_house = (lagna_sign + h - 1) % 12
        lord = SIGN_LORDS[sign_of_house]
        if lord in positions and positions[lord]["house"] in dusthana:
            yogas_found.append({"name": "Vipareeta Raja Yoga",
                                "description": f"{lord} (lord of {h}) in dusthana \u2014 rise after setbacks",
                                "type": "positive"})

    # 9. Kemadruma Yoga
    check_planets = ["Mars", "Mercury", "Jupiter", "Venus", "Saturn"]
    sign_2_from_moon = (moon_sign + 1) % 12
    sign_12_from_moon = (moon_sign - 1) % 12
    has_planet_near_moon = any(
        positions[cp]["sign"] in [sign_2_from_moon, sign_12_from_moon]
        for cp in check_planets
    )
    if not has_planet_near_moon:
        yogas_found.append({"name": "Kemadruma Yoga",
                            "description": "No planets in 2nd/12th from Moon \u2014 potential difficulties (check cancellation)",
                            "type": "caution"})

    # 10. Vish Yoga
    if positions["Moon"]["sign"] == positions["Saturn"]["sign"]:
        yogas_found.append({"name": "Vish Yoga",
                            "description": "Moon and Saturn conjunct in the same sign — emotional heaviness, delays, karmic lessons; strength depends on sign and house",
                            "type": "caution"})

    # 11. Saraswati Yoga
    good_houses = [1, 2, 4, 5, 7, 9, 10]
    if all(positions[p]["house"] in good_houses for p in ["Jupiter", "Venus", "Mercury"]):
        yogas_found.append({"name": "Saraswati Yoga",
                            "description": "Jupiter, Venus, Mercury well-placed \u2014 learning, arts, wisdom",
                            "type": "positive"})

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
        "sadesati": sadesati
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
                        test_lon, _ = get_sidereal_pos(test_jd, PLANETS[name])
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
        s, _ = get_sidereal_pos(jd, swe.SUN)
        m, _ = get_sidereal_pos(jd, swe.MOON)
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

    sun_lon, _ = get_sidereal_pos(jd_noon, swe.SUN)
    moon_lon, _ = get_sidereal_pos(jd_noon, swe.MOON)

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
        _, trise = swe.rise_trans(jd_search, swe.SUN, "", swe.CALC_RISE,  geopos, 0, 0)
        _, tset  = swe.rise_trans(jd_search, swe.SUN, "", swe.CALC_SET,   geopos, 0, 0)
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


def compute_transits():
    """Return current sidereal positions of all 9 grahas."""
    from zoneinfo import ZoneInfo
    from datetime import datetime as _dt

    swe.set_sid_mode(swe.SIDM_LAHIRI)
    now = _dt.now(ZoneInfo("UTC"))
    jd_now = swe.julday(now.year, now.month, now.day, now.hour + now.minute / 60.0)

    result = []
    for name, pid in PLANETS.items():
        lon, speed = get_sidereal_pos(jd_now, pid)
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
