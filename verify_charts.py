#!/usr/bin/env python3
"""Verify Jyotish chart accuracy against published reference data (Lahiri ayanamsa)."""

from jyotish_engine import compute_chart

# Reference data sourced from AstroSage (Lahiri ayanamsa)
# Format: (name, year, month, day, hour, minute, lat, lon, tz_offset,
#           expected_lagna, expected_moon_nakshatra,
#           {planet: expected_sign})
TEST_CASES = [
    (
        "Mahatma Gandhi", 1869, 10, 2, 7, 12, 21.6417, 69.6293, 4.644,
        "Libra", "Ashlesha",
        {"Sun": "Virgo", "Moon": "Cancer", "Mars": "Libra", "Mercury": "Libra",
         "Jupiter": "Aries", "Venus": "Libra", "Saturn": "Scorpio",
         "Rahu": "Cancer", "Ketu": "Capricorn"},
    ),
    (
        "Jawaharlal Nehru", 1889, 11, 14, 23, 0, 25.4358, 81.8463, 5.456,
        "Cancer", "Ashlesha",
        {"Sun": "Scorpio", "Moon": "Cancer", "Mars": "Virgo", "Mercury": "Libra",
         "Jupiter": "Sagittarius", "Venus": "Libra", "Saturn": "Leo",
         "Rahu": "Gemini", "Ketu": "Sagittarius"},
    ),
    (
        "Swami Vivekananda", 1863, 1, 12, 6, 33, 22.5726, 88.3639, 5.884,
        "Sagittarius", "Hasta",
        {"Sun": "Sagittarius", "Moon": "Virgo", "Mars": "Aries", "Mercury": "Capricorn",
         "Jupiter": "Libra", "Venus": "Capricorn", "Saturn": "Virgo",
         "Rahu": "Scorpio", "Ketu": "Taurus"},
    ),
    (
        "Albert Einstein", 1879, 3, 14, 11, 30, 48.4011, 9.9876, 1.0,
        "Gemini", "Jyeshtha",
        {"Sun": "Pisces", "Moon": "Scorpio", "Mars": "Capricorn", "Mercury": "Pisces",
         "Jupiter": "Aquarius", "Venus": "Pisces", "Saturn": "Pisces",
         "Rahu": "Capricorn", "Ketu": "Cancer"},
    ),
    (
        "Steve Jobs", 1955, 2, 24, 19, 15, 37.7749, -122.4194, -8.0,
        "Leo", "Uttara Bhadrapada",
        {"Sun": "Aquarius", "Moon": "Pisces", "Mars": "Aries", "Mercury": "Capricorn",
         "Jupiter": "Gemini", "Venus": "Sagittarius", "Saturn": "Libra",
         "Rahu": "Sagittarius", "Ketu": "Gemini"},
    ),
    (
        "Narendra Modi", 1950, 9, 17, 11, 0, 23.7871, 72.6375, 5.5,
        "Scorpio", "Anuradha",
        {"Sun": "Virgo", "Moon": "Scorpio", "Mars": "Scorpio", "Mercury": "Virgo",
         "Jupiter": "Aquarius", "Venus": "Leo", "Saturn": "Leo",
         "Rahu": "Pisces", "Ketu": "Virgo"},
    ),
    (
        "Amitabh Bachchan", 1942, 10, 11, 16, 0, 25.4358, 81.8463, 5.5,
        "Aquarius", "Swati",
        {"Sun": "Virgo", "Moon": "Libra", "Mars": "Virgo", "Mercury": "Virgo",
         "Jupiter": "Cancer", "Venus": "Virgo", "Saturn": "Taurus",
         "Rahu": "Leo", "Ketu": "Aquarius"},
    ),
    (
        "Sachin Tendulkar", 1973, 4, 24, 16, 25, 19.0760, 72.8777, 5.5,
        "Virgo", "Purva Ashadha",
        {"Sun": "Aries", "Moon": "Sagittarius", "Mars": "Capricorn", "Mercury": "Pisces",
         "Jupiter": "Capricorn", "Venus": "Aries", "Saturn": "Taurus",
         "Rahu": "Sagittarius", "Ketu": "Gemini"},
    ),
    (
        "APJ Abdul Kalam", 1931, 10, 15, 1, 0, 9.2876, 79.3129, 5.5,
        "Cancer", "Anuradha",
        {"Sun": "Virgo", "Moon": "Scorpio", "Mars": "Libra", "Mercury": "Virgo",
         "Jupiter": "Cancer", "Venus": "Libra", "Saturn": "Sagittarius",
         "Rahu": "Pisces", "Ketu": "Virgo"},
    ),
]

PLANETS = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]


def verify_chart(test_case):
    (name, year, month, day, hour, minute, lat, lon, tz,
     exp_lagna, exp_moon_nak, exp_planets) = test_case

    chart = compute_chart(year, month, day, hour, minute, lat, lon, tz, name)

    results = {"name": name, "issues": [], "passed": 0, "total": 0}

    # Check lagna
    results["total"] += 1
    actual_lagna = chart["lagna"]["sign_name"]
    if actual_lagna == exp_lagna:
        results["passed"] += 1
    else:
        results["issues"].append(f"Lagna: expected {exp_lagna}, got {actual_lagna}")

    # Check moon nakshatra
    results["total"] += 1
    moon_planet = next(p for p in chart["planets"] if p["name"] == "Moon")
    actual_moon_nak = moon_planet["nakshatra"]
    if actual_moon_nak == exp_moon_nak:
        results["passed"] += 1
    else:
        results["issues"].append(f"Moon Nakshatra: expected {exp_moon_nak}, got {actual_moon_nak}")

    # Check planet signs
    for planet_name in PLANETS:
        results["total"] += 1
        planet = next(p for p in chart["planets"] if p["name"] == planet_name)
        actual_sign = planet["sign_name"]
        expected_sign = exp_planets.get(planet_name)
        if actual_sign == expected_sign:
            results["passed"] += 1
        else:
            results["issues"].append(f"{planet_name}: expected {expected_sign}, got {actual_sign}")

    return results


def main():
    print("=" * 80)
    print("JYOTISH CHART VERIFICATION (Lahiri Ayanamsa)")
    print("Reference: AstroSage celebrity horoscopes")
    print("=" * 80)
    print()

    total_pass = 0
    total_checks = 0
    all_results = []

    for tc in TEST_CASES:
        result = verify_chart(tc)
        all_results.append(result)
        total_pass += result["passed"]
        total_checks += result["total"]

        status = "PASS" if not result["issues"] else "FAIL"
        score = f"{result['passed']}/{result['total']}"
        print(f"{'[PASS]' if not result['issues'] else '[FAIL]'} {result['name']} — {score}")
        if result["issues"]:
            for issue in result["issues"]:
                print(f"       -> {issue}")
        print()

    print("=" * 80)
    print(f"TOTAL: {total_pass}/{total_checks} checks passed "
          f"({total_pass/total_checks*100:.1f}%)")

    failed = [r for r in all_results if r["issues"]]
    if not failed:
        print("All charts match reference data!")
    else:
        print(f"{len(failed)} chart(s) had mismatches.")
    print("=" * 80)


if __name__ == "__main__":
    main()
