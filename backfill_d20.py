#!/usr/bin/env python3
"""Backfill script: recompute all saved charts to add D20 Vimshamsha."""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

from database import init_db, get_all_charts_for_backfill, bulk_update_chart_data
from jyotish_engine import compute_chart

def main():
    init_db()
    charts = get_all_charts_for_backfill()
    print(f"Found {len(charts)} saved charts to backfill")

    updates = []
    for chart_id, input_data, chart_data in charts:
        try:
            inp = input_data
            new_chart = compute_chart(
                inp["year"], inp["month"], inp["day"],
                inp.get("hour", 12), inp.get("minute", 0),
                inp["lat"], inp["lon"],
                inp.get("tz_offset", 5.5),
                inp.get("place", "")
            )
            updates.append((chart_id, new_chart))
            print(f"  Recomputed chart {chart_id}")
        except Exception as e:
            print(f"  SKIP chart {chart_id}: {e}")

    if updates:
        bulk_update_chart_data(updates)
        print(f"Updated {len(updates)} charts")
    else:
        print("No charts to update")

if __name__ == "__main__":
    main()
