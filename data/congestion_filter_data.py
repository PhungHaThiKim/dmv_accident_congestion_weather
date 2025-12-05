import os
import re
import pandas as pd

BASE_DIR = "data/"
cong_path = BASE_DIR + "us_congestion_2016_2022/us_congestion_2016_2022_sample_2m.csv"

OUT_DIR = os.path.join(BASE_DIR, "cities_filtered_congestion")
os.makedirs(OUT_DIR, exist_ok=True)

CHUNK = 200_000


def split_time(df, start_col, end_col):
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce", utc=True)
    df[end_col]   = pd.to_datetime(df[end_col],   errors="coerce", utc=True)
    df = df.dropna(subset=[start_col, end_col])

    r16_start = pd.Timestamp("2016-01-01", tz="UTC")
    r21_end   = pd.Timestamp("2021-12-31 23:59:59", tz="UTC")
    r22_start = pd.Timestamp("2022-01-01", tz="UTC")
    r22_end   = pd.Timestamp("2022-12-31 23:59:59", tz="UTC")

    df_16_21 = df[(df[start_col] <= r21_end) & (df[end_col] >= r16_start)]
    df_22    = df[(df[start_col] <= r22_end) & (df[end_col] >= r22_start)]
    return df_16_21, df_22


# 4 city đại diện vùng
CITIES = {
    "new_york":    {"state": "NY", "pattern": r"\bNew York\b"},
    "chicago":     {"state": "IL", "pattern": r"\bChicago\b"},
    "miami":       {"state": "FL", "pattern": r"\bMiami\b"},
    "los_angeles": {"state": "CA", "pattern": r"\bLos Angeles\b"},
}

# chuẩn bị output paths + xoá file cũ
out_paths = {}
first_write = {}
totals = {}

for key in CITIES:
    out_16_21 = os.path.join(OUT_DIR, f"{key}_congestion_2016_2021.csv")
    out_22    = os.path.join(OUT_DIR, f"{key}_congestion_2022.csv")
    out_paths[key] = (out_16_21, out_22)

    for p in (out_16_21, out_22):
        if os.path.exists(p):
            os.remove(p)

    first_write[key] = {"16_21": True, "22": True}
    totals[key] = {"all": 0, "16_21": 0, "22": 0}

# compile regex cho nhanh
for key in CITIES:
    CITIES[key]["regex"] = re.compile(CITIES[key]["pattern"], flags=re.IGNORECASE)


for chunk in pd.read_csv(cong_path, chunksize=CHUNK, low_memory=False):
    city_col = chunk["City"].astype(str).str.strip()  # strip để sạch space thừa

    for key, cfg in CITIES.items():
        state_code = cfg["state"]
        pat = cfg["regex"]

        sub = chunk[
            chunk["State"].eq(state_code) &
            city_col.str.contains(pat, na=False)
        ].copy()

        if sub.empty:
            continue

        totals[key]["all"] += len(sub)

        df16, df22 = split_time(sub, "StartTime", "EndTime")
        out_16_21, out_22 = out_paths[key]

        if not df16.empty:
            df16.to_csv(out_16_21, mode="a", index=False, header=first_write[key]["16_21"])
            first_write[key]["16_21"] = False
            totals[key]["16_21"] += len(df16)

        if not df22.empty:
            df22.to_csv(out_22, mode="a", index=False, header=first_write[key]["22"])
            first_write[key]["22"] = False
            totals[key]["22"] += len(df22)

    print("...processed chunk")


print("\nCONGESTION DONE")
for key in CITIES:
    print(
        f"{key.upper():12s} total: {totals[key]['all']:,} "
        f"| 2016-21: {totals[key]['16_21']:,} | 2022: {totals[key]['22']:,}"
    )
    print("Saved:", *out_paths[key])
