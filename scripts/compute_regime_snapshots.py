import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


def safe_zscore(series: pd.Series, window: int = 12) -> pd.Series:
    rolling_mean = series.rolling(window).mean()
    rolling_std = series.rolling(window).std()

    z = (series - rolling_mean) / rolling_std
    return z.replace([float("inf"), float("-inf")], pd.NA)


def load_macro_features() -> pd.DataFrame:
    result = (
        supabase.table("macro_features")
        .select("as_of_date,feature_name,feature_value")
        .execute()
    )

    df = pd.DataFrame(result.data)

    if df.empty:
        print("macro_features table is empty")
        raise SystemExit

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["feature_value"] = pd.to_numeric(df["feature_value"], errors="coerce")

    df = df.pivot_table(
        index="as_of_date",
        columns="feature_name",
        values="feature_value"
    )

    df = df.sort_index()

    # forward fill slower-moving / released-less-frequently data
    df = df.ffill()

    print("Available columns:")
    print(df.columns.tolist())

    return df


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    # ---------------------------
    # Growth block
    # ---------------------------
    # GDP anchor
    if "gdp_growth_real" in df.columns and "gdp_growth_4q_avg" in df.columns:
        df["gdp_signal"] = df["gdp_growth_real"] - df["gdp_growth_4q_avg"]
        df["gdp_z"] = safe_zscore(df["gdp_signal"], window=12)

    # IIP monthly pulse
    if "iip_yoy_change" in df.columns:
        df["iip_z"] = safe_zscore(df["iip_yoy_change"], window=12)

    # GST monthly pulse
    if "gst_3m_yoy_avg" in df.columns:
        df["gst_z"] = safe_zscore(df["gst_3m_yoy_avg"], window=12)

    growth_inputs = []
    if "gdp_z" in df.columns:
        growth_inputs.append(("gdp_z", 0.4))
    if "iip_z" in df.columns:
        growth_inputs.append(("iip_z", 0.3))
    if "gst_z" in df.columns:
        growth_inputs.append(("gst_z", 0.3))

    if growth_inputs:
        df["growth_score"] = 0.0
        total_weight = 0.0

        for col, weight in growth_inputs:
            df["growth_score"] = df["growth_score"] + df[col].fillna(0) * weight
            total_weight += weight

        # only keep values where at least one core signal exists
        valid_mask = df[[col for col, _ in growth_inputs]].notna().any(axis=1)
        df.loc[~valid_mask, "growth_score"] = pd.NA

    # ---------------------------
    # Inflation block
    # ---------------------------
    if "cpi_headline_index_yoy_change" in df.columns:
        df["inflation_z"] = safe_zscore(df["cpi_headline_index_yoy_change"], window=12)
        df["inflation_score"] = df["inflation_z"]

    # ---------------------------
    # Policy block
    # ---------------------------
    if "repo_rate" in df.columns and "cpi_headline_index_yoy_change" in df.columns:
        df["real_policy_rate"] = df["repo_rate"] - df["cpi_headline_index_yoy_change"]
        df["real_policy_rate_z"] = safe_zscore(df["real_policy_rate"], window=12)

        # tighter policy = more negative score
        df["policy_score"] = -df["real_policy_rate_z"]

    # ---------------------------
    # External block
    # ---------------------------
    if "oil_mom_change" in df.columns:
        df["oil_z"] = safe_zscore(df["oil_mom_change"], window=12)

    if "usd_inr_mom_change" in df.columns:
        df["usd_inr_z"] = safe_zscore(df["usd_inr_mom_change"], window=12)

    if "oil_z" in df.columns and "usd_inr_z" in df.columns:
        # oil up = bad for India
        # USD/INR up = INR weaker = bad for India
        df["external_score"] = -0.7 * df["oil_z"] - 0.3 * df["usd_inr_z"]
    elif "oil_z" in df.columns:
        df["external_score"] = -df["oil_z"]
    elif "usd_inr_z" in df.columns:
        df["external_score"] = -df["usd_inr_z"]

    return df


def classify_regime(row: pd.Series):
    g = row.get("growth_score")
    i = row.get("inflation_score")

    if pd.isna(g) or pd.isna(i):
        return None, None

    if g > 0 and i < 0:
        return "Goldilocks Expansion", "Growth accelerating while inflation cools"
    if g > 0 and i > 0:
        return "Overheating Economy", "Growth strong but inflation rising"
    if g < 0 and i > 0:
        return "Stagflation Risk", "Growth slowing while inflation remains high"
    if g < 0 and i < 0:
        return "Slowdown / Disinflation", "Growth slowing and inflation easing"

    return None, None


def build_rows(df: pd.DataFrame):
    df = df.reset_index()

    df[["regime_label", "explanation"]] = df.apply(
        lambda r: pd.Series(classify_regime(r)),
        axis=1
    )

    rows = []

    for _, r in df.iterrows():
        if pd.isna(r.get("regime_label")):
            continue

        rows.append({
            "as_of_date": str(pd.to_datetime(r["as_of_date"]).date()),
            "growth_score": round(float(r["growth_score"]), 4) if pd.notna(r.get("growth_score")) else None,
            "inflation_score": round(float(r["inflation_score"]), 4) if pd.notna(r.get("inflation_score")) else None,
            "policy_score": round(float(r["policy_score"]), 4) if pd.notna(r.get("policy_score")) else None,
            "external_score": round(float(r["external_score"]), 4) if pd.notna(r.get("external_score")) else None,
            "regime_label": r["regime_label"],
            "explanation": r["explanation"]
        })

    return rows


def upsert_rows(rows):
    if not rows:
        print("No regime rows generated. Check feature inputs.")
        raise SystemExit

    print("Rows prepared:", len(rows))
    print("Sample rows:")
    for row in rows[-5:]:
        print(row)

    result = supabase.table("regime_snapshots").upsert(
        rows,
        on_conflict="as_of_date"
    ).execute()

    print("Inserted/updated regime snapshots:", len(rows))
    print(result)


def main():
    df = load_macro_features()
    df = compute_scores(df)
    rows = build_rows(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()