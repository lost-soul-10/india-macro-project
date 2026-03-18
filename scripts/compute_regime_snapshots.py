import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

Z_WINDOW = 36
FFILL_LIMIT = 6
SMOOTHING_WINDOW = 3
INFLATION_MOMENTUM_WINDOW = 6
EXTERNAL_SMOOTH_WINDOW = 3
ENABLE_REGIME_METADATA = os.getenv("ENABLE_REGIME_METADATA", "0").strip() in {"1", "true", "True", "yes", "YES"}
REGIME_VERSION = "v2_growth_inflation_level_momentum_overlay"
EARLY_MIN_PERIODS = 6


def safe_zscore(series: pd.Series, window: int = Z_WINDOW, min_periods: int | None = None) -> pd.Series:
    if min_periods is None:
        min_periods = max(6, window // 2)

    rolling_mean = series.rolling(window=window, min_periods=min_periods).mean()
    rolling_std = series.rolling(window=window, min_periods=min_periods).std()
    rolling_std = rolling_std.mask(rolling_std == 0)

    z = (series - rolling_mean) / rolling_std
    return z.replace([float("inf"), float("-inf")], pd.NA)


def robust_zscore(series: pd.Series, window: int = Z_WINDOW, min_periods: int | None = None) -> pd.Series:
    """
    Rolling robust z-score using median and MAD.
    Uses MAD scaling factor 1.4826 so it is comparable to std for normal data.
    """
    # Using window//2 is often too strict for shorter histories (e.g., India features since 2022).
    # This default still avoids very early noisy z-scores while allowing history to show up sooner.
    if min_periods is None:
        min_periods = max(EARLY_MIN_PERIODS, window // 3)

    rolling_median = series.rolling(window=window, min_periods=min_periods).median()
    mad = (series - rolling_median).abs().rolling(window=window, min_periods=min_periods).median()
    denom = (1.4826 * mad).mask(mad == 0)

    z = (series - rolling_median) / denom
    return z.replace([float("inf"), float("-inf")], pd.NA)


def robust_zscore_adaptive(series: pd.Series, window: int = Z_WINDOW) -> pd.Series:
    """
    Rolling robust z-score, but falls back to an expanding baseline early in the sample.
    This helps produce usable historical scores even when you have < window observations.
    """
    s = series.astype("float64")

    # Expanding baseline (early sample)
    exp_med = s.expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_mad = (s - exp_med).abs().expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_denom = (1.4826 * exp_mad).mask(exp_mad == 0)
    exp_z = (s - exp_med) / exp_denom

    # Rolling baseline (once enough history exists)
    roll_z = robust_zscore(s, window=window)

    # Use rolling when available, else expanding
    out = roll_z.where(roll_z.notna(), exp_z)
    return out.replace([float("inf"), float("-inf")], pd.NA)


def weighted_rowwise_average(df: pd.DataFrame, inputs: list[tuple[str, float]]) -> pd.Series:
    """
    Weighted average per-row, renormalizing weights to available (non-NA) inputs.
    Prevents missing series from being treated as 0 and biasing the score.
    """
    cols = [c for c, _ in inputs]
    weights = pd.Series({c: w for c, w in inputs}, dtype="float64")

    avail = df[cols].notna()
    denom = avail.mul(weights, axis=1).sum(axis=1)
    numer = df[cols].mul(weights, axis=1).where(avail).sum(axis=1)

    out = numer / denom
    return out.mask(denom == 0)


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
    df = df.ffill(limit=FFILL_LIMIT)

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
        df["gdp_z"] = robust_zscore_adaptive(df["gdp_signal"])
    elif "gdp_growth_real" in df.columns and "gdp_growth_4q_avg" not in df.columns:
        # Fallback: compute a simple 4-period trailing mean if the precomputed avg isn't available.
        df["gdp_growth_4q_avg"] = df["gdp_growth_real"].rolling(window=4, min_periods=2).mean()
        df["gdp_signal"] = df["gdp_growth_real"] - df["gdp_growth_4q_avg"]
        df["gdp_z"] = robust_zscore_adaptive(df["gdp_signal"])

    # IIP monthly pulse
    if "iip_yoy_change" in df.columns:
        df["iip_z"] = robust_zscore_adaptive(df["iip_yoy_change"])

    # GST monthly pulse
    if "gst_3m_yoy_avg" in df.columns:
        df["gst_z"] = robust_zscore_adaptive(df["gst_3m_yoy_avg"])

    growth_inputs = []
    if "gdp_z" in df.columns:
        growth_inputs.append(("gdp_z", 0.4))
    if "iip_z" in df.columns:
        growth_inputs.append(("iip_z", 0.3))
    if "gst_z" in df.columns:
        growth_inputs.append(("gst_z", 0.3))

    if growth_inputs:
        df["growth_score"] = weighted_rowwise_average(df, growth_inputs)

    # ---------------------------
    # Inflation block
    # ---------------------------
    if "cpi_headline_index_yoy_change" in df.columns or "cpi_headline_index_mom_change" in df.columns:
        # Prefer YoY when available; fall back to MoM pulse early in history.
        infl_level_series = (
            df["cpi_headline_index_yoy_change"]
            if "cpi_headline_index_yoy_change" in df.columns
            else df["cpi_headline_index_mom_change"]
        )

        # Level: where inflation stands vs its own history
        df["inflation_level_z"] = robust_zscore_adaptive(infl_level_series)

        # Momentum: whether inflation is heating up or cooling down recently
        # Using deviation from a rolling mean is more robust than MoM noise.
        infl = infl_level_series
        infl_trend = infl.rolling(window=INFLATION_MOMENTUM_WINDOW, min_periods=max(3, INFLATION_MOMENTUM_WINDOW // 2)).mean()
        df["inflation_momentum"] = infl - infl_trend
        df["inflation_momentum_z"] = robust_zscore_adaptive(df["inflation_momentum"])

        # Composite inflation score: higher = more inflation pressure (worse for Goldilocks)
        df["inflation_score"] = 0.6 * df["inflation_level_z"] + 0.4 * df["inflation_momentum_z"]

    # ---------------------------
    # Policy block
    # ---------------------------
    if "repo_rate" in df.columns and ("cpi_headline_index_yoy_change" in df.columns or "cpi_headline_index_mom_change" in df.columns):
        infl_for_real_rate = (
            df["cpi_headline_index_yoy_change"]
            if "cpi_headline_index_yoy_change" in df.columns
            else df["cpi_headline_index_mom_change"]
        )
        df["real_policy_rate"] = df["repo_rate"] - infl_for_real_rate
        df["real_policy_rate_z"] = robust_zscore_adaptive(df["real_policy_rate"])

        # tighter policy = more negative score
        df["policy_score"] = -df["real_policy_rate_z"]

    # ---------------------------
    # External block
    # ---------------------------
    if "oil_mom_change" in df.columns:
        oil = df["oil_mom_change"].rolling(window=EXTERNAL_SMOOTH_WINDOW, min_periods=1).mean()
        df["oil_z"] = robust_zscore_adaptive(oil)

    if "usd_inr_mom_change" in df.columns:
        fx = df["usd_inr_mom_change"].rolling(window=EXTERNAL_SMOOTH_WINDOW, min_periods=1).mean()
        df["usd_inr_z"] = robust_zscore_adaptive(fx)

    if "oil_z" in df.columns and "usd_inr_z" in df.columns:
        # oil up = bad for India
        # USD/INR up = INR weaker = bad for India
        df["external_score"] = -0.7 * df["oil_z"] - 0.3 * df["usd_inr_z"]
    elif "oil_z" in df.columns:
        df["external_score"] = -df["oil_z"]
    elif "usd_inr_z" in df.columns:
        df["external_score"] = -df["usd_inr_z"]

    # Optional smoothing to reduce month-to-month regime flips
    for col in ["growth_score", "inflation_score", "policy_score", "external_score"]:
        if col in df.columns:
            df[f"{col}_smoothed"] = df[col].rolling(window=SMOOTHING_WINDOW, min_periods=1).mean()

    return df


def classify_regime(row: pd.Series):
    g = row.get("growth_score_smoothed", row.get("growth_score"))
    i = row.get("inflation_score_smoothed", row.get("inflation_score"))

    if pd.isna(g) or pd.isna(i):
        return None, None

    # Note: inflation_score > 0 means inflation pressure is ABOVE its recent baseline (level+momentum)
    if g > 0 and i < 0:
        return "Goldilocks Expansion", "Growth is above trend while inflation pressure is easing"
    if g > 0 and i > 0:
        return "Overheating Economy", "Growth is strong but inflation pressure is building"
    if g < 0 and i > 0:
        return "Stagflation Risk", "Growth is below trend while inflation pressure remains elevated"
    if g < 0 and i < 0:
        return "Slowdown / Disinflation", "Growth is below trend and inflation pressure is easing"

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

        # Driver tags for UI + agent grounding (optional, to avoid breaking DB schema)
        driver_tags: list[str] = []
        policy = r.get("policy_score_smoothed", r.get("policy_score"))
        external = r.get("external_score_smoothed", r.get("external_score"))
        infl_mom = r.get("inflation_momentum_z")

        if pd.notna(policy) and float(policy) < -0.75:
            driver_tags.append("policy_tight")
        if pd.notna(policy) and float(policy) > 0.75:
            driver_tags.append("policy_supportive")
        if pd.notna(external) and float(external) < -0.75:
            driver_tags.append("external_stress")
        if pd.notna(external) and float(external) > 0.75:
            driver_tags.append("external_tailwind")
        if pd.notna(infl_mom) and float(infl_mom) > 0.75:
            driver_tags.append("inflation_heating")
        if pd.notna(infl_mom) and float(infl_mom) < -0.75:
            driver_tags.append("inflation_cooling")

        # Simple confidence: how much of the core blocks are present + signal strength
        core_vals = [
            r.get("growth_score"),
            r.get("inflation_score"),
            r.get("policy_score"),
            r.get("external_score"),
        ]
        available_frac = sum(pd.notna(v) for v in core_vals) / 4.0
        strength = 0.0
        for v in [r.get("growth_score_smoothed"), r.get("inflation_score_smoothed")]:
            if pd.notna(v):
                strength += min(abs(float(v)) / 2.0, 1.0)  # cap at |z|=2
        strength = strength / 2.0  # 0..1
        confidence = round(0.7 * available_frac + 0.3 * strength, 3)

        row_out = {
            "as_of_date": str(pd.to_datetime(r["as_of_date"]).date()),
            "growth_score": round(float(r["growth_score"]), 4) if pd.notna(r.get("growth_score")) else None,
            "inflation_score": round(float(r["inflation_score"]), 4) if pd.notna(r.get("inflation_score")) else None,
            "policy_score": round(float(r["policy_score"]), 4) if pd.notna(r.get("policy_score")) else None,
            "external_score": round(float(r["external_score"]), 4) if pd.notna(r.get("external_score")) else None,
            "regime_label": r["regime_label"],
            "explanation": r["explanation"]
        }

        if ENABLE_REGIME_METADATA:
            row_out["regime_version"] = REGIME_VERSION
            row_out["confidence"] = confidence
            row_out["driver_tags"] = driver_tags

        rows.append(row_out)

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