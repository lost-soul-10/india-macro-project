import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

Z_WINDOW = 12
FFILL_LIMIT = 6
SMOOTHING_WINDOW = 3
INFLATION_MOMENTUM_WINDOW = 6
EXTERNAL_SMOOTH_WINDOW = 3

ENABLE_REGIME_METADATA = os.getenv("ENABLE_REGIME_METADATA", "0").strip() in {"1", "true", "True", "yes", "YES"}
REGIME_VERSION = "v9_policy_zscore_with_date_cutoff"

EARLY_MIN_PERIODS = 6
REGIME_THRESHOLD = float(os.getenv("REGIME_THRESHOLD", "0.35"))
STAGFLATION_G_THRESHOLD = float(os.getenv("STAGFLATION_G_THRESHOLD", "0.6"))
STAGFLATION_I_THRESHOLD = float(os.getenv("STAGFLATION_I_THRESHOLD", "0.6"))
STAGFLATION_REQUIRE_HEATING = os.getenv("STAGFLATION_REQUIRE_HEATING", "1").strip() in {"1", "true", "True", "yes", "YES"}
STAGFLATION_CPI_MIN = float(os.getenv("STAGFLATION_CPI_MIN", "4.0"))
OVERHEATING_CPI_MIN = float(os.getenv("OVERHEATING_CPI_MIN", "4.5"))

CPI_BREAK_DATE = os.getenv("CPI_BREAK_DATE")
WPI_BREAK_DATE = os.getenv("WPI_BREAK_DATE")
LATEST_ALLOWED_DATE = os.getenv("LATEST_ALLOWED_DATE")


def robust_zscore(series: pd.Series, window: int = Z_WINDOW, min_periods: int | None = None) -> pd.Series:
    if min_periods is None:
        min_periods = max(EARLY_MIN_PERIODS, window // 3)

    rolling_median = series.rolling(window=window, min_periods=min_periods).median()
    mad = (series - rolling_median).abs().rolling(window=window, min_periods=min_periods).median()
    denom = (1.4826 * mad).mask(mad == 0)

    z = (series - rolling_median) / denom
    return z.replace([float("inf"), float("-inf")], pd.NA)


def robust_zscore_adaptive(series: pd.Series, window: int = Z_WINDOW) -> pd.Series:
    s = series.astype("float64")

    exp_med = s.expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_mad = (s - exp_med).abs().expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_denom = (1.4826 * exp_mad).mask(exp_mad == 0)
    exp_z = (s - exp_med) / exp_denom

    roll_z = robust_zscore(s, window=window)
    out = roll_z.where(roll_z.notna(), exp_z)

    return out.replace([float("inf"), float("-inf")], pd.NA)


def weighted_rowwise_average(df: pd.DataFrame, inputs: list[tuple[str, float]]) -> pd.Series:
    cols = [c for c, _ in inputs]
    weights = pd.Series({c: w for c, w in inputs}, dtype="float64")

    avail = df[cols].notna()
    denom = avail.mul(weights, axis=1).sum(axis=1)
    numer = df[cols].mul(weights, axis=1).where(avail).sum(axis=1)

    out = numer / denom
    return out.mask(denom == 0)


def zscore_with_optional_break(series: pd.Series, break_date: str | None) -> pd.Series:
    if not break_date:
        return robust_zscore_adaptive(series)

    bd = pd.to_datetime(break_date)
    pre = series.loc[series.index < bd]
    post = series.loc[series.index >= bd]

    z_pre = robust_zscore_adaptive(pre) if not pre.empty else pre
    z_post = robust_zscore_adaptive(post) if not post.empty else post

    out = pd.concat([z_pre, z_post]).sort_index()
    return out.reindex(series.index)


def map_regime_bucket(regime_label: str) -> str:
    if regime_label == "Overheating Economy":
        return "Overheating"
    if regime_label in ["Goldilocks Expansion", "Expansion (Inflation Neutral)"]:
        return "Expansion"
    if regime_label in ["Slowdown / Disinflation", "Slowdown (Inflation Neutral)"]:
        return "Slowdown"
    if regime_label == "Disinflation (Growth Neutral)":
        return "Disinflation"
    if regime_label in ["Inflation Shock (Growth Neutral)", "Stagflation Risk"]:
        return "Stagflation"
    if regime_label in ["Neutral / Transition", "Neutral / Mixed", "Inflation Firming (Growth Neutral)"]:
        return "Neutral / Mixed"
    return "Neutral / Mixed"


def map_regime_color(regime_bucket: str) -> str:
    color_map = {
        "Overheating": "#C96A2B",
        "Expansion": "#2F8F83",
        "Slowdown": "#4B7BEC",
        "Disinflation": "#6C8AE4",
        "Stagflation": "#B24C63",
        "Neutral / Mixed": "#8B7CF6",
        "Default": "#9CA3AF",
    }
    return color_map.get(regime_bucket, color_map["Default"])


def score_band(value) -> str:
    if pd.isna(value):
        return "missing"

    v = float(value)
    if v <= -1.0:
        return "strong_negative"
    if v <= -0.35:
        return "negative"
    if v < 0.35:
        return "neutral"
    if v < 1.0:
        return "positive"
    return "strong_positive"


def load_macro_features() -> pd.DataFrame:
    all_rows = []
    page_size = 1000
    start = 0

    while True:
        result = (
            supabase.table("macro_features")
            .select("as_of_date,feature_name,feature_value")
            .order("as_of_date")
            .range(start, start + page_size - 1)
            .execute()
        )

        batch = result.data or []
        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("macro_features table is empty")
        raise SystemExit

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["feature_value"] = pd.to_numeric(df["feature_value"], errors="coerce")

    df = df.pivot_table(
        index="as_of_date",
        columns="feature_name",
        values="feature_value",
        aggfunc="last"
    )

    df = df.sort_index()
    df = df.resample("MS").last()
    df = df.ffill(limit=FFILL_LIMIT)

    if "repo_rate" in df.columns:
        df["repo_rate"] = df["repo_rate"].ffill()

    if LATEST_ALLOWED_DATE:
        cutoff = pd.to_datetime(LATEST_ALLOWED_DATE)
        df = df.loc[df.index <= cutoff]

    print("Loaded macro_features rows:", len(all_rows))
    print("Available columns:")
    print(df.columns.tolist())
    print("Max date after resample/cutoff:", df.index.max())
    print("Monthly index preview:")
    print(df.index[-12:])

    return df


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    # Growth
    if "gdp_growth_real" in df.columns and "gdp_growth_4q_avg" in df.columns:
        df["gdp_signal"] = df["gdp_growth_real"] - df["gdp_growth_4q_avg"]
        df["gdp_z"] = robust_zscore_adaptive(df["gdp_signal"])
    elif "gdp_growth_real" in df.columns and "gdp_growth_4q_avg" not in df.columns:
        df["gdp_growth_4q_avg"] = df["gdp_growth_real"].rolling(window=4, min_periods=2).mean()
        df["gdp_signal"] = df["gdp_growth_real"] - df["gdp_growth_4q_avg"]
        df["gdp_z"] = robust_zscore_adaptive(df["gdp_signal"])

    if "iip_yoy_change" in df.columns:
        df["iip_smooth"] = df["iip_yoy_change"].rolling(window=3, min_periods=1).mean()
        df["iip_z"] = robust_zscore_adaptive(df["iip_smooth"])

    if "gst_3m_yoy_avg" in df.columns:
        df["gst_z"] = robust_zscore_adaptive(df["gst_3m_yoy_avg"])
    elif "gst_yoy_change" in df.columns:
        df["gst_smooth"] = df["gst_yoy_change"].rolling(window=3, min_periods=1).mean()
        df["gst_z"] = robust_zscore_adaptive(df["gst_smooth"])

    growth_inputs = []
    if "gdp_z" in df.columns:
        growth_inputs.append(("gdp_z", 0.5))
    if "iip_z" in df.columns:
        growth_inputs.append(("iip_z", 0.2))
    if "gst_z" in df.columns:
        growth_inputs.append(("gst_z", 0.3))

    if growth_inputs:
        df["growth_score"] = weighted_rowwise_average(df, growth_inputs)

    # Inflation
    if "cpi_headline_index_yoy_change" in df.columns:
        cpi_yoy = df["cpi_headline_index_yoy_change"]
        df["cpi_level_actual"] = cpi_yoy

        df["cpi_yoy_smooth"] = cpi_yoy.rolling(window=3, min_periods=2).mean()
        df["inflation_level_z"] = zscore_with_optional_break(df["cpi_yoy_smooth"], CPI_BREAK_DATE)

        df["cpi_yoy_trend"] = df["cpi_yoy_smooth"].rolling(
            window=INFLATION_MOMENTUM_WINDOW,
            min_periods=max(3, INFLATION_MOMENTUM_WINDOW // 2)
        ).mean()

        df["inflation_momentum"] = df["cpi_yoy_smooth"] - df["cpi_yoy_trend"]
        df["inflation_momentum_z"] = zscore_with_optional_break(df["inflation_momentum"], CPI_BREAK_DATE)

        df["inflation_score_cpi_core"] = 0.7 * df["inflation_level_z"] + 0.3 * df["inflation_momentum_z"]
        df["inflation_score"] = df["inflation_score_cpi_core"]
    else:
        df["cpi_level_actual"] = pd.NA

    # Optional WPI add-on
    if "wpi_yoy_change" in df.columns:
        df["wpi_yoy_smooth"] = df["wpi_yoy_change"].rolling(window=3, min_periods=2).mean()
        df["wpi_level_z"] = zscore_with_optional_break(df["wpi_yoy_smooth"], WPI_BREAK_DATE)

        if "inflation_score_cpi_core" in df.columns:
            df["inflation_score"] = weighted_rowwise_average(
                df,
                [
                    ("inflation_score_cpi_core", 0.8),
                    ("wpi_level_z", 0.2),
                ]
            )
        else:
            df["inflation_score"] = df["wpi_level_z"]

    # Policy
    if "repo_rate" in df.columns and "cpi_headline_index_yoy_change" in df.columns:
        df["real_policy_rate"] = df["repo_rate"] - df["cpi_headline_index_yoy_change"]
        df["real_policy_rate_smooth"] = df["real_policy_rate"].rolling(window=3, min_periods=1).mean()
        df["real_policy_rate_z"] = robust_zscore_adaptive(df["real_policy_rate_smooth"])
        df["policy_score"] = -df["real_policy_rate_z"]

    # External
    if "oil_mom_change" in df.columns:
        oil = df["oil_mom_change"].rolling(window=EXTERNAL_SMOOTH_WINDOW, min_periods=1).mean()
        df["oil_z"] = robust_zscore_adaptive(oil)

    if "usd_inr_3m_change" in df.columns:
        df["usd_inr_z"] = robust_zscore_adaptive(df["usd_inr_3m_change"])
    elif "usd_inr_mom_change" in df.columns:
        fx = df["usd_inr_mom_change"].rolling(window=EXTERNAL_SMOOTH_WINDOW, min_periods=1).mean()
        df["usd_inr_z"] = robust_zscore_adaptive(fx)

    if "oil_z" in df.columns and "usd_inr_z" in df.columns:
        df["external_score"] = -0.7 * df["oil_z"] - 0.3 * df["usd_inr_z"]
    elif "oil_z" in df.columns:
        df["external_score"] = -df["oil_z"]
    elif "usd_inr_z" in df.columns:
        df["external_score"] = -df["usd_inr_z"]

    for col in ["growth_score", "inflation_score", "policy_score", "external_score"]:
        if col in df.columns:
            df[f"{col}_smoothed"] = df[col].rolling(window=SMOOTHING_WINDOW, min_periods=1).mean()

    return df


def classify_regime(row: pd.Series):
    g = row.get("growth_score_smoothed", row.get("growth_score"))
    i = row.get("inflation_score_smoothed", row.get("inflation_score"))
    infl_mom = row.get("inflation_momentum_z")
    cpi_actual = row.get("cpi_level_actual")

    if pd.isna(g) or pd.isna(i):
        return None, None

    g = float(g)
    i = float(i)
    infl_mom_val = None if pd.isna(infl_mom) else float(infl_mom)
    cpi_actual_val = None if pd.isna(cpi_actual) else float(cpi_actual)

    if abs(g) < REGIME_THRESHOLD and abs(i) < REGIME_THRESHOLD:
        return "Neutral / Transition", "Signals are close to trend; regime is not strongly defined"

    if g > REGIME_THRESHOLD and i < -REGIME_THRESHOLD:
        return "Goldilocks Expansion", "Growth is above trend while inflation pressure is easing"

    if (
        g > REGIME_THRESHOLD
        and i > REGIME_THRESHOLD
        and cpi_actual_val is not None
        and cpi_actual_val >= OVERHEATING_CPI_MIN
    ):
        return "Overheating Economy", "Growth is strong and inflation pressure is meaningfully elevated"

    if (
        g < -STAGFLATION_G_THRESHOLD
        and i > STAGFLATION_I_THRESHOLD
        and cpi_actual_val is not None
        and cpi_actual_val >= STAGFLATION_CPI_MIN
    ):
        if (not STAGFLATION_REQUIRE_HEATING) or (infl_mom_val is not None and infl_mom_val >= 0):
            return "Stagflation Risk", "Growth is weak while inflation is elevated and not clearly cooling"

    if g < -REGIME_THRESHOLD and i < -REGIME_THRESHOLD:
        return "Slowdown / Disinflation", "Growth is below trend and inflation pressure is easing"

    if g > REGIME_THRESHOLD and abs(i) <= REGIME_THRESHOLD:
        return "Expansion (Inflation Neutral)", "Growth is above trend; inflation pressure is near baseline"

    if g < -REGIME_THRESHOLD and abs(i) <= REGIME_THRESHOLD:
        return "Slowdown (Inflation Neutral)", "Growth is below trend; inflation pressure is near baseline"

    if i > REGIME_THRESHOLD and abs(g) <= REGIME_THRESHOLD:
        if cpi_actual_val is not None and cpi_actual_val >= STAGFLATION_CPI_MIN:
            return "Inflation Shock (Growth Neutral)", "Inflation pressure is elevated while growth is near baseline"
        return "Inflation Firming (Growth Neutral)", "Inflation is running above trend, but not at a clearly problematic level"

    if i < -REGIME_THRESHOLD and abs(g) <= REGIME_THRESHOLD:
        return "Disinflation (Growth Neutral)", "Inflation pressure is easing; growth is near baseline"

    return "Neutral / Mixed", "Signals are mixed and do not point to a strong regime tilt"


def build_rows(df: pd.DataFrame):
    df = df.reset_index().rename(columns={"index": "as_of_date"})
    df[["regime_label", "explanation"]] = df.apply(lambda r: pd.Series(classify_regime(r)), axis=1)

    rows = []
    for _, r in df.iterrows():
        if pd.isna(r.get("regime_label")):
            continue

        g_used = r.get("growth_score_smoothed", r.get("growth_score"))
        i_used = r.get("inflation_score_smoothed", r.get("inflation_score"))
        p_used = r.get("policy_score_smoothed", r.get("policy_score"))
        e_used = r.get("external_score_smoothed", r.get("external_score"))

        regime_label = r["regime_label"]
        regime_bucket = map_regime_bucket(regime_label)
        regime_color = map_regime_color(regime_bucket)

        driver_tags: list[str] = []
        infl_mom = r.get("inflation_momentum_z")

        if pd.notna(p_used) and float(p_used) < -0.75:
            driver_tags.append("policy_tight")
        if pd.notna(p_used) and float(p_used) > 0.75:
            driver_tags.append("policy_supportive")
        if pd.notna(e_used) and float(e_used) < -0.75:
            driver_tags.append("external_stress")
        if pd.notna(e_used) and float(e_used) > 0.75:
            driver_tags.append("external_tailwind")
        if pd.notna(infl_mom) and float(infl_mom) > 0.75:
            driver_tags.append("inflation_heating")
        if pd.notna(infl_mom) and float(infl_mom) < -0.75:
            driver_tags.append("inflation_cooling")

        core_vals = [g_used, i_used, p_used, e_used]
        available_frac = sum(pd.notna(v) for v in core_vals) / 4.0

        strength = 0.0
        for v in [g_used, i_used]:
            if pd.notna(v):
                strength += min(abs(float(v)) / 2.0, 1.0)
        strength = strength / 2.0

        confidence = round(0.7 * available_frac + 0.3 * strength, 3)

        row_out = {
            "as_of_date": str(pd.to_datetime(r["as_of_date"]).date()),
            "growth_score": round(float(g_used), 4) if pd.notna(g_used) else None,
            "inflation_score": round(float(i_used), 4) if pd.notna(i_used) else None,
            "policy_score": round(float(p_used), 4) if pd.notna(p_used) else None,
            "external_score": round(float(e_used), 4) if pd.notna(e_used) else None,
            "growth_band": score_band(g_used),
            "inflation_band": score_band(i_used),
            "policy_band": score_band(p_used),
            "external_band": score_band(e_used),
            "regime_label": regime_label,
            "regime_bucket": regime_bucket,
            "regime_color": regime_color,
            "explanation": r["explanation"],
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

    result = supabase.table("regime_snapshots").upsert(rows, on_conflict="as_of_date").execute()
    print("Inserted/updated regime snapshots:", len(rows))
    print(result)


def main():
    df = load_macro_features()
    df = compute_scores(df)

    debug_cols = [
        col for col in [
            "growth_score",
            "growth_score_smoothed",
            "inflation_score",
            "inflation_score_smoothed",
            "inflation_score_cpi_core",
            "inflation_level_z",
            "inflation_momentum_z",
            "wpi_level_z",
            "policy_score",
            "policy_score_smoothed",
            "external_score",
            "external_score_smoothed",
            "cpi_level_actual",
            "repo_rate",
            "real_policy_rate",
            "real_policy_rate_smooth",
            "real_policy_rate_z",
            "cpi_headline_index_yoy_change",
            "wpi_yoy_change",
        ] if col in df.columns
    ]

    if debug_cols:
        print("Debug preview:")
        print(df[debug_cols].tail(12))

    rows = build_rows(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()