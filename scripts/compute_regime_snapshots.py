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
REGIME_VERSION = "v4_monthly_resampled_classification_aligned"
EARLY_MIN_PERIODS = 6
REGIME_THRESHOLD = float(os.getenv("REGIME_THRESHOLD", "0.35"))
POLICY_IMPULSE_LAG_MONTHS = int(os.getenv("POLICY_IMPULSE_LAG_MONTHS", "6"))
POLICY_IMPULSE_WEIGHT = float(os.getenv("POLICY_IMPULSE_WEIGHT", "0.6"))
POLICY_STANCE_WEIGHT = float(os.getenv("POLICY_STANCE_WEIGHT", "0.4"))
STAGFLATION_G_THRESHOLD = float(os.getenv("STAGFLATION_G_THRESHOLD", "0.6"))
STAGFLATION_I_THRESHOLD = float(os.getenv("STAGFLATION_I_THRESHOLD", "0.6"))
STAGFLATION_REQUIRE_HEATING = os.getenv("STAGFLATION_REQUIRE_HEATING", "1").strip() in {"1", "true", "True", "yes", "YES"}
STAGFLATION_CPI_MIN = float(os.getenv("STAGFLATION_CPI_MIN", "4.0"))
OVERHEATING_CPI_MIN = float(os.getenv("OVERHEATING_CPI_MIN", "4.5"))


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

    exp_med = s.expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_mad = (s - exp_med).abs().expanding(min_periods=EARLY_MIN_PERIODS).median()
    exp_denom = (1.4826 * exp_mad).mask(exp_mad == 0)
    exp_z = (s - exp_med) / exp_denom

    roll_z = robust_zscore(s, window=window)

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
        values="feature_value",
        aggfunc="last"
    )

    df = df.sort_index()

    # -------------------------------------------------
    # CRITICAL FIX 1:
    # Force mixed-frequency feature rows onto a clean
    # monthly month-start regime calendar.
    # -------------------------------------------------
    df = df.resample("MS").last()

    # Forward fill slower-moving / released-less-frequently data
    df = df.ffill(limit=FFILL_LIMIT)

    print("Available columns:")
    print(df.columns.tolist())
    print("Monthly index preview:")
    print(df.index[-12:])

    return df


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    # ---------------------------
    # Growth block
    # ---------------------------
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

    # ---------------------------
    # Inflation block
    # ---------------------------
    if "cpi_headline_index_yoy_change" in df.columns or "cpi_headline_index_mom_change" in df.columns:
        cpi_yoy = df["cpi_headline_index_yoy_change"] if "cpi_headline_index_yoy_change" in df.columns else None
        infl_level_series = (
            cpi_yoy if cpi_yoy is not None else df["cpi_headline_index_mom_change"]
        )

        # Guardrails (STAGFLATION_CPI_MIN / OVERHEATING_CPI_MIN) are defined in YoY % terms,
        # so only store YoY CPI here (leave NA if only MoM is available).
        df["cpi_level_actual"] = cpi_yoy

        df["inflation_level_z"] = robust_zscore_adaptive(infl_level_series)

        infl = infl_level_series
        infl_trend = infl.rolling(
            window=INFLATION_MOMENTUM_WINDOW,
            min_periods=max(3, INFLATION_MOMENTUM_WINDOW // 2)
        ).mean()
        df["inflation_momentum"] = infl - infl_trend
        df["inflation_momentum_z"] = robust_zscore_adaptive(df["inflation_momentum"])

        df["inflation_score"] = 0.6 * df["inflation_level_z"] + 0.4 * df["inflation_momentum_z"]

    # ---------------------------
    # Policy block
    # ---------------------------
    # Repo is a step function; for a monthly policy signal use "stance + impulse".
    # - Stance: level of real policy rate (restrictive vs accommodative)
    # - Impulse: change in real policy rate over POLICY_IMPULSE_LAG_MONTHS (tightening vs easing)
    if "repo_rate" in df.columns and "cpi_headline_index_yoy_change" in df.columns:
        df["real_policy_rate"] = df["repo_rate"] - df["cpi_headline_index_yoy_change"]

        df["real_policy_rate_z"] = robust_zscore_adaptive(df["real_policy_rate"])

        df["real_policy_impulse"] = df["real_policy_rate"] - df["real_policy_rate"].shift(POLICY_IMPULSE_LAG_MONTHS)
        df["real_policy_impulse_z"] = robust_zscore_adaptive(df["real_policy_impulse"])

        # tighter conditions = more negative score
        blended = (
            POLICY_IMPULSE_WEIGHT * df["real_policy_impulse_z"]
            + POLICY_STANCE_WEIGHT * df["real_policy_rate_z"]
        )
        df["policy_score"] = -blended

    # ---------------------------
    # External block
    # ---------------------------
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

    # -------------------------------------------------
    # Smooth classification inputs to reduce regime flips
    # -------------------------------------------------
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

    # Neutral / transition band
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

    df[["regime_label", "explanation"]] = df.apply(
        lambda r: pd.Series(classify_regime(r)),
        axis=1
    )

    rows = []

    for _, r in df.iterrows():
        if pd.isna(r.get("regime_label")):
            continue

        # -------------------------------------------------
        # CRITICAL FIX 2:
        # Store the same values actually used to classify.
        # -------------------------------------------------
        g_used = r.get("growth_score_smoothed", r.get("growth_score"))
        i_used = r.get("inflation_score_smoothed", r.get("inflation_score"))
        p_used = r.get("policy_score_smoothed", r.get("policy_score"))
        e_used = r.get("external_score_smoothed", r.get("external_score"))

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

    debug_cols = [
        col for col in [
            "growth_score",
            "growth_score_smoothed",
            "inflation_score",
            "inflation_score_smoothed",
            "policy_score",
            "policy_score_smoothed",
            "external_score",
            "external_score_smoothed",
            "cpi_level_actual",
            "inflation_momentum_z"
        ] if col in df.columns
    ]
    if debug_cols:
        print("Debug preview:")
        print(df[debug_cols].tail(12))

    rows = build_rows(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()