from pathlib import Path
import sqlite3
import pandas as pd

from metrics import CreatorAnalytics
import helpers as h


def main():
    # --- Portable paths (works on Mac/Windows/Linux) ---
    BASE_DIR = Path(__file__).resolve().parent
    DB_PATH = BASE_DIR / "data" / "creator_analytics.db"
    OUTPUT_DIR = BASE_DIR / "docs"  # where charts will be saved
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Safety check ---
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at: {DB_PATH}\n"
            f"Run: python3 generate_data.py\n"
            f"Then try again."
        )

    # --- Patch helpers.save_plot default output path (no need to edit helpers.py) ---
    _original_save_plot = h.save_plot

    def _save_plot_portable(fig, filename, path=None):
        return _original_save_plot(fig, filename, path=str(OUTPUT_DIR))

    h.save_plot = _save_plot_portable

    analytics = CreatorAnalytics(str(DB_PATH))

    print("Generating visualizations...")

    # 1) Monthly Active Supporters
    mas_df = analytics.get_monthly_active_supporters()
    h.plot_time_series(
        mas_df, "month", "monthly_active_supporters",
        "Monthly Active Supporters (MAS) Trend", "Active Supporters",
        "mas_trend.png"
    )

    # 2) Monthly Churn Rate
    churn_df = analytics.get_monthly_churn_rate()
    h.plot_time_series(
        churn_df, "month", "monthly_churn_rate_pct",
        "Monthly Churn Rate (%)", "Churn Rate (%)",
        "churn_trend.png"
    )

    # 3) ARPM Overall
    arpm_df = analytics.get_arpm()
    h.plot_time_series(
        arpm_df, "month", "arpm",
        "Average Revenue Per Member (ARPM) Trend", "ARPM ($)",
        "arpm_trend.png"
    )

    # 4) ARPM by Membership Tier
    arpm_tier_df = analytics.get_arpm_by_membership_tier()
    h.plot_segmented_time_series(
        arpm_tier_df, "month", "arpm", "segment_value",
        "ARPM by Membership Tier", "ARPM ($)",
        "arpm_by_tier.png"
    )

    # 5) ARPM by Creator Category
    arpm_cat_df = analytics.get_arpm_by_creator_category()
    h.plot_segmented_time_series(
        arpm_cat_df, "month", "arpm", "segment_value",
        "ARPM by Creator Category", "ARPM ($)",
        "arpm_by_category.png"
    )

    # 6) Cohort Retention Heatmap (computed in pandas)
    with sqlite3.connect(str(DB_PATH)) as conn:
        memberships_df = pd.read_sql_query("SELECT * FROM memberships", conn)

    retention_df = h.calculate_cohort_retention(memberships_df)
    h.plot_retention_heatmap(
        retention_df,
        "Cohort Retention Analysis (%)",
        "retention_heatmap.png"
    )

    # 7) Top Creators by Membership Duration
    drivers = analytics.get_top_drivers_of_recurring_support()
    top_creators = drivers["top_creators"]
    h.plot_bar_chart(
        top_creators, "creator_id", "avg_membership_duration_days",
        "Top 10 Creators by Avg Membership Duration",
        "Creator ID", "Avg Duration (Days)",
        "top_creators_duration.png"
    )

    print(f"✅ All visualizations generated successfully in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
