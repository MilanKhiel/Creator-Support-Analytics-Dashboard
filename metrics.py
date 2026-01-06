import os
import sqlite3
from datetime import datetime
import numpy as np
import pandas as pd


class CreatorAnalytics:
    """
    Computes core analytics metrics for the Creator Support Analytics Dashboard.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)

        # Helpful error message if path is wrong
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(
                f"SQLite DB file not found at: {self.db_path}\n"
                f"Make sure you ran generate_data.py and that the DB exists at ./data/creator_analytics.db"
            )

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def __del__(self):
        # Avoid error if init failed before creating self.conn
        if hasattr(self, "conn"):
            try:
                self.conn.close()
            except Exception:
                pass

    def _execute_query(self, query: str, params=()):
        return pd.read_sql_query(query, self.conn, params=params)

    def get_monthly_active_supporters(self):
        """
        Monthly Active Supporters (MAS):
        Unique fans with an active membership in a given month.
        """
        query = """
        WITH RECURSIVE months(month_start) AS (
            SELECT date('2024-01-01')
            UNION ALL
            SELECT date(month_start, '+1 month')
            FROM months
            WHERE month_start < date('2025-06-01')
        )
        SELECT
            strftime('%Y-%m', m.month_start) AS month,
            COUNT(DISTINCT t1.fan_id) AS monthly_active_supporters
        FROM months m
        JOIN memberships t1 ON
            t1.start_date <= m.month_start AND
            (t1.end_date IS NULL OR t1.end_date > m.month_start)
        GROUP BY 1
        ORDER BY 1;
        """
        return self._execute_query(query)

    def get_monthly_churn_rate(self):
        """
        Monthly Churn Rate:
        (Memberships ended in month) / (Active memberships at start of month) * 100
        """
        query = """
        WITH RECURSIVE months(month_start) AS (
            SELECT date('2024-02-01')
            UNION ALL
            SELECT date(month_start, '+1 month')
            FROM months
            WHERE month_start < date('2025-06-01')
        ),
        monthly_status AS (
            SELECT
                strftime('%Y-%m', m.month_start) AS month,
                m.month_start,
                COUNT(t1.membership_id) AS active_at_start,
                SUM(
                    CASE
                        WHEN t1.end_date IS NOT NULL
                         AND strftime('%Y-%m', t1.end_date) = strftime('%Y-%m', m.month_start)
                        THEN 1 ELSE 0
                    END
                ) AS churned_in_month
            FROM months m
            JOIN memberships t1 ON
                t1.start_date < m.month_start AND
                (t1.end_date IS NULL OR t1.end_date >= m.month_start)
            GROUP BY 1, 2
        )
        SELECT
            month,
            active_at_start,
            churned_in_month,
            CAST(churned_in_month AS REAL) * 100.0 / active_at_start AS monthly_churn_rate_pct
        FROM monthly_status
        WHERE active_at_start > 0
        ORDER BY 1;
        """
        return self._execute_query(query)

    def get_arpm(self, segment_by=None):
        """
        ARPM (Average Revenue Per Member):
        Total revenue from active memberships in a month / Monthly Active Supporters.

        segment_by:
          - None
          - 'creator_category'
          - 'membership_tier'
          - 'content_type' (proxy via creator's most frequent content type)
        """
        segment_map = {
            "creator_category": "t2.category",
            "membership_tier": "t1.tier",
        }

        if segment_by == "content_type":
            return self.get_arpm_by_content_type()

        if segment_by and segment_by not in segment_map:
            raise ValueError(f"Invalid segment_by: {segment_by}")

        segment_select = f", {segment_map[segment_by]} AS segment_value" if segment_by else ""
        segment_group = ", segment_value" if segment_by else ""

        query = f"""
        WITH RECURSIVE months(month_start) AS (
            SELECT date('2024-01-01')
            UNION ALL
            SELECT date(month_start, '+1 month')
            FROM months
            WHERE month_start < date('2025-06-01')
        )
        SELECT
            strftime('%Y-%m', m.month_start) AS month
            {segment_select},
            SUM(t1.monthly_price) AS total_monthly_revenue,
            COUNT(DISTINCT t1.fan_id) AS monthly_active_supporters,
            SUM(t1.monthly_price) * 1.0 / COUNT(DISTINCT t1.fan_id) AS arpm
        FROM months m
        JOIN memberships t1 ON
            t1.start_date <= m.month_start AND
            (t1.end_date IS NULL OR t1.end_date > m.month_start)
        LEFT JOIN creators t2 ON t1.creator_id = t2.creator_id
        GROUP BY 1 {segment_group}
        ORDER BY 1 {segment_group};
        """
        return self._execute_query(query)

    def get_arpm_by_creator_category(self):
        return self.get_arpm(segment_by="creator_category")

    def get_arpm_by_membership_tier(self):
        return self.get_arpm(segment_by="membership_tier")

    def get_arpm_by_content_type(self):
        """
        ARPM segmented by content type (proxy):
        We approximate a creator's "primary content type" by their most frequent content_type.
        """
        query = """
        WITH creator_primary_type AS (
            SELECT creator_id, content_type
            FROM (
                SELECT
                    creator_id,
                    content_type,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (PARTITION BY creator_id ORDER BY COUNT(*) DESC) AS rn
                FROM content
                GROUP BY 1, 2
            )
            WHERE rn = 1
        ),
        months(month_start) AS (
            SELECT date('2024-01-01')
            UNION ALL
            SELECT date(month_start, '+1 month')
            FROM months
            WHERE month_start < date('2025-06-01')
        )
        SELECT
            strftime('%Y-%m', m.month_start) AS month,
            cpt.content_type AS segment_value,
            SUM(ms.monthly_price) AS total_monthly_revenue,
            COUNT(DISTINCT ms.fan_id) AS monthly_active_supporters,
            SUM(ms.monthly_price) * 1.0 / COUNT(DISTINCT ms.fan_id) AS arpm
        FROM months m
        JOIN memberships ms ON
            ms.start_date <= m.month_start AND
            (ms.end_date IS NULL OR ms.end_date > m.month_start)
        JOIN creator_primary_type cpt ON ms.creator_id = cpt.creator_id
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
        return self._execute_query(query)

    def get_engagement_dropoff_prior_to_churn(self):
        """
        Engagement Drop-off Prior to Churn:
        For each churned membership, compare engagement in the month before churn
        vs. baseline average engagement over the previous 3 months.
        """
        churned = self._execute_query(
            """
            SELECT membership_id, fan_id, creator_id, start_date, end_date
            FROM memberships
            WHERE end_date IS NOT NULL;
            """
        )
        if churned.empty:
            return pd.DataFrame(
                columns=[
                    "membership_id",
                    "creator_id",
                    "churn_date",
                    "engagement_pre_churn",
                    "engagement_baseline_avg",
                    "dropoff_pct",
                ]
            )

        fan_ids = tuple(int(x) for x in churned["fan_id"].unique())
        creator_ids = tuple(int(x) for x in churned["creator_id"].unique())

        engagement_query = f"""
        SELECT
            e.event_id,
            e.fan_id,
            c.creator_id,
            e.event_date,
            e.event_type
        FROM engagement_events e
        JOIN content c ON e.content_id = c.content_id
        WHERE e.fan_id IN {fan_ids} AND c.creator_id IN {creator_ids};
        """
        all_engagement = self._execute_query(engagement_query)
        all_engagement["event_date"] = pd.to_datetime(all_engagement["event_date"])

        results = []
        for _, row in churned.iterrows():
            churn_date = pd.to_datetime(row["end_date"])
            fan_id = row["fan_id"]
            creator_id = row["creator_id"]

            fan_creator = all_engagement[
                (all_engagement["fan_id"] == fan_id)
                & (all_engagement["creator_id"] == creator_id)
            ]

            pre_churn_start = churn_date - pd.DateOffset(months=1)
            engagement_pre_churn = fan_creator[
                (fan_creator["event_date"] >= pre_churn_start)
                & (fan_creator["event_date"] < churn_date)
            ]["event_id"].count()

            baseline_end = pre_churn_start
            baseline_start = baseline_end - pd.DateOffset(months=3)

            engagement_baseline = (
                fan_creator[
                    (fan_creator["event_date"] >= baseline_start)
                    & (fan_creator["event_date"] < baseline_end)
                ]["event_id"].count()
                / 3.0
            )

            dropoff_pct = None
            if engagement_baseline > 0:
                dropoff_pct = ((engagement_baseline - engagement_pre_churn) / engagement_baseline) * 100.0

            results.append(
                {
                    "membership_id": row["membership_id"],
                    "creator_id": creator_id,
                    "churn_date": row["end_date"],
                    "engagement_pre_churn": engagement_pre_churn,
                    "engagement_baseline_avg": engagement_baseline,
                    "dropoff_pct": dropoff_pct,
                }
            )

        return pd.DataFrame(results)

    def get_top_drivers_of_recurring_support(self):
        """
        Top Drivers of Recurring Support:
        Identify creators and tiers with the highest average membership duration.
        """
        creator_query = """
        SELECT
            m.creator_id,
            c.category,
            AVG(
                julianday(CASE WHEN m.end_date IS NULL THEN date('2025-06-30') ELSE m.end_date END)
                - julianday(m.start_date)
            ) AS avg_membership_duration_days,
            COUNT(m.membership_id) AS total_memberships
        FROM memberships m
        JOIN creators c ON m.creator_id = c.creator_id
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 10;
        """
        top_creators = self._execute_query(creator_query)

        tier_query = """
        SELECT
            tier,
            AVG(
                julianday(CASE WHEN end_date IS NULL THEN date('2025-06-30') ELSE end_date END)
                - julianday(start_date)
            ) AS avg_membership_duration_days,
            COUNT(membership_id) AS total_memberships
        FROM memberships
        GROUP BY 1
        ORDER BY 2 DESC;
        """
        tier_perf = self._execute_query(tier_query)

        return {"top_creators": top_creators, "tier_performance": tier_perf}

    def get_all_metrics(self):
        return {
            "monthly_active_supporters": self.get_monthly_active_supporters(),
            "monthly_churn_rate": self.get_monthly_churn_rate(),
            "arpm_overall": self.get_arpm(),
            "arpm_by_creator_category": self.get_arpm_by_creator_category(),
            "arpm_by_membership_tier": self.get_arpm_by_membership_tier(),
            "arpm_by_content_type": self.get_arpm_by_content_type(),
            "engagement_dropoff_prior_to_churn": self.get_engagement_dropoff_prior_to_churn(),
            "top_drivers_of_recurring_support": self.get_top_drivers_of_recurring_support(),
        }
