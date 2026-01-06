import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# -----------------------------
# Configuration (portable)
# -----------------------------
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 6, 30)  # 18 months

NUM_CREATORS = 20
NUM_FANS = 1000

DB_NAME = "creator_analytics.db"

# Use project root (folder containing this file)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / DB_NAME

# IMPORTANT: schema.sql is expected to be in the project root (same folder as this script)
SCHEMA_PATH = BASE_DIR / "schema.sql"

# Performance knobs (so it doesn’t run forever)
# Reduces amount of content scanned per fan by sampling content per creator
MAX_CONTENT_PER_CREATOR_PER_FAN = 120  # lower = faster, higher = more events
BASE_ENGAGEMENT_RATE = 0.005


# -----------------------------
# Helper functions
# -----------------------------
def get_random_date(start: datetime, end: datetime) -> datetime:
    """Generate a random datetime between start and end."""
    return start + (end - start) * np.random.random()


def generate_creators() -> pd.DataFrame:
    categories = ["Gaming", "Education", "Art", "Music", "Vlogging", "Cooking", "Fitness"]
    creator_data = []
    for i in range(1, NUM_CREATORS + 1):
        join_date = get_random_date(START_DATE, START_DATE + timedelta(days=365))
        creator_data.append(
            {
                "creator_id": i,
                "category": np.random.choice(categories, p=[0.2, 0.2, 0.15, 0.15, 0.1, 0.1, 0.1]),
                "join_date": join_date.strftime("%Y-%m-%d"),
            }
        )
    return pd.DataFrame(creator_data)


def generate_fans() -> pd.DataFrame:
    countries = ["USA", "CAN", "GBR", "AUS", "DEU", "FRA", "JPN", "IND"]
    fan_data = []
    for i in range(1, NUM_FANS + 1):
        signup_date = get_random_date(START_DATE, END_DATE)
        fan_data.append(
            {
                "fan_id": i,
                "signup_date": signup_date.strftime("%Y-%m-%d"),
                "country": np.random.choice(countries, p=[0.3, 0.1, 0.15, 0.05, 0.1, 0.05, 0.05, 0.2]),
            }
        )
    return pd.DataFrame(fan_data)


def generate_content(creators_df: pd.DataFrame) -> pd.DataFrame:
    content_types = ["video", "post", "livestream"]
    content_data = []
    content_id = 1

    for _, creator in creators_df.iterrows():
        join_dt = datetime.strptime(creator["join_date"], "%Y-%m-%d")
        num_content = np.random.randint(50, 200)

        for _ in range(num_content):
            publish_date = get_random_date(join_dt, END_DATE)
            content_data.append(
                {
                    "content_id": content_id,
                    "creator_id": int(creator["creator_id"]),
                    "content_type": np.random.choice(content_types, p=[0.5, 0.3, 0.2]),
                    "publish_date": publish_date.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            content_id += 1

    df = pd.DataFrame(content_data)
    return df


def generate_memberships(fans_df: pd.DataFrame, creators_df: pd.DataFrame) -> pd.DataFrame:
    tiers = {"Bronze": 5.00, "Silver": 10.00, "Gold": 20.00, "Platinum": 50.00}
    tier_names = list(tiers.keys())

    membership_data = []
    membership_id = 1

    creators = creators_df["creator_id"].unique()

    for _, fan in fans_df.iterrows():
        num_creators_supported = np.random.choice([0, 1, 2, 3], p=[0.5, 0.3, 0.15, 0.05])
        if num_creators_supported == 0:
            continue

        supported_creators = np.random.choice(creators, num_creators_supported, replace=False)

        fan_signup = datetime.strptime(fan["signup_date"], "%Y-%m-%d")

        for creator_id in supported_creators:
            creator_join = datetime.strptime(
                creators_df.loc[creators_df["creator_id"] == creator_id, "join_date"].iloc[0],
                "%Y-%m-%d",
            )
            start_date_min = max(fan_signup, creator_join)

            latest_start = END_DATE - timedelta(days=30)
            if start_date_min >= latest_start:
                continue

            start_date = get_random_date(start_date_min, latest_start)

            tier_name = np.random.choice(tier_names, p=[0.4, 0.3, 0.2, 0.1])
            monthly_price = tiers[tier_name]

            is_active = np.random.choice([True, False], p=[0.6, 0.4])
            end_date = None
            if not is_active:
                min_churn_date = start_date + timedelta(days=30)
                if min_churn_date < END_DATE:
                    end_date = get_random_date(min_churn_date, END_DATE).strftime("%Y-%m-%d")

            membership_data.append(
                {
                    "membership_id": membership_id,
                    "fan_id": int(fan["fan_id"]),
                    "creator_id": int(creator_id),
                    "tier": tier_name,
                    "monthly_price": float(monthly_price),
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date,
                }
            )
            membership_id += 1

    return pd.DataFrame(membership_data)


def generate_engagement_events(
    content_df: pd.DataFrame,
    memberships_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generates engagement events for members interacting with content from creators they support.
    This version is faster and avoids scanning *all* content for each fan.
    """
    event_types = ["view", "like", "comment"]
    engagement_rows = []
    event_id = 1

    # Prepare datetimes
    content = content_df.copy()
    content["publish_dt"] = pd.to_datetime(content["publish_date"])

    memberships = memberships_df.copy()
    memberships["start_dt"] = pd.to_datetime(memberships["start_date"])
    memberships["end_dt"] = pd.to_datetime(memberships["end_date"]).fillna(pd.Timestamp(END_DATE))

    # Index content by creator_id for fast sampling
    content_by_creator = {
        cid: grp for cid, grp in content.groupby("creator_id", sort=False)
    }

    # Group memberships by fan
    for fan_id, fan_memberships in memberships.groupby("fan_id", sort=False):
        for _, m in fan_memberships.iterrows():
            creator_id = int(m["creator_id"])
            creator_content = content_by_creator.get(creator_id)
            if creator_content is None or creator_content.empty:
                continue

            # Sample content for speed
            if len(creator_content) > MAX_CONTENT_PER_CREATOR_PER_FAN:
                sampled = creator_content.sample(MAX_CONTENT_PER_CREATOR_PER_FAN, random_state=None)
            else:
                sampled = creator_content

            # Filter to content during membership window
            window_content = sampled[
                (sampled["publish_dt"] >= m["start_dt"]) & (sampled["publish_dt"] <= m["end_dt"])
            ]
            if window_content.empty:
                continue

            # Tier affects probability
            engagement_prob = BASE_ENGAGEMENT_RATE * 5.0
            tier = m["tier"]
            if tier == "Silver":
                engagement_prob *= 1.2
            elif tier == "Gold":
                engagement_prob *= 1.5
            elif tier == "Platinum":
                engagement_prob *= 2.0

            for _, c in window_content.iterrows():
                if np.random.rand() < engagement_prob:
                    num_events = np.random.randint(1, 4)
                    for _ in range(num_events):
                        event_date = c["publish_dt"] + timedelta(hours=int(np.random.randint(1, 168)))
                        engagement_rows.append(
                            {
                                "event_id": event_id,
                                "fan_id": int(fan_id),
                                "content_id": int(c["content_id"]),
                                "event_type": np.random.choice(event_types),
                                "event_date": event_date.strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                        event_id += 1

    return pd.DataFrame(engagement_rows)


def create_db_and_tables(conn: sqlite3.Connection) -> None:
    """Creates the database tables using schema.sql."""
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"schema.sql not found at: {SCHEMA_PATH}\n"
            f"Place schema.sql in the same folder as generate_data.py."
        )

    print("Creating database tables...")
    sql_script = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(sql_script)
    conn.commit()
    print("Tables created successfully.")


def save_to_sql(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection) -> None:
    print(f"Saving {len(df)} records to {table_name}...")
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Finished saving {table_name}.")


def main() -> None:
    print("Starting data generation...")

    # Create ./data directory
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Remove old DB if exists
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))

    try:
        # Create tables
        create_db_and_tables(conn)

        # Generate core data
        creators_df = generate_creators()
        fans_df = generate_fans()
        content_df = generate_content(creators_df)
        memberships_df = generate_memberships(fans_df, creators_df)

        # Generate engagement
        engagement_df = generate_engagement_events(content_df, memberships_df)

        # Save to DB
        save_to_sql(creators_df, "creators", conn)
        save_to_sql(fans_df, "fans", conn)
        save_to_sql(content_df, "content", conn)
        save_to_sql(memberships_df, "memberships", conn)
        save_to_sql(engagement_df, "engagement_events", conn)

        print(f"✅ Data generation complete. Database created at: {DB_PATH}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
