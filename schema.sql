-- Creator Support Analytics Dashboard Schema
-- Designed for SQLite (Postgres-compatible types used where possible)

-- Table 1: creators
CREATE TABLE creators (
    creator_id INTEGER PRIMARY KEY,
    category TEXT NOT NULL, -- e.g. gaming, education, art
    join_date DATE NOT NULL
);

-- Table 2: fans
CREATE TABLE fans (
    fan_id INTEGER PRIMARY KEY,
    signup_date DATE NOT NULL,
    country TEXT NOT NULL
);

-- Table 3: content
CREATE TABLE content (
    content_id INTEGER PRIMARY KEY,
    creator_id INTEGER NOT NULL,
    content_type TEXT NOT NULL, -- video, post, livestream
    publish_date DATETIME NOT NULL,
    FOREIGN KEY (creator_id) REFERENCES creators(creator_id)
);

-- Table 4: engagement_events
CREATE TABLE engagement_events (
    event_id INTEGER PRIMARY KEY,
    fan_id INTEGER NOT NULL,
    content_id INTEGER NOT NULL,
    event_type TEXT NOT NULL, -- view, like, comment
    event_date DATETIME NOT NULL,
    FOREIGN KEY (fan_id) REFERENCES fans(fan_id),
    FOREIGN KEY (content_id) REFERENCES content(content_id)
);

-- Table 5: memberships
CREATE TABLE memberships (
    membership_id INTEGER PRIMARY KEY,
    fan_id INTEGER NOT NULL,
    creator_id INTEGER NOT NULL,
    tier TEXT NOT NULL, -- e.g. $5, $10, $20
    monthly_price REAL NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE, -- nullable, indicates active membership if NULL
    FOREIGN KEY (fan_id) REFERENCES fans(fan_id),
    FOREIGN KEY (creator_id) REFERENCES creators(creator_id)
);
