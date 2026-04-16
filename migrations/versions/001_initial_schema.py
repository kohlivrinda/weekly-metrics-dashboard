"""Create initial tables for GSC, GA4, keyword_rankings, and profound.

Revision ID: 001
Revises:
Create Date: 2026-04-16
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE gsc (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            date        DATE             NOT NULL,
            page        TEXT             NOT NULL,
            query       TEXT             NOT NULL,
            clicks      INTEGER          NOT NULL DEFAULT 0,
            impressions INTEGER          NOT NULL DEFAULT 0,
            ctr         DOUBLE PRECISION NOT NULL DEFAULT 0,
            position    DOUBLE PRECISION NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX idx_gsc_natural_key ON gsc (date, md5(page), md5(query));
        CREATE INDEX idx_gsc_date ON gsc (date);
    """)

    op.execute("""
        CREATE TABLE ga4 (
            date            DATE    NOT NULL,
            page_path       TEXT    NOT NULL,
            session_source  TEXT    NOT NULL,
            session_medium  TEXT    NOT NULL DEFAULT '',
            sessions        INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, page_path, session_source, session_medium)
        );
        CREATE INDEX idx_ga4_date ON ga4 (date);
    """)

    op.execute("""
        CREATE TABLE keyword_rankings (
            keyword         TEXT             NOT NULL,
            date            DATE             NOT NULL,
            rank            DOUBLE PRECISION,
            result_type     TEXT    NOT NULL DEFAULT '',
            landing_page    TEXT    NOT NULL DEFAULT '',
            search_volume   INTEGER,
            cpc             DOUBLE PRECISION,
            difficulty      DOUBLE PRECISION,
            tags            TEXT    NOT NULL DEFAULT '',
            intents         TEXT    NOT NULL DEFAULT '',
            PRIMARY KEY (keyword, date)
        );
        CREATE INDEX idx_keywords_date ON keyword_rankings (date);
    """)

    op.execute("""
        CREATE TABLE profound (
            date                DATE    NOT NULL,
            topic               TEXT    NOT NULL,
            prompt              TEXT    NOT NULL,
            platform            TEXT    NOT NULL,
            position            TEXT    NOT NULL DEFAULT '',
            mentioned           BOOLEAN NOT NULL DEFAULT FALSE,
            mentions            TEXT    NOT NULL DEFAULT '',
            normalized_mentions TEXT    NOT NULL DEFAULT '',
            citations           JSONB   NOT NULL DEFAULT '[]'::jsonb,
            response            TEXT    NOT NULL DEFAULT '',
            run_id              TEXT    NOT NULL DEFAULT '',
            platform_id         TEXT    NOT NULL DEFAULT '',
            tags                TEXT    NOT NULL DEFAULT '',
            region              TEXT    NOT NULL DEFAULT '',
            persona             TEXT    NOT NULL DEFAULT '',
            type                TEXT    NOT NULL DEFAULT '',
            search_queries      TEXT    NOT NULL DEFAULT '',
            PRIMARY KEY (date, topic, prompt, platform)
        );
        CREATE INDEX idx_profound_date ON profound (date);
        CREATE INDEX idx_profound_topic ON profound (topic);
        CREATE INDEX idx_profound_platform ON profound (platform);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS profound")
    op.execute("DROP TABLE IF EXISTS keyword_rankings")
    op.execute("DROP TABLE IF EXISTS ga4")
    op.execute("DROP TABLE IF EXISTS gsc")
