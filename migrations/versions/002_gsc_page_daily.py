"""Add gsc_page_daily table for accurate page-level aggregates.

Fetching GSC with the `query` dimension drops anonymized-query rows, so sums
under-count vs the GSC UI totals. This table is populated by a separate fetch
without the `query` dimension so page-level totals match the UI.

Revision ID: 002
Revises: 001
Create Date: 2026-04-21
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE gsc_page_daily (
            date        DATE             NOT NULL,
            page        TEXT             NOT NULL,
            clicks      INTEGER          NOT NULL DEFAULT 0,
            impressions INTEGER          NOT NULL DEFAULT 0,
            ctr         DOUBLE PRECISION NOT NULL DEFAULT 0,
            position    DOUBLE PRECISION NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX idx_gsc_page_daily_key ON gsc_page_daily (date, md5(page));
        CREATE INDEX idx_gsc_page_daily_date ON gsc_page_daily (date);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS gsc_page_daily")
