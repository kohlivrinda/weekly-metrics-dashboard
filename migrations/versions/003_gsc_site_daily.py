"""Add gsc_site_daily table for accurate site-level GSC totals.

Date-only (no page dimension) fetch matches GSC UI headline numbers exactly,
avoiding the multi-URL-per-query impression inflation that page-level aggregation
introduces.

Revision ID: 003
Revises: 002
"""

from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE gsc_site_daily (
            date DATE PRIMARY KEY,
            clicks INTEGER NOT NULL DEFAULT 0,
            impressions INTEGER NOT NULL DEFAULT 0,
            ctr DOUBLE PRECISION NOT NULL DEFAULT 0,
            position DOUBLE PRECISION NOT NULL DEFAULT 0
        );
        CREATE INDEX idx_gsc_site_daily_date ON gsc_site_daily (date);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS gsc_site_daily;")
