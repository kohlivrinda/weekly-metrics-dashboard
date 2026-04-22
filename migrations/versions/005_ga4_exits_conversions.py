"""GA4 exits and conversions columns for dropoff and conversion attribution.

Revision ID: 005
Revises: 004
Create Date: 2026-04-22
"""

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    # exits = number of sessions where this page was the last page (for dropoff analysis)
    # conversions = key events fired in sessions that included this page
    op.execute("""
        ALTER TABLE ga4
            ADD COLUMN IF NOT EXISTS exits       INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS conversions INTEGER NOT NULL DEFAULT 0
    """)

    op.execute("""
        ALTER TABLE ga4_landing_pages
            ADD COLUMN IF NOT EXISTS conversions INTEGER NOT NULL DEFAULT 0
    """)


def downgrade():
    op.execute("""
        ALTER TABLE ga4_landing_pages
            DROP COLUMN IF EXISTS conversions
    """)
    op.execute("""
        ALTER TABLE ga4
            DROP COLUMN IF EXISTS exits,
            DROP COLUMN IF EXISTS conversions
    """)
