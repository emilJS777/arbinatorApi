"""orderbook recovery manual margin controls

Revision ID: h1c9d8e7f602
Revises: g8d5e2f1a904
Create Date: 2026-06-15 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "h1c9d8e7f602"
down_revision = "g8d5e2f1a904"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("recovery_state", sa.Column("last_manual_recovery_reset_at", sa.DateTime(), nullable=True))
    op.add_column("recovery_state", sa.Column("last_manual_margin_override_at", sa.DateTime(), nullable=True))
    op.add_column("recovery_state", sa.Column("last_manual_margin_override_value", sa.Float(), nullable=True))


def downgrade():
    op.drop_column("recovery_state", "last_manual_margin_override_value")
    op.drop_column("recovery_state", "last_manual_margin_override_at")
    op.drop_column("recovery_state", "last_manual_recovery_reset_at")
