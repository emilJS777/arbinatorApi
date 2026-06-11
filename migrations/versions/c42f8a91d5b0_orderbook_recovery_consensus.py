"""add order book recovery consensus settings

Revision ID: c42f8a91d5b0
Revises: a31b7f4c2d90
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "c42f8a91d5b0"
down_revision = "a31b7f4c2d90"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.add_column(sa.Column("consensus_enabled", sa.Boolean(), server_default=sa.true(), nullable=False))
        batch_op.add_column(sa.Column("min_confirming_exchanges", sa.Integer(), server_default="2", nullable=False))
        batch_op.add_column(sa.Column("min_consensus_ratio", sa.Float(), server_default="0.6", nullable=False))
        batch_op.add_column(sa.Column("max_snapshot_age_seconds", sa.Float(), server_default="5", nullable=False))
        batch_op.add_column(sa.Column("require_configured_exchange_signal", sa.Boolean(), server_default=sa.true(), nullable=False))


def downgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.drop_column("require_configured_exchange_signal")
        batch_op.drop_column("max_snapshot_age_seconds")
        batch_op.drop_column("min_consensus_ratio")
        batch_op.drop_column("min_confirming_exchanges")
        batch_op.drop_column("consensus_enabled")
