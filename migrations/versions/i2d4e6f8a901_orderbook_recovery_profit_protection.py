"""orderbook recovery profit protection filters

Revision ID: i2d4e6f8a901
Revises: h1c9d8e7f602
Create Date: 2026-06-18 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "i2d4e6f8a901"
down_revision = "h1c9d8e7f602"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.add_column(sa.Column("live_fee_filter_enabled", sa.Boolean(), nullable=False, server_default=sa.true()))
        batch_op.add_column(sa.Column("live_fee_filter_taker_fee_percent", sa.Float(), nullable=False, server_default="0.1"))
        batch_op.add_column(sa.Column("momentum_confirmation_enabled", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch_op.add_column(sa.Column("side_quality_filter_enabled", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch_op.add_column(sa.Column("side_quality_lookback_trades", sa.Integer(), nullable=False, server_default="5"))
        batch_op.add_column(sa.Column("side_quality_cooldown_seconds", sa.Integer(), nullable=False, server_default="600"))

    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.add_column(sa.Column("gross_pnl", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("net_pnl", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("total_fee", sa.Float(), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade") as batch_op:
        batch_op.drop_column("total_fee")
        batch_op.drop_column("net_pnl")
        batch_op.drop_column("gross_pnl")

    with op.batch_alter_table("order_book_pattern_strategy_config") as batch_op:
        batch_op.drop_column("side_quality_cooldown_seconds")
        batch_op.drop_column("side_quality_lookback_trades")
        batch_op.drop_column("side_quality_filter_enabled")
        batch_op.drop_column("momentum_confirmation_enabled")
        batch_op.drop_column("live_fee_filter_taker_fee_percent")
        batch_op.drop_column("live_fee_filter_enabled")
