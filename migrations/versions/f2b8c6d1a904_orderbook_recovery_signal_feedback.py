"""add order book recovery signal feedback fields

Revision ID: f2b8c6d1a904
Revises: e7a1c4d9b205
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "f2b8c6d1a904"
down_revision = "e7a1c4d9b205"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.add_column(sa.Column("feedback_enabled", sa.Boolean(), server_default=sa.true(), nullable=False))
        batch_op.add_column(sa.Column("feedback_lookback_trades", sa.Integer(), server_default="50", nullable=False))
        batch_op.add_column(sa.Column("side_loss_streak_limit", sa.Integer(), server_default="3", nullable=False))
        batch_op.add_column(sa.Column("side_cooldown_seconds", sa.Integer(), server_default="600", nullable=False))
        batch_op.add_column(sa.Column("min_side_win_rate", sa.Float(), server_default="40", nullable=False))
        batch_op.add_column(sa.Column("adaptive_consensus_boost", sa.Float(), server_default="0.1", nullable=False))
        batch_op.add_column(sa.Column("adaptive_min_valid_exchanges_boost", sa.Integer(), server_default="1", nullable=False))

    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.add_column(sa.Column("signal_consensus_direction", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("signal_valid_exchanges_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("signal_confirming_long_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("signal_confirming_short_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("signal_consensus_ratio_long", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_consensus_ratio_short", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_average_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_average_momentum", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_configured_exchange_imbalance", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_configured_exchange_spread", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_configured_exchange_momentum", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("signal_entry_blocked_reason", sa.String(length=120), nullable=True))
        batch_op.add_column(sa.Column("signal_per_exchange_features_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("holding_seconds", sa.Float(), nullable=True))


def downgrade():
    with op.batch_alter_table("strategy_run_trade", schema=None) as batch_op:
        batch_op.drop_column("holding_seconds")
        batch_op.drop_column("signal_per_exchange_features_json")
        batch_op.drop_column("signal_entry_blocked_reason")
        batch_op.drop_column("signal_configured_exchange_momentum")
        batch_op.drop_column("signal_configured_exchange_spread")
        batch_op.drop_column("signal_configured_exchange_imbalance")
        batch_op.drop_column("signal_average_momentum")
        batch_op.drop_column("signal_average_imbalance")
        batch_op.drop_column("signal_consensus_ratio_short")
        batch_op.drop_column("signal_consensus_ratio_long")
        batch_op.drop_column("signal_confirming_short_count")
        batch_op.drop_column("signal_confirming_long_count")
        batch_op.drop_column("signal_valid_exchanges_count")
        batch_op.drop_column("signal_consensus_direction")

    with op.batch_alter_table("order_book_pattern_strategy_config", schema=None) as batch_op:
        batch_op.drop_column("adaptive_min_valid_exchanges_boost")
        batch_op.drop_column("adaptive_consensus_boost")
        batch_op.drop_column("min_side_win_rate")
        batch_op.drop_column("side_cooldown_seconds")
        batch_op.drop_column("side_loss_streak_limit")
        batch_op.drop_column("feedback_lookback_trades")
        batch_op.drop_column("feedback_enabled")
