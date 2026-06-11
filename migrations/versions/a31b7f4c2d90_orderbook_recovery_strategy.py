"""add order book recovery strategy tables

Revision ID: a31b7f4c2d90
Revises: 9480a506ed06
Create Date: 2026-06-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "a31b7f4c2d90"
down_revision = "9480a506ed06"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "order_book_pattern_strategy_config",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("base_margin_usdt", sa.Float(), nullable=False),
        sa.Column("leverage", sa.Float(), nullable=False),
        sa.Column("max_recovery_steps", sa.Integer(), nullable=False),
        sa.Column("recovery_multiplier", sa.Float(), nullable=False),
        sa.Column("take_profit_percent_of_margin", sa.Float(), nullable=False),
        sa.Column("stop_loss_percent_of_margin", sa.Float(), nullable=False),
        sa.Column("max_daily_loss_usdt", sa.Float(), nullable=False),
        sa.Column("max_total_loss_usdt", sa.Float(), nullable=False),
        sa.Column("max_open_positions", sa.Integer(), nullable=False),
        sa.Column("cooldown_after_loss_seconds", sa.Integer(), nullable=False),
        sa.Column("cooldown_after_win_seconds", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("paper_mode_only", sa.Boolean(), nullable=False),
        sa.Column("long_imbalance_threshold", sa.Float(), nullable=False),
        sa.Column("short_imbalance_threshold", sa.Float(), nullable=False),
        sa.Column("max_spread_percent", sa.Float(), nullable=False),
        sa.Column("momentum_window_snapshots", sa.Integer(), nullable=False),
        sa.Column("paper_equity_usdt", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "recovery_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("strategy_config_id", sa.Integer(), nullable=False),
        sa.Column("current_step", sa.Integer(), nullable=False),
        sa.Column("current_margin", sa.Float(), nullable=False),
        sa.Column("last_trade_result", sa.String(length=20), nullable=True),
        sa.Column("consecutive_losses", sa.Integer(), nullable=False),
        sa.Column("is_stopped", sa.Boolean(), nullable=False),
        sa.Column("stop_reason", sa.String(length=120), nullable=True),
        sa.Column("last_closed_at", sa.DateTime(), nullable=True),
        sa.Column("last_opened_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["strategy_config_id"], ["order_book_pattern_strategy_config.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "strategy_run",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("strategy_config_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("stopped_at", sa.DateTime(), nullable=True),
        sa.Column("stop_reason", sa.String(length=120), nullable=True),
        sa.ForeignKeyConstraint(["strategy_config_id"], ["order_book_pattern_strategy_config.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "strategy_run_trade",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=True),
        sa.Column("strategy_config_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("side", sa.String(length=20), nullable=False),
        sa.Column("margin", sa.Float(), nullable=False),
        sa.Column("leverage", sa.Float(), nullable=False),
        sa.Column("notional", sa.Float(), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("entry_price", sa.Float(), nullable=False),
        sa.Column("exit_price", sa.Float(), nullable=True),
        sa.Column("pnl", sa.Float(), nullable=False),
        sa.Column("result", sa.String(length=20), nullable=True),
        sa.Column("recovery_step", sa.Integer(), nullable=False),
        sa.Column("reason_open", sa.String(length=255), nullable=True),
        sa.Column("reason_close", sa.String(length=80), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=False),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["strategy_config_id"], ["order_book_pattern_strategy_config.id"]),
        sa.ForeignKeyConstraint(["strategy_run_id"], ["strategy_run.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_strategy_run_trade_open", "strategy_run_trade", ["strategy_config_id", "closed_at"])


def downgrade():
    op.drop_index("ix_strategy_run_trade_open", table_name="strategy_run_trade")
    op.drop_table("strategy_run_trade")
    op.drop_table("strategy_run")
    op.drop_table("recovery_state")
    op.drop_table("order_book_pattern_strategy_config")
