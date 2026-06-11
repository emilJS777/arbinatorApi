"""add research backtest tables

Revision ID: 6d1e3b7a4c55
Revises: 5c2f8a9d0e44
Create Date: 2026-06-09 00:00:03.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "6d1e3b7a4c55"
down_revision = "5c2f8a9d0e44"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "backtest_run",
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("start_date", sa.DateTime(), nullable=True),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        sa.Column("trades_count", sa.Integer(), nullable=False),
        sa.Column("total_pnl", sa.Float(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=False),
        sa.Column("profit_factor", sa.Float(), nullable=False),
        sa.Column("max_drawdown", sa.Float(), nullable=False),
        sa.Column("sharpe_ratio", sa.Float(), nullable=False),
        sa.Column("expectancy", sa.Float(), nullable=False),
        sa.Column("sortino_ratio", sa.Float(), nullable=False),
        sa.Column("recovery_factor", sa.Float(), nullable=False),
        sa.Column("average_holding_minutes", sa.Float(), nullable=False),
        sa.Column("longest_losing_streak", sa.Integer(), nullable=False),
        sa.Column("longest_winning_streak", sa.Integer(), nullable=False),
        sa.Column("average_r_multiple", sa.Float(), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column("equity_curve_json", sa.JSON(), nullable=True),
        sa.Column("drawdown_curve_json", sa.JSON(), nullable=True),
        sa.Column("monthly_returns_json", sa.JSON(), nullable=True),
        sa.Column("monte_carlo_json", sa.JSON(), nullable=True),
        sa.Column("walk_forward_json", sa.JSON(), nullable=True),
        sa.Column("optimization_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "backtest_trade",
        sa.Column("backtest_run_id", sa.Integer(), nullable=False),
        sa.Column("entry_time", sa.DateTime(), nullable=False),
        sa.Column("exit_time", sa.DateTime(), nullable=False),
        sa.Column("side", sa.String(length=20), nullable=False),
        sa.Column("entry_price", sa.Float(), nullable=False),
        sa.Column("exit_price", sa.Float(), nullable=False),
        sa.Column("pnl", sa.Float(), nullable=False),
        sa.Column("pnl_percent", sa.Float(), nullable=False),
        sa.Column("r_multiple", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(length=40), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["backtest_run_id"], ["backtest_run.id"]),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("backtest_trade")
    op.drop_table("backtest_run")
