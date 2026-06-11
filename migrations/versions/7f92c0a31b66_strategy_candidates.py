"""add strategy candidates

Revision ID: 7f92c0a31b66
Revises: 6d1e3b7a4c55
Create Date: 2026-06-09 00:00:04.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "7f92c0a31b66"
down_revision = "6d1e3b7a4c55"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "strategy_candidate",
        sa.Column("exchange", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("period_days", sa.Integer(), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("profit_factor", sa.Float(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=False),
        sa.Column("expectancy", sa.Float(), nullable=False),
        sa.Column("max_drawdown", sa.Float(), nullable=False),
        sa.Column("max_drawdown_percent", sa.Float(), nullable=False),
        sa.Column("sharpe", sa.Float(), nullable=False),
        sa.Column("trades_count", sa.Integer(), nullable=False),
        sa.Column("stability_score", sa.Float(), nullable=False),
        sa.Column("profit_factor_score", sa.Float(), nullable=False),
        sa.Column("drawdown_score", sa.Float(), nullable=False),
        sa.Column("sharpe_score", sa.Float(), nullable=False),
        sa.Column("walk_forward_score", sa.Float(), nullable=False),
        sa.Column("rejection_reasons", sa.JSON(), nullable=True),
        sa.Column("equity_curve_json", sa.JSON(), nullable=True),
        sa.Column("drawdown_curve_json", sa.JSON(), nullable=True),
        sa.Column("monte_carlo_json", sa.JSON(), nullable=True),
        sa.Column("walk_forward_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_strategy_candidate_stability_score", "strategy_candidate", ["stability_score"], unique=False)


def downgrade():
    op.drop_index("ix_strategy_candidate_stability_score", table_name="strategy_candidate")
    op.drop_table("strategy_candidate")
